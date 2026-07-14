# Natify — High-Performance Communication Framework

> Built on NATS Core | Version 1.2.0 | netstandard2.1;net9.0

Framework giao tiếp hiệu năng cao cho Microservice và Game Server (tối ưu Unity), xây trên nền NATS Core. Điểm nổi bật của v1.2.0: **zero-copy send/receive pipeline** dùng `ArrayPool` + manual protobuf wire format + `ByteString` pass-through.

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Google.Protobuf | 3.34.1 | Serialization (protobuf wire format) |
| NATS.Net (NATS.Client.Core) | 2.7.3 | Network transport |
| Gcoder.Collections | 1.3.0 | TimedSortedSet — TTL dedup wheel |
| Microsoft.Bcl.AsyncInterfaces | 10.0.6 | `IAsyncDisposable` trên netstandard2.1 |
| System.Threading.Channels | built-in | Lock-free queue (Channel) |

**Không cần** NATS JetStream.

---

## Architecture Deep Dive

### 1. Queue Group & Horizontal Scaling

**Đây là điểm quan trọng nhất — dễ bị hiểu nhầm.**

Tham số `groupName` trong factory method **chính là NATS queue group**. Khi N instance cùng `groupName` subscribe cùng subject, NATS Core tự động giao mỗi message cho **đúng 1 instance** duy nhất:

```csharp
// Trong OnMessage — subscribe với queueGroup:
_connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName, ...)
```

→ **Có thể scale N instance an toàn**, không lo duplicate processing.

**Ví dụ Router scale ngang:**
```csharp
// Router instance 1
var router1 = await INatifyServer.CreateAsync("nats://localhost:4222", "Router", "RouterGroup", "GameClient");

// Router instance 2 — cùng serverName + groupName
var router2 = await INatifyServer.CreateAsync("nats://localhost:4222", "Router", "RouterGroup", "GameClient");

// NATS chỉ gửi mỗi message từ Unity đến 1 trong 2 instance
```

### 2. Dedup Scope (Chống trùng lặp)

Dedup trong Natify (`_processedMessages` + `TimedSortedSet`, TTL 10s) dùng để chặn **cùng 1 instance** xử lý cùng 1 batch 2 lần do retransmission (at-least-once retry).

**Đây KHÔNG phải cross-instance dedup.** Cross-instance được NATS queue group xử lý.

```
Cùng 1 instance:         sender → NATS → receiver → ACK lost → retry → receiver
                                                                     ↓
                          dedup ngăn không xử lý batch lần 2       ✓

Nhiều instance:          sender → NATS → instance-1 (nhận) + instance-2 (không nhận)
                          queue group đảm bảo chỉ 1 instance nhận  ✓
```

**Thứ tự xử lý khi nhận batch (đã sửa trong v1.2.0):**
1. Dedup check — nếu batchId đã có → skip
2. Deserialize batch
3. Nếu deserialize fail → TryRemove batchId khỏi dedup + log error
4. Thêm batchId vào TTL wheel + gửi ACK
5. Lặp qua từng message trong batch → gọi callback

### 3. Thread Model

| Class | Callback thread | Cần Tick()? |
|-------|----------------|-------------|
| `NatifyClient` (Unity) | Main Thread (qua `Tick()`) | **CÓ** |
| `NatifyClientFast` (Backend) | ThreadPool (ngay lập tức) | KHÔNG (no-op) |
| `NatifyServer` | ThreadPool (ngay lập tức) | KHÔNG |
| `Trigger` evaluation | ThreadPool (mỗi 500ms) | KHÔNG |

**Quan trọng:** Trong Unity, `NatifyClient` đẩy callback vào `ConcurrentQueue<Action>`, `Tick()` lấy tối đa 100 action/frame xử lý trên Main Thread. Không gọi `Tick()` = callback không bao giờ chạy.

### 4. Topic Convention (NatifyTopics)

Tất cả subject được auto-generate — **code chỉ cần dùng topic "clean"**, không prefix:

| Direction | Method | Generated Subject |
|-----------|--------|-------------------|
| Client → Server (publish) | `GetClientPublishSubject` | `NatifyServer.{server}.{client}.{region}.{topic}` |
| Server → Client (publish) | `GetServerPublishSubject` | `NatifyClient.{client}.{server}.{region}.{topic}` |
| Server listen (wildcard region) | `GetServerListenSubject` | `NatifyServer.{server}.{client}.*.{topic}` |
| Client listen | `GetClientListenSubject` | `NatifyClient.{client}.{server}.{region}.{topic}` |

- `*` trong server listen subject là **wildcard cho REGION**, không phải clientName.
- Server luôn nhận được `regionId` trong callback (extract từ subject) để biết message từ region nào.
- Tất cả `clientName`, `serverName`, `regionId` **không được chứa dấu `.`** (validated trong `NatifyTopics`).

### 5. Request-Reply Mechanism

```
Client gọi RequestAsync("Login", req, timeout)
    │
    ├── Publish("Login", req, "REQ", out reqId)
    │       → NATS: NatifyServer.Srv.Client.VN.Login
    │
    ├── _replyTasks[reqId] = (TaskCompletionSource<ByteString>, CTS)  // chờ reply
    │
    │   Server xử lý OnRequest:
    │       → return reply
    │       → Publish($"Rep-{instanceId}", reply, "REP", repId=reqId)
    │           → NATS: NatifyClient.Client.Srv.VN.Rep-{instanceId}
    │
    └── Nhận reply → _replyTasks[reqId].SetResult(ByteString)
            → Deserialize<TRes>(ByteString) → return
```

Server trả lời qua topic `Rep-{instanceId}` — instanceId là `Guid.NewGuid()` của client, đảm bảo reply về đúng client gửi request.

`TaskCompletionSource<ByteString>` — reply payload là `ByteString` (zero-copy), deserialize bằng `IMessage.MergeFrom(ByteString)` không cần `ToByteArray()`.

### 6. Micro-Batching & Zero-Copy Pipeline (v1.2.0)

#### Send Pipeline (Zero-Copy)

```
Publish(msg)
    │
    ├── NatifySerializer.SerializePooled(msg)  → RentedBuffer (ArrayPool<byte>)
    │       └── message.WriteTo(CodedOutputStream) vào rented buffer — không allocate byte[]
    │
    ├── Channel<(subject, RentedBuffer, msgType, reqId, repId)>  — lock-free funnel
    │
    └── BatchWorkerAsync:
            ├── Gom messages theo subject → BatchAccumulator (per-subject)
            │       └── Giữ RentedBuffer gốc (không copy)
            │
            ├── Khi đạt: 1000 msg | 50KB | 50ms → flush
            │
            ├── NatifySerializer.SerializeBatchPooled(payloads, reqIds, msgTypes, repIds, fromInstanceId)
            │       └── Manual proto3 wire format — KHÔNG tạo NatifyBatch object
            │       └── KHÔNG dùng ByteString.CopyFrom
            │       └── Copy trực tiếp từ RentedBuffer.Data.Span vào batch buffer
            │       └── Trả về RentedBuffer mới (ArrayPool)
            │
            ├── Dispose từng RentedBuffer gốc (try/finally qua BatchAccumulator.Dispose)
            │
            └── PublishAsync(subject, rented.Data, headers) → NATS
                    └── Lưu RentedBuffer vào _unackedMessages để retry
```

#### Receive Pipeline (Zero-Copy)

```
NATS → SubscribeAsync<byte[]>
    │
    ├── Dedup check (batchId)
    ├── NatifySerializer.Deserialize<NatifyBatch>(payload)  → parse batch protobuf
    │
    ├── Gửi ACK + thêm batchId vào TTL wheel
    │
    └── Lặp qua batch.Payloads[i]  (ByteString — zero-copy slice của batch buffer)
            ├── new Data<ByteString>(payload, instanceId, reqId, repId)
            └── Callback nhận Data<ByteString>
                    └── NatifySerializer.Deserialize<T>(ByteString)
                            └── message.MergeFrom(ByteString) — KHÔNG cần ToByteArray()
```

#### RentedBuffer

```csharp
public sealed class RentedBuffer : IDisposable
{
    public ReadOnlyMemory<byte> Data { get; }   // Buffer từ ArrayPool, chỉ lấy [0..Length]
    public int Length { get; }
    public void Dispose();                        // Return buffer về ArrayPool
}
```

`RentedBuffer` wraps `ArrayPool<byte>.Shared.Rent()` — Dispose() trả buffer về pool. Tất cả `RentedBuffer` trong channel và `_unackedMessages` đều được Dispose đúng cách khi:
- Batch đã embed xong (BatchAccumulator.Dispose)
- ACK nhận được (unacked.Buffer?.Dispose())
- Retry vượt giới hạn (drop + dispose)
- Shutdown (drain channel + dispose remaining)

### 7. Reliability (ACK + Retry)

Mỗi batch gửi đi được lưu vào `_unackedMessages` với `RentedBuffer`. Retry loop quét mỗi 100ms:
- Nếu `now - LastSent > 100ms` → retry (tối đa 10 lần)
- Receiver gửi ACK về:
  - **Client nhận từ Server:** `NatifyServer.{server}.{client}.{region}.ACK.{batchId}`
  - **Server nhận từ Client:** `NatifyClient.{client}.{server}.{region}.ACK.{batchId}`
- Khi nhận ACK → xóa khỏi `_unackedMessages` + Dispose RentedBuffer
- Receiver dùng dedup (TTL 10s) để bỏ qua batch đã xử lý nếu retry đến

### 8. Graceful Shutdown (DisposeAsync)

Tất cả class implement `IAsyncDisposable`. Thứ tự shutdown:

```
1. _batchChannel.Writer.Complete()           // Khóa van đầu vào
2. Drain channel: TryRead + Dispose từng RentedBuffer còn lại
3. Task.WhenAny(batchWorker, 2s)             // Chờ xả nốt batch cuối
4. Chờ ACK drain (2s)                        // Đợi đối tác xác nhận
5. Dispose remaining _unackedMessages         // Giải phóng RentedBuffer chưa ACK
6. _cts.Cancel()                             // Ngắt vòng lặp ngầm
7. Cancel _replyTasks (TrySetException + Dispose CTS)  // Hủy request đang chờ
8. Task.WhenAll(retryWorker, ackListener, 1s) // Chờ luồng tắt
9. Trigger.Dispose()                         // Dừng trigger monitor loop
10. Drain _mainThreadActions (NatifyClient only)  // Xả nốt callback trên main thread
11. _connection.DisposeAsync()               // Đóng kết nối NATS
12. _messageTtlWheel.OnExpired -= handler    // Unsubscribe event
13. _messageTtlWheel.Dispose()               // Giải phóng time wheel
14. _cts.Dispose()                           // Giải phóng CTS
```

---

## Complete API Reference

> **Lưu ý:** `NatifyServer`, `NatifyClient`, `NatifyClientFast` đều là `internal`. User tương tác qua interface `INatifyServer` / `INatifyClient` với factory method.

### INatifyServer

```csharp
public interface INatifyServer : IAsyncDisposable
```

**Factory:**
```csharp
static Task<INatifyServer> CreateAsync(string url, string serverName, string groupName,
    string clientNameToConnect, Config? config = null)
```

| Member | Signature |
|--------|-----------|
| `Publish` | `void Publish<T>(string topic, string regionId, T msg) where T : IMessage` |
| `OnMessage` | `void OnMessage<T>(string topic, Action<(string regionId, Data<T> data)> cb) where T : IMessage, new()` |
| `OnMessage` | `void OnMessage<T>(string topic, Func<(string regionId, Data<T> data), Task> cb) where T : IMessage, new()` |
| `RequestAsync` | `Task<TRes> RequestAsync<TReq, TRes>(string topic, string regionId, TReq data, TimeSpan timeout) where TReq : IMessage where TRes : IMessage, new()` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), TRep> handler) where TReq : IMessage, new() where TRep : IMessage` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), Task<TRep>> handlerAsync) where TReq : IMessage, new() where TRep : IMessage` |
| `Trigger` | `NatifyServerTriggers Trigger { get; }` |
| `DisposeAsync` | `ValueTask DisposeAsync()` |

**Tham số factory:**
- `url` — NATS URL (vd: `"nats://localhost:4222"`)
- `serverName` — Định danh server (xuất hiện trong NATS subject, **không chứa `.`**)
- `groupName` — **NATS queue group** → dùng chung cho tất cả instance cùng loại để scale ngang
- `clientNameToConnect` — Tên chính xác của client mà server giao tiếp. Server dùng giá trị này trong cả subscribe VÀ publish subject nên **không được dùng wildcard `"*"`**. Cần đặt trùng với `clientName` của `NatifyClient`/`NatifyClientFast` tương ứng.
- `config` — Tuỳ chỉnh tham số micro-batching (xem bên dưới)

### INatifyClient

```csharp
public interface INatifyClient : IAsyncDisposable
```

**Factory:**
```csharp
// Tạo NatifyClient (Unity — callback trên Main Thread qua Tick())
static Task<INatifyClient> Create(string url, string clientName, string groupName,
    string regionId, string serverNameToConnect, Config? config = null)

// Tạo NatifyClientFast (Backend — callback ngay trên ThreadPool)
static Task<INatifyClient> CreateFast(string url, string clientName, string groupName,
    string regionId, string serverNameToConnect, Config? config = null)
```

| Member | Signature |
|--------|-----------|
| `Publish` | `void Publish<T>(string topic, T msg) where T : IMessage` |
| `OnMessage` | `void OnMessage<T>(string topic, Action<Data<T>> cb) where T : IMessage, new()` |
| `OnMessage` | `void OnMessage<T>(string topic, Func<Data<T>, Task> cb) where T : IMessage, new()` |
| `RequestAsync` | `Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq data, TimeSpan timeout) where TReq : IMessage where TRes : IMessage, new()` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler) where TReq : IMessage, new() where TRep : IMessage` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync) where TReq : IMessage, new() where TRep : IMessage` |
| `Tick` | `void Tick()` — **Bắt buộc gọi trong Update()** với `Create()`, no-op với `CreateFast()` |
| `Trigger` | `NatifyClientTriggers Trigger { get; }` |
| `DisposeAsync` | `ValueTask DisposeAsync()` |

**Tham số factory:**
- `url` — NATS URL
- `clientName` — Định danh client (xuất hiện trong subject, **không chứa `.`**)
- `groupName` — **NATS queue group** cho scale ngang
- `regionId` — Region của client (vd: `"VN"`, `"US"`, **không chứa `.`**)
- `serverNameToConnect` — Tên server mà client giao tiếp
- `config` — Tuỳ chỉnh tham số micro-batching

### Data\<T\> — Message Envelope

```csharp
public readonly struct Data<T>
{
    public readonly T Value;           // Đã deserialize (hoặc ByteString nếu internal)
    public readonly string InstanceId; // Instance gửi
    public readonly string ReqId;      // Request correlation ID
    public readonly string RepId;      // Reply correlation ID
}
```

### NatifyServer callbacks — Tuple pattern

```csharp
server.OnMessage<MyMsg>("topic", tuple => {
    var regionId = tuple.regionId;  // region của client gửi
    var data     = tuple.data;      // Data<MyMsg> envelope
    var msg      = data.Value;      // MyMsg đã deserialize
});
```

### Trigger Telemetry

`NatifyClientTriggers` / `NatifyServerTriggers` — cả hai giống nhau, khác ở `BatchesSent` vs `BatchesReceived`:

| Property | Type | Mô tả |
|----------|------|-------|
| `BytesSent` | long | Tổng bytes đã gửi |
| `BytesReceived` | long | Tổng bytes đã nhận |
| `MessagesSent` | long | Tổng messages riêng lẻ đã gửi |
| `MessagesReceived` | long | Tổng messages riêng lẻ đã nhận |
| `BatchesSent` | long | Tổng batch đã gửi **(Client only)** |
| `BatchesReceived` | long | Tổng batch đã nhận **(Server only)** |
| `ErrorsCount` | long | Tổng lỗi |
| `CurrentDedupCacheSize` | long | Items trong dedup cache |
| `TotalDedupExpired` | long | Items đã hết hạn khỏi dedup |
| `ProcessMemoryMB` | double | RAM process (MB) |

| Method | Signature |
|--------|-----------|
| `RegisterTrigger` | `Guid RegisterTrigger(Func<T, bool> condition, Action<T> action, bool oneTime = false)` |
| `RemoveTrigger` | `void RemoveTrigger(Guid ruleId)` |

Triggers được đánh giá mỗi 500ms trên ThreadPool.

### Config — Micro-Batching Tuning

```csharp
public class Config
{
    public int MaxCount = 1000;                              // Số message tối đa trong 1 batch
    public int MaxSize = 50 * 1024;                          // Dung lượng tối đa (50 KB)
    public TimeSpan MaxWait = TimeSpan.FromMilliseconds(50); // Thời gian chờ tối đa trước flush
}
```

| Property | Type | Default | Mô tả |
|----------|------|---------|-------|
| `MaxCount` | int | 1000 | Số messages tối đa trước khi flush batch |
| `MaxSize` | int | 51200 | Payload tối đa (bytes) trước khi flush batch |
| `MaxWait` | TimeSpan | 50ms | Thời gian chờ tối đa từ message đầu tiên đến khi flush |

**Ví dụ:**
```csharp
var config = new Config {
    MaxCount = 500,
    MaxSize = 25 * 1024,
    MaxWait = TimeSpan.FromMilliseconds(30)
};
var server = await INatifyServer.CreateAsync("nats://localhost:4222", "GameServer", "SrvGroup", "Client1", config);
```

### NatifySerializer

| Method | Signature | Mô tả |
|--------|-----------|-------|
| `SerializeSimple<T>` | `byte[] SerializeSimple<T>(T msg) where T : IMessage` | Serialize đơn giản (allocate `byte[]`) |
| `SerializePooled<T>` | `RentedBuffer SerializePooled<T>(T msg) where T : IMessage` | Serialize vào `ArrayPool` buffer (zero-alloc) |
| `SerializeBatchPooled` | `RentedBuffer SerializeBatchPooled(payloads, reqIds, msgTypes, repIds, fromInstanceId)` | Manual proto3 wire format — không tạo `NatifyBatch` object |
| `Deserialize<T>` | `T Deserialize<T>(byte[] data, int length) where T : IMessage, new()` | Deserialize từ `byte[]` |
| `Deserialize<T>` | `T Deserialize<T>(ByteString data) where T : IMessage, new()` | Deserialize từ `ByteString` (zero-copy via `MergeFrom`) |

### NatifyLogger

```csharp
public static class NatifyLogger
{
    public static event Action<string>? OnInfo;
    public static event Action<string>? OnWarning;
    public static event Action<string>? OnError;
}
```

Hook event để nhận log từ Natify (vd: ghi ra Unity Console hoặc file log).

```csharp
NatifyLogger.OnError += msg => Debug.LogError(msg);
```

---

## Code Examples

### Pub/Sub cơ bản

```csharp
// Server
var server = await INatifyServer.CreateAsync("nats://localhost:4222", "GameServer", "SrvGroup", "Client1");
server.OnMessage<StringValue>("Chat", tuple => {
    Console.WriteLine($"[{tuple.regionId}] {tuple.data.Value.Value}");
});

// Client (Backend)
var client = await INatifyClient.CreateFast("nats://localhost:4222", "Client1", "Grp1", "VN", "GameServer");
client.Publish("Chat", new StringValue { Value = "Hello" });
```

### Request/Reply

```csharp
// Server xử lý request
server.OnRequest<StringValue, StringValue>("Login", tuple => {
    return new StringValue { Value = tuple.request.Value + "_OK" };
});

// Client gọi request
var reply = await client.RequestAsync<StringValue, StringValue>(
    "Login", new StringValue { Value = "user123" }, TimeSpan.FromSeconds(5));
Console.WriteLine(reply.Value); // "user123_OK"
```

### Unity Client

```csharp
var client = await INatifyClient.Create("nats://localhost:4222", "UnityClient", "GrpA", "VN", "GameServer");

client.OnMessage<Int32Value>("UpdateHealth", data => {
    healthBar.SetValue(data.Value.Value); // Chạy trên Main Thread sau Tick()
});

NatifyLogger.OnError += msg => Debug.LogError(msg);

void Update() {
    client.Tick(); // Bắt buộc!
}

async void OnDestroy() {
    await client.DisposeAsync();
}
```

### Scale ngang (N instances cùng loại)

```csharp
// Tất cả instance Router giống hệt factory call:
var router = await INatifyServer.CreateAsync("nats://localhost:4222", "Router", "RouterGroup", "AccountService");

// Tất cả instance Account giống hệt factory call:
var account = await INatifyClient.CreateFast("nats://localhost:4222", "AccountService", "AccountGroup", "ALL", "Router");

// Unity gửi event lên → NATS queue group "RouterGroup" → chỉ 1 Router instance nhận
// Router gửi request xuống Account → NATS queue group "AccountGroup" → chỉ 1 Account instance nhận
```

### Giám sát

```csharp
server.Trigger.RegisterTrigger(
    condition: t => t.ProcessMemoryMB > 1500,
    action: t => Console.WriteLine($"[OOM] RAM: {t.ProcessMemoryMB} MB!"),
    oneTime: false
);
```

---

## Changelog

- **v1.2.0** — Zero-copy send/receive pipeline: `RentedBuffer` (ArrayPool), `BatchAccumulator` + manual proto3 wire format (loại `NatifyBatch` object + `ByteString.CopyFrom`), `ByteString` pass-through trên receive (`MergeFrom` thay vì `ToByteArray`). Class → `internal`, expose qua `INatifyClient`/`INatifyServer` + factory method. `IAsyncDisposable`. Sửa thứ tự ACK (dedup → deserialize → ACK → callback). `NatifyLogger`. Validation trong `NatifyTopics`.
- **v1.0.2** — `NatifyClientFast` (Tick-free cho Backend). Cải thiện Request pipeline (200K RPS).
- **v1.0.1** — Fix request miss khi gửi nhiều request liên tục.
