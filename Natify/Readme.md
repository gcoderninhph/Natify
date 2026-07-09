# Natify — High-Performance Communication Framework

> Built on NATS Core | Version 1.0.2 | netstandard2.0

Framework giao tiếp hiệu năng cao cho Microservice và Game Server (tối ưu Unity), xây trên nền NATS Core.

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Google.Protobuf | 3.34.1 | Serialization |
| NATS.Net (NATS.Client.Core) | 2.7.3 | Network transport |
| System.Threading.Channels | built-in | Lock-free queue |

**Không cần** NATS JetStream.

---

## Architecture Deep Dive

### 1. Queue Group & Horizontal Scaling

**Đây là điểm quan trọng nhất — dễ bị hiểu nhầm.**

Tham số `groupName` trong constructor **chính là NATS queue group**. Khi N instance cùng `groupName` subscribe cùng subject, NATS Core tự động giao mỗi message cho **đúng 1 instance** duy nhất:

```csharp
// Trong OnMessage — NatifyClient.cs:290 và NatifyServer.cs:293
_connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName, ...)
```

→ **Có thể scale N instance an toàn**, không lo duplicate processing.

**Ví dụ Router scale ngang:**
```csharp
// Router instance 1
new NatifyServer("nats://localhost:4222", "Router", "RouterGroup", "GameClient");

// Router instance 2 — cùng serverName + groupName
new NatifyServer("nats://localhost:4222", "Router", "RouterGroup", "GameClient");

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

### 3. Thread Model

| Class | Callback thread | Cần Tick()? |
|-------|----------------|-------------|
| `NatifyClient` (Unity) | Main Thread (qua `Tick()`) | **CÓ** |
| `NatifyClientFast` (Backend) | ThreadPool (ngay lập tức) | KHÔNG |
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
- Server luôn nhận được `regionId` trong callback để biết message từ region nào.

### 5. Request-Reply Mechanism

```
Client gọi RequestAsync("Login", req, timeout)
    │
    ├── Publish("Login", req, "REQ", out reqId)
    │       → NATS: NatifyServer.Srv.Client.VN.Login
    │
    ├──_replyTasks[reqId] = (TCS, CTS)     // chờ reply
    │
    │   Server xử lý OnRequest:
    │       → return reply
    │       → Publish("Rep-{instanceId}", reply, "REP", repId=reqId)
    │           → NATS: NatifyClient.Client.Srv.VN.Rep-{instanceId}
    │
    └── Nhận reply → _replyTasks[reqId].SetResult(bytes)
            → Deserialize<TRes> → return
```

Server trả lời qua topic `Rep-{instanceId}` — instanceId là `Guid.NewGuid()` của client, đảm bảo reply về đúng client gửi request.

### 6. Micro-Batching

Message được gom vào `Channel<(subject, payload, type, reqId, repId)>`, background worker flush batch khi đạt:
- **1000 messages** hoặc
- **50KB** payload hoặc
- **50ms** kể từ message đầu tiên

Batch được serialize thành `NatifyBatch` protobuf, publish kèm `Natify-BatchId` header.

### 7. Reliability (ACK + Retry)

Mỗi batch gửi đi được lưu vào `_unackedMessages`. Retry loop quét mỗi 100ms:
- Nếu `now - LastSent > 100ms` → retry (tối đa 10 lần)
- Receiver gửi ACK về `NatifyServer.{srv}.{client}.{region}.ACK.{batchId}`
- Khi nhận ACK → xóa khỏi `_unackedMessages`
- Receiver dùng dedup để bỏ qua batch đã xử lý nếu retry đến

### 8. Graceful Shutdown (Dispose)

```
1. batchChannel.Writer.Complete()           // Khóa van đầu vào
2. Task.WaitAll(batchWorker, 2s)            // Chờ xả nốt batch cuối
3. Chờ ACK drain (2s)                       // Đợi đối tác xác nhận
4. _cts.Cancel()                            // Ngắt vòng lặp ngầm
5. Task.WaitAll(retryWorker, ackListener, 1s) // Chờ luồng tắt
6. _connection.DisposeAsync()               // Đóng kết nối
7. _messageTtlWheel.Dispose()               // Giải phóng time wheel
```

---

## Complete API Reference

### NatifyServer

```csharp
public class NatifyServer : IDisposable
```

| Member | Signature |
|--------|-----------|
| **ctor** | `NatifyServer(string url, string serverName, string groupName, string clientNameToConnect, Config? config = null)` |
| `Publish` | `void Publish<T>(string topic, string regionId, T msg) where T : IMessage` |
| `OnMessage` | `void OnMessage<T>(string topic, Action<(string regionId, Data<T> data)> cb) where T : IMessage, new()` |
| `OnMessage` | `void OnMessage<T>(string topic, Func<(string regionId, Data<T> data), Task> cb)` |
| `RequestAsync` | `Task<TRes> RequestAsync<TReq, TRes>(string topic, string regionId, TReq data, TimeSpan timeout)` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq req), TRep> handler)` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq req), Task<TRep>> handlerAsync)` |
| `Trigger` | `NatifyServerTriggers Trigger { get; }` |
| `Dispose` | `void Dispose()` |

**Tham số constructor:**
- `url` — NATS URL (vd: `"nats://localhost:4222"`)
- `serverName` — Định danh server này (xuất hiện trong NATS subject)
- `groupName` — **NATS queue group** → dùng chung cho tất cả instance cùng loại để scale ngang
- `clientNameToConnect` — Tên chính xác của client mà server giao tiếp. Vì server dùng giá trị này trong cả subscribe VÀ publish subject nên **không được dùng wildcard `"*"`** (sẽ khiến publish/ACK gửi sai subject). Cần đặt trùng với `clientName` của `NatifyClient`/`NatifyClientFast` tương ứng.

### NatifyClient (Unity)

```csharp
public class NatifyClient : IDisposable
```

| Member | Signature |
|--------|-----------|
| **ctor** | `NatifyClient(string url, string clientName, string groupName, string regionId, string serverNameToConnect, Config? config = null)` |
| `Publish` | `void Publish<T>(string topic, T msg) where T : IMessage` |
| `OnMessage` | `void OnMessage<T>(string topic, Action<Data<T>> cb) where T : IMessage, new()` |
| `OnMessage` | `void OnMessage<T>(string topic, Func<Data<T>, Task> cb)` |
| `RequestAsync` | `Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq data, TimeSpan timeout)` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)` |
| `OnRequest` | `void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)` |
| `Tick` | `void Tick()` — **Bắt buộc gọi trong Update()**, xử lý tối đa 100 callback/frame |
| `Trigger` | `NatifyClientTriggers Trigger { get; }` |
| `Dispose` | `void Dispose()` |

### NatifyClientFast (Backend / Console)

```csharp
public class NatifyClientFast : IDisposable
```

API giống hệt `NatifyClient` nhưng:
- **Không có `Tick()`** — callback chạy ngay trên ThreadPool
- Callback truyền vào `OnMessage` nhận `Data<T>` (không qua hàng đợi main thread)

### Data\<T\> — Message Envelope

```csharp
public readonly struct Data<T>
{
    public T Value;           // Đã deserialize
    public string InstanceId; // Instance gửi
    public string ReqId;      // Request correlation ID
    public string RepId;      // Reply correlation ID
}
```

### NatifyServer callbacks — Tuple pattern

```csharp
server.OnMessage<MyMsg>("topic", tuple => {
    var regionId    = tuple.regionId;  // region của client gửi
    var data        = tuple.data;      // Data<MyMsg> envelope
    var msg         = data.Value;      // MyMsg đã deserialize
});
```

### Trigger Telemetry

`NatifyClientTriggers` / `NatifyServerTriggers` — cả hai giống nhau:

| Property | Type | Mô tả |
|----------|------|-------|
| `BytesSent` | long | Tổng bytes đã gửi |
| `BytesReceived` | long | Tổng bytes đã nhận |
| `MessagesSent` | long | Tổng messages riêng lẻ đã gửi |
| `MessagesReceived` | long | Tổng messages riêng lẻ đã nhận |
| `BatchesSent` / `BatchesReceived` | long | Tổng batch |
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

Tuỳ chỉnh tham số gom batch. Cả `NatifyServer`, `NatifyClient`, `NatifyClientFast` đều nhận `Config? config = null` ở tham số cuối constructor.

```csharp
public class Config
{
    public int MaxCount = 1000;                    // Số message tối đa trong 1 batch
    public int MaxSize = 50 * 1024;                // Dung lượng tối đa trong 1 batch (50 KB)
    public TimeSpan MaxWait = TimeSpan.FromMilliseconds(50); // Thời gian chờ tối đa trước khi flush
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
var server = new NatifyServer("nats://localhost:4222", "GameServer", "SrvGroup", "Client1", config);
```

---

## Code Examples

### Pub/Sub cơ bản

```csharp
// Server
var server = new NatifyServer("nats://localhost:4222", "GameServer", "SrvGroup", "Client1");
server.OnMessage<StringValue>("Chat", tuple => {
    Console.WriteLine($"[{tuple.regionId}] {tuple.data.Value.Value}");
});

// Client (Backend)
var client = new NatifyClientFast("nats://localhost:4222", "Client1", "Grp1", "VN", "GameServer");
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
var client = new NatifyClient("nats://localhost:4222", "UnityClient", "GrpA", "VN", "GameServer");

client.OnMessage<Int32Value>("UpdateHealth", data => {
    healthBar.SetValue(data.Value.Value); // Chạy trên Main Thread sau Tick()
});

void Update() {
    client.Tick(); // Bắt buộc!
}

void OnDestroy() {
    client.Dispose();
}
```

### Scale ngang (N instances cùng loại)

```csharp
// Tất cả instance Router giống hệt constructor:
var router = new NatifyServer("nats://localhost:4222", "Router", "RouterGroup", "AccountService");

// Tất cả instance Account giống hệt constructor:
var account = new NatifyClientFast("nats://localhost:4222", "AccountService", "AccountGroup", "ALL", "Router");

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

- **v1.0.2** — `NatifyClientFast` (Tick-free cho Backend). Cải thiện Request pipeline (200K RPS).
- **v1.0.1** — Fix request miss khi gửi nhiều request liên tục.
