Dự án `Natify` của bạn đã trải qua một cuộc lột xác ngoạn mục, từ một Wrapper NATS cơ bản trở thành một **hệ thống High-Performance Pub/Sub cấp độ công nghiệp**, với khả năng Micro-Batching, đảm bảo giao hàng (Reliability), chống trùng lặp (Deduplication) và giám sát thời gian thực (Telemetry).

Dưới đây là bản cập nhật hoàn chỉnh cho file `Request.md` (hoặc `README.md`) phản ánh chính xác kiến trúc mới nhất của bạn.

***

### Natify

Đây là một framework giao tiếp High-Performance dành cho kiến trúc Microservice và Game Server (đặc biệt tối ưu cho Unity), được xây dựng trên nền tảng NATS Core.

#### Phụ thuộc (Dependencies)
- **Google.Protobuf** (v3.34.1): Mã hóa dữ liệu siêu tốc.
- **NATS.Client.Core** (v2.7.3): Giao tiếp mạng (Không cần cấu hình JetStream phức tạp).
- **System.Threading.Channels**: Xử lý hàng đợi đa luồng Lock-free.

---

### Tính năng Nổi bật (Core Features)
1. **Micro-Batching 2 Chiều:** Tự động gom lô tin nhắn để tối ưu băng thông. Kích hoạt xả lô khi đạt 1.000 tin nhắn, hoặc 50KB, hoặc sau 50ms.
2. **Tin cậy cấp Ứng dụng (Reliability):** Tự động gửi ACK và Retry (thử lại) nếu gói tin bị rớt do lỗi mạng, đảm bảo không bao giờ mất tin nhắn (At-Least-Once Delivery).
3. **Chống trùng lặp siêu tốc (O(1) Deduplication):** Tích hợp `TimedSortedSet` (Time Wheel) dọn rác tự động sau 10 giây, chặn đứng lỗi nhân đôi dữ liệu (Dupe Bug) khi mạng chập chờn.
4. **Graceful Shutdown (Tắt mềm):** Cơ chế `Dispose()` an toàn, chờ vắt kiệt hàng đợi và xác nhận ACK trước khi ngắt mạng, chống treo Game/App.
5. **Telemetry & Triggers:** Tích hợp bộ đếm nguyên tử (Atomic Counters) để giám sát RAM, lưu lượng mạng và số lượng gói tin theo thời gian thực mà không làm nghẽn luồng chính.

---

### Client `NatifyClient` (Dành cho Unity / App)

Khởi tạo Client:
```csharp
var client = new NatifyClient("nats://localhost:4222", "client_name", "group_name", "regionId", "server_name_to_connect");
```
- `client_name`: Tên định danh của client.
- `group_name`: Consumer Group của NATS (chia sẻ tải).
- `regionId`: ID phân vùng của Client (VD: "VN-01").
- `server_name_to_connect`: Tên Server đích muốn kết nối tới.

#### 1. Nhận tin nhắn (Subscribe)
```csharp
client.OnMessage<IMessageProto>("topic", data => 
{
    // Xử lý dữ liệu
});
```
- **Lưu ý quan trọng cho Unity:** Callback truyền vào `OnMessage` sẽ **KHÔNG** chạy ngay lập tức. Nó được đẩy vào một hàng đợi an toàn. Bạn bắt buộc phải gọi hàm `client.Tick()` trong vòng lặp `Update()` của Unity để thực thi callback trên Main Thread.
```csharp
void Update()
{
    client.Tick(); // Lấy tối đa 100 action ra xử lý mỗi frame
}
```

#### 2. Gửi tin nhắn (Publish)
```csharp
client.Publish("topic", new IMessageProto { ... });
```
- Cơ chế gửi là **Fire-and-Forget ở luồng gọi**, nhưng bên dưới dữ liệu được đẩy vào `Channel`, gom thành Batch và gửi đi an toàn ở một luồng ngầm (`BatchWorker`), không làm giật lag FPS.

#### 3. Request - Reply (RPC)
Gửi yêu cầu và đợi kết quả (Bất đồng bộ):
```csharp
var response = await client.RequestAsync<IReqProto, IResProto>("topic", requestData, TimeSpan.FromSeconds(2));
```

Lắng nghe và trả lời Request từ Server (Chạy trên Unity Main Thread):
```csharp
// Dạng Đồng bộ (Xử lý logic nhanh)
client.OnRequest<IReqProto, IResProto>("topic", request => {
    return new IResProto { ... };
});

// Dạng Bất đồng bộ (Load Asset, đọc File)
client.OnRequest<IReqProto, IResProto>("topic", async request => {
    await Task.Delay(500); // Tác vụ nặng
    return new IResProto { ... };
});
```

---

### Server `NatifyServer` (Dành cho Game Server / Microservices)

Khởi tạo Server:
```csharp
var server = new NatifyServer("nats://localhost:4222", "server_name", "group_name", "client_name_to_connect");
```

#### 1. Nhận tin nhắn
```csharp
server.OnMessage<IMessageProto>("topic", callbackData => 
{
    string regionId = callbackData.regionId;
    IMessageProto data = callbackData.data;
});
```
- Khác với Client, Server xử lý dữ liệu đa luồng lập tức ngay khi nhận được, tối đa hóa sức mạnh CPU. Không cần gọi `Tick()`.

#### 2. Gửi tin nhắn xuống Client
```csharp
// Cần chỉ định đích danh RegionId của Client
server.Publish("topic", "VN-01", new IMessageProto { ... });
```

#### 3. Request - Reply (RPC)
Gửi yêu cầu xuống một Client cụ thể:
```csharp
var response = await server.RequestAsync<IReqProto, IResProto>("topic", "VN-01", requestData, TimeSpan.FromSeconds(2));
```

Xử lý Request từ Client gửi lên (Đa luồng):
```csharp
server.OnRequest<IReqProto, IResProto>("topic", incoming => 
{
    string clientRegion = incoming.regionId;
    IReqProto request = incoming.request;
    
    return new IResProto { ... };
});
```

---

### Giám sát Hệ thống (Telemetry & Triggers)

Cả `NatifyClient` và `NatifyServer` đều có thuộc tính `.Trigger`, cho phép bạn truy xuất các chỉ số thời gian thực và cài đặt cảnh báo.

**Xem chỉ số (VD: làm API Dashboard):**
```csharp
long totalSent = server.Trigger.MessagesSent;
double ramUsage = server.Trigger.ProcessMemoryMB;
long dedupCacheSize = server.Trigger.CurrentDedupCacheSize;
```

**Cài đặt Luật Cảnh Báo (Smart Triggers):**
Trigger sử dụng luồng chạy ngầm độc lập (Cold Path), không làm chậm tốc độ mạng (Hot Path).
```csharp
// Báo động nếu RAM quá 1.5 GB
server.Trigger.RegisterTrigger(
    condition: t => t.ProcessMemoryMB > 1500,
    action: t => Console.WriteLine($"[CẢNH BÁO OOM] RAM đang ở mức {t.ProcessMemoryMB} MB!"),
    oneTime: false
);

// In log định kỳ sau mỗi 100.000 tin nhắn
server.Trigger.RegisterTrigger(
    condition: t => t.MessagesReceived % 100000 == 0 && t.MessagesReceived > 0,
    action: t => Console.WriteLine($"Đã xử lý: {t.MessagesReceived} tin nhắn."),
);
```

---

### Quy ước Chủ đề (Routing Topics)
Hệ thống sử dụng bộ định tuyến ngầm định trong `NatifyTopics`:
- Client lắng nghe: `NatifyClient.<client_name>.<server_name>.<regionId>.<topic>`
- Server lắng nghe: `NatifyServer.<server_name>.<client_name>.*.<topic>`
- *(Ký tự `*` cho phép Server gom tập trung tin nhắn từ mọi Region vào chung một đầu mối xử lý).*

---

### Quản lý Vòng đời (Dispose & Graceful Shutdown)

Hệ thống được thiết kế **Tắt Mềm (Graceful Shutdown)** để bảo vệ dữ liệu.
Khi gọi `client.Dispose()` hoặc `server.Dispose()`:
1. Van đầu vào bị khóa (Không nhận thêm lệnh `Publish` mới).
2. Hệ thống sẽ đợi tối đa **2 giây** để các luồng ngầm gom nốt Batch cuối cùng và chờ nhận đủ `ACK` từ phía đối diện.
3. Giải phóng bộ nhớ TimeWheel.
4. An toàn đóng kết nối NATS.

Bắt buộc phải gọi `.Dispose()` khi tắt ứng dụng hoặc khi đối tượng không còn được sử dụng để tránh Memory Leak.