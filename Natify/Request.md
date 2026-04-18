### Natify

Đây là dự án kết nối giữa các dịch vụ Microservice với nhau

#### Phụ thuộc

- Google.Protobuf: để mã hóa dữ liệu v.3.34.1
- NATS.Net: để giao tiếp v.2.7.3
- Microsoft.Extensions.ObjectPool: để quản lý bộ nhớ v.9.0.0

### Client `NatifyClient`

    Cách khởi tạo
```C#
var client = new NatifyClient("nats://localhost:4222", "client_name", "group_name", "regionId", "server_name_to_connect");
```
- client_name : là tên của client
- group_name : cách hoạt động giống group của NATS
- regionId : là id của client
- server_name_to_connect : là tên server mà client muốn kết nối đến

Cách nhận tin nhắn

```C#
client.OnMessage<IMessageProto>("topic", Action<IMessageProto> callback) : void
```
- topic : là tên chủ đề mà client muốn nhận tin nhắn
- callback : là hàm callback để xử lý tin nhắn khi nhận được

Cách gửi tin nhắn

```C#
client.Publish(string topic, IMessageProto message) : void
```

Request-Reply

```C#
var response = await client.RequestAsync<IReqProto, IResProto>("topic", requestData, timeout);
```

Subject NATS Client lắng nghe

`NatifyClient.<client_name>.<server_name>.<regionId>.<topic>`

Hàm `Tick()`
- Client sử dụng phải Tick liên tục để nhận tin nhắn
- Sử dụng trong Upadte của Unity

- Luồng CallBack của Subscribe sẽ là luồng từ Tick, đảm bảo rằng các callback sẽ được gọi trong luồng chính của Unity.

Xử lý ngoại lệ (Exception Handling) trong Unity Tick

> Tìm cách log sao cho chỉ hiển thị vị trí bên trong code gây ra lỗi của callback.
```c#
    try {
        callback(data);
    } catch (Exception ex) {
        Debug.LogError($"[Natify] Callback error on topic {topic}: {ex.Message}");
    }
```


Luồng nhận 

    Tin nhắn được nhận về từ Nats => đẩy vào queue => Tick sẽ lấy tin nhắn từ Queue => gọi callback để xử lý tin nhắn

Luồng gửi

    Tạo 1 chanel và 1 theard riêng để gửi, khi Publish -> đây tin nhắn vào chanel => theard sẽ lấy tin nhắn từ chanel và gửi đi => đảm bảo rằng việc gửi tin nhắn không bị ảnh hưởng bởi luồng chính của Unity

### Server `NatifyServer`

    Cách khởi tạo
```c#
var server = new NatifyServer("nats://localhost:4222", "server_name", "group_name" , "client_name_to_connect");
```
    
- server_name : là tên của server
- group_name : cách hoạt động giống group của NATS
- client_name_to_connect : là tên client mà server muốn kết nối đến
Cách nhận tin nhắn

```C#
server.OnMessage<IMessageProto>("topic", Action<(string regionId, IMessageProto data)> callback) : void
```
- topic : là tên chủ đề mà server muốn nhận tin nhắn
- callback : là hàm callback để xử lý tin nhắn khi nhận được, trong đó regionId là id của client gửi tin nhắn và data là nội dung tin nhắn

Cách gửi tin nhắn

```C#
server.Publish(string topic, string regionId, IMessageProto message) : void
```

Request-Reply

```C#
var response = await server.RequestAsync<IReqProto, IResProto>("topic", regionId, requestData, timeout);
```

Subject NATS Server lắng nghe

`NatifyServer.<server_name>.<client_name>.*.<topic>`

Luồng gửi
    Gửi luôn
Luồng nhận
    Nhận luôn

### Batching

- Config batch của NATS JetStream để kích hoạt tính năng Batching
- Tối đa 50 tin nhắn trong 1 batch hoặc tối đa 10 ms để gửi 1 batch, tùy điều kiện nào đến trước
- Áp dụng cả 2 chiều gửi và nhận

### Payload
- Bắt buộc các payload phải được định nghĩa dưới dạng Protobuf để đảm bảo tính tương thích và hiệu suất cao trong việc truyền dữ liệu giữa các dịch vụ Microservice.

### Dispose

cả `NatifyClient` và `NatifyServer` đều implement `IDisposable`, khi không sử dụng nữa thì nên gọi hàm `Dispose()` để giải phóng tài nguyên và đóng kết nối với NATS.