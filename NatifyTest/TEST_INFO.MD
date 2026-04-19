# Tài liệu Kịch bản Kiểm thử - Natify Project (Bản Chi Tiết)

Tài liệu này quy định chi tiết 30 kịch bản kiểm thử (Test Cases) được thiết kế cho hệ thống Pub/Sub hiệu năng cao Natify. Hệ thống hỗ trợ Micro-Batching, Gửi tin cậy 2 chiều (Reliable Messaging), Chống trùng lặp (Deduplication) và Giám sát thời gian thực (Telemetry Triggers).

---

## Nhóm A: Kiểm thử Tích hợp & Chức năng (Integration Tests)

**01. ClientToServer_RegionExtract**
* **Mô tả:** Client (VN-01) gửi một gói tin `PlayerJoin` lên Server.
* **Kết quả kỳ vọng:** Server nhận được tin nhắn và hàm `ExtractRegionIdFromServerSubject` trích xuất chính xác chuỗi `"VN-01"`.

**02. ServerToClient_TickRequirement**
* **Mô tả:** Server gửi gói tin `UpdateHealth` xuống Client. Đợi một khoảng thời gian trước khi gọi hàm `Tick()` của Client.
* **Kết quả kỳ vọng:** Callback của Client **tuyệt đối không** được thực thi cho đến khi hàm `Tick()` được gọi. Sau khi gọi `Tick()`, callback chạy thành công.

**03. TwoWayPingPong**
* **Mô tả:** Client gửi lệnh Ping. Server bắt lệnh Ping và lập tức phản hồi lệnh Pong dựa vào RegionId của người gửi.
* **Kết quả kỳ vọng:** Client nhận được đúng chuỗi `Server_Acknowledged` trong khoảng thời gian cho phép (2 giây).

**04. BatchingLoadTest**
* **Mô tả:** Client dùng vòng lặp xả nhanh 100 tin nhắn liên tục (Mô phỏng đạn bắn).
* **Kết quả kỳ vọng:** Server nhận **đúng và đủ 100 tin nhắn**, không sót một tin nào. Bộ đếm trên Server khớp 100% với Client.

**05. WildcardRouting**
* **Mô tả:** 2 Client ở 2 khu vực khác nhau (VN-01 và US-West) cùng gửi tin nhắn vào Topic `GlobalChat`.
* **Kết quả kỳ vọng:** Server (lắng nghe bằng ký tự wildcard `*`) nhận được cả 2 tin nhắn và nhận diện đúng nguồn gốc Region của từng tin.

**06. LargePayload_NoCorruption**
* **Mô tả:** Client gửi một chuỗi văn bản dài 500,000 ký tự (~500KB) lên Server.
* **Kết quả kỳ vọng:** Server nhận được gói tin thành công. Chiều dài và nội dung chuỗi nhận được khớp 100% với chuỗi ban đầu (Không bị xé mảnh hay mất dữ liệu).

**07. MultiTopic_Concurrency**
* **Mô tả:** 3 luồng (Task) song song cùng lúc xả mỗi luồng 100 tin nhắn vào 3 Topic khác nhau (Chat, Move, HP).
* **Kết quả kỳ vọng:** Server nhận đủ 300 tin nhắn. Không có hiện tượng tin nhắn của Topic này bị nhảy nhầm sang callback của Topic kia.

**08. GracefulShutdown_CrashTest**
* **Mô tả:** Cố tình gọi lệnh `_client.Dispose()` ngay lập tức sau khi gọi lệnh `Publish`. Thử Publish tiếp sau khi đã Dispose.
* **Kết quả kỳ vọng:** Ứng dụng không bị Crash. Thư viện xử lý việc khóa Channel an toàn. Không có Exception rò rỉ làm sập Game/App.

**09. RequestReply_Sync**
* **Mô tả:** Client thực hiện gọi hàm `RequestAsync` lên Server. Server xử lý bằng hàm `OnRequest` (đồng bộ).
* **Kết quả kỳ vọng:** Client nhận được Data trả về trực tiếp từ Server thay vì báo lỗi Timeout.

**10. ConcurrentPublishers**
* **Mô tả:** Khởi tạo 10 Thread/Task, mỗi Thread cùng gọi hàm `Publish` 100 lần cùng một lúc.
* **Kết quả kỳ vọng:** Phễu gom lô (`Channel`) hoạt động an toàn đa luồng. Server nhận đúng 1000 tin nhắn mà không bị lỗi Race Condition.

**11. RPC_Timeout_Handling**
* **Mô tả:** Client gửi Request với Timeout 1 giây. Server cố tình thiết lập `Task.Delay(3000)` để xử lý trễ.
* **Kết quả kỳ vọng:** Client ném ra chính xác `TimeoutException` sau 1 giây và không bị kẹt luồng vĩnh viễn.

**12. PoisonPill_Resilience**
* **Mô tả:** Dùng NATS Connection raw bắn một mảng byte rác (không tuân theo định dạng Protobuf của Natify) vào Topic. Sau đó bắn tin hợp lệ.
* **Kết quả kỳ vọng:** Thread xử lý của Natify nuốt trọn lỗi, bỏ qua mảng byte rác và vẫn xử lý thành công tin nhắn hợp lệ ngay sau đó. Hệ thống không bị Crash.

**13. PartialBatchFlush**
* **Mô tả:** Client gửi 3 tin nhắn (chưa đủ điều kiện gom mốc 1000 tin).
* **Kết quả kỳ vọng:** Sau tối đa 50ms, hệ thống tự động chốt lô và xả (Flush) 3 tin nhắn này đi. Server nhận được sau tối đa ~60ms.

**14. RPC_AsyncHandler_Parallel**
* **Mô tả:** 2 Client cùng gọi Request lên Server. Logic xử lý trên Server tốn 1 giây (Bất đồng bộ).
* **Kết quả kỳ vọng:** Thời gian hoàn thành của cả 2 Request xấp xỉ 1 giây (chứng tỏ Server xử lý song song), thay vì 2 giây (xử lý tuần tự).

**15. ServerToClient_RPC_Tick**
* **Mô tả:** Server chủ động gọi `RequestAsync` xuống Topic của Client. Client nhận và trả lời thông qua luồng `Tick()`.
* **Kết quả kỳ vọng:** Server nhận được câu trả lời chính xác từ Client.

**16. RPC_OfflineClient_Timeout**
* **Mô tả:** Server gọi Request xuống một Client hiện không tồn tại hoặc đã mất kết nối.
* **Kết quả kỳ vọng:** Server bắt được lỗi Timeout an toàn thay vì bị treo (Deadlock).

**17. StrictThreadCheck**
* **Mô tả:** Ghi nhận ID của luồng gọi hàm `Tick()` và ID của luồng chạy hàm Callback `OnMessage`.
* **Kết quả kỳ vọng:** Hai ID này phải giống hệt nhau (Khẳng định tính an toàn cho Unity Main Thread).

**18. MassiveRPC_10000**
* **Mô tả:** Bắn 10,000 lệnh `RequestAsync` song song lên Server.
* **Kết quả kỳ vọng:** Nhận về 10,000 kết quả tương ứng. Dữ liệu Request số X phải khớp đúng với Response số X (Không bị lẫn lộn dữ liệu).

**19. UserCodeExceptionResilience**
* **Mô tả:** Logic Game bên trong Callback của Server chủ động ném ra `InvalidOperationException`.
* **Kết quả kỳ vọng:** Gói tin đó bị lỗi, nhưng hệ thống Catch được, ghi Log, và Server vẫn tiếp tục sống để phục vụ các Request tiếp theo.

**20. FIFO_OrderGuarantee**
* **Mô tả:** Client gửi các số nguyên liên tục từ 0 đến 999.
* **Kết quả kỳ vọng:** Server nhận và đưa vào List. Danh sách nhận được phải sắp xếp đúng thứ tự từ 0 -> 999, không bị đảo lộn.

**21. UTF8_Emoji_Support**
* **Mô tả:** Gửi chuỗi chứa ký tự tiếng Việt, tiếng Nhật và Emoji 🐉.
* **Kết quả kỳ vọng:** Chuỗi giải mã ở đầu nhận không bị lỗi Font (Encoding nguyên vẹn).

**22. Tick_EmptyQueue**
* **Mô tả:** Gọi hàm `Tick()` 100,000 lần liên tục khi không có tin nhắn nào.
* **Kết quả kỳ vọng:** Không văng lỗi NullReference và tốc độ thực thi gần như tức thời.

**23. ClientAsyncRPC**
* **Mô tả:** Client nhận Request từ Server, thực hiện Load Asset giả lập tốn 500ms (Bất đồng bộ), sau đó trả lời.
* **Kết quả kỳ vọng:** Server nhận được kết quả. Tổng thời gian tốn >= 500ms, và luồng Tick của Client không bị đơ trong lúc chờ.

---

## Nhóm B: Kiểm thử Hiệu năng (Performance Tests)

**24. Throughput_100000MPS**
* **Mô tả:** Xả 100,000 tin nhắn qua mạng nhanh nhất có thể.
* **Kết quả kỳ vọng:** Server nhận đủ 100,000 tin nhắn. Tốc độ (MPS - Messages Per Second) phải vượt mức 10,000 MPS. Test pass trong thời gian < 10 giây.

**25. Latency_AverageRTT**
* **Mô tả:** Thực hiện 1000 lệnh Ping-Pong tuần tự để đo độ trễ (Round-Trip Time).
* **Kết quả kỳ vọng:** Độ trễ trung bình cho mỗi lượt đi-về phải dưới 2ms (Trên môi trường Localhost).

**26. Memory_GC_Count**
* **Mô tả:** Gửi 50,000 tin nhắn. Ép Garbage Collection chạy trước khi test, và đếm số lần GC Gen 0 chạy trong quá trình test.
* **Kết quả kỳ vọng:** Số lần GC thu dọn bộ nhớ Gen 0 diễn ra < 10 lần (Khẳng định tính hiệu quả của cơ chế `ArrayPool` giúp tái sử dụng RAM).

---

## Nhóm C: Kiểm thử Độ tin cậy & Giám sát (Reliability & Metrics)

**27. Telemetry_Triggers**
* **Mô tả:** Khởi động hệ thống Trigger. Gửi 2,500 tin nhắn.
* **Kết quả kỳ vọng:** Chỉ số `MessagesSent` của Client và `MessagesReceived` của Server phải bằng đúng 2500. Số lượng `BatchesSent` phải lớn hơn 0 và nhỏ hơn 2500.

**28. Deduplication_AntiDupe**
* **Mô tả:** Cố tình giả mạo 3 lô tin nhắn (Batch) chứa cùng chung 1 mã `Natify-MsgId` và bắn liên tiếp vào Server.
* **Kết quả kỳ vọng:** Server chỉ chạy Callback đúng 1 lần cho lô đầu tiên. Hai lô sau bị bộ đệm Cache phát hiện trùng lặp và loại bỏ.

**29. BidirectionalReliability**
* **Mô tả:** Đảo ngược chiều gửi: Server dùng `PublishReliable` gửi 50 tin nhắn xuống cho Client.
* **Kết quả kỳ vọng:** Client nhận đúng 50 tin, không trùng lặp, giải nén đúng Batch và bắn trả 50 lệnh ACK lên cho Server.

**30. TimeWheel_GC_TTL**
* **Mô tả:** Nhận 1 gói tin tin cậy, lưu vào Cache. Hệ thống `TimedSortedSet` đếm ngược 10 giây. Chờ 11 giây và kiểm tra lại Trigger.
* **Kết quả kỳ vọng:** Chỉ số `CurrentDedupCacheSize` tự động giảm về 0, và `TotalDedupExpired` tăng lên (Chứng tỏ TimeWheel đã tự động xóa mã ID khỏi RAM để chống Memory Leak).

**31. ServerToClient_FanOut**
* **Mô tả:** Server chủ động gửi 2 gói tin khác nhau đến 2 Client thuộc 2 Region khác nhau (VN-01 và US-West).
* **Kết quả kỳ vọng:** Hệ thống định tuyến (Routing) hoạt động chính xác. Client ở Region nào chỉ nhận đúng dữ liệu của Region đó, không bị giao thoa (Cross-talk).

**32. ServerToClient_SlowClient**
* **Mô tả:** Server đẩy nhanh 500 tin nhắn. Game Client cố tình bị đơ (Tụt FPS), không gọi hàm `Tick()` trong 2 giây. Sau đó mới gọi `Tick()`.
* **Kết quả kỳ vọng:** Dữ liệu không bị mất mát hay gây Crash RAM. Mọi tin nhắn được giữ an toàn trong `ConcurrentQueue` và được xả chính xác 500 tin khi `Tick()` hoạt động trở lại.

**33. ServerToClient_Deduplication**
* **Mô tả:** Đóng giả mạng bị lỗi, Server cố tình gửi lại 3 lần cùng một lô hàng (chung 1 `Natify-MsgId`) xuống Client.
* **Kết quả kỳ vọng:** Lớp khiên bảo vệ `TimedSortedSet` của Client phát huy tác dụng. Callback tặng vật phẩm trong Game chỉ được chạy đúng 1 lần duy nhất, chặn đứng triệt để lỗi nhân bản đồ đạc (Dupe Item).

**34. ServerToClient_AsyncRPC_LargePayload**
* **Mô tả:** Server thực hiện lệnh Request gọi xuống Client yêu cầu nộp File Save Game. Client đọc file nặng 100KB (chạy bất đồng bộ) và trả về Server.
* **Kết quả kỳ vọng:** Hệ thống xử lý mượt mà dữ liệu lớn đi ngược chiều từ Client lên Server. Không bị đơ luồng Main Thread và dữ liệu nguyên vẹn 100,000 bytes.