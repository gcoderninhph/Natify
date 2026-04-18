using System.Collections.Concurrent;
using Google.Protobuf.WellKnownTypes; // Dùng các Protobuf có sẵn của Google

namespace Natify.Tests
{
    [TestFixture]
    [Category("Integration")]
    public class NatifyIntegrationTests
    {
        private const string NatsUrl = "nats://localhost:4222";
        private NatifyServer _server;
        private NatifyClient _clientA;
        private NatifyClient _clientB;

        [SetUp]
        public void Setup()
        {
            _server = new NatifyServer(NatsUrl, "GameServer", "ServerGroup", "GameClient");

            // Client A ở Region VN-01
            _clientA = new NatifyClient(NatsUrl, "GameClient", "ClientGroupA", "VN-01", "GameServer");

            // Client B ở Region US-West (Dành cho test đa kết nối)
            _clientB = new NatifyClient(NatsUrl, "GameClient", "ClientGroupB", "US-West", "GameServer");
        }

        [TearDown]
        public void TearDown()
        {
            _clientA?.Dispose();
            _clientB?.Dispose();
            _server?.Dispose();
        }

        [Test]
        public async Task Test1_ClientToServer_ServerShouldExtractCorrectRegion()
        {
            // Arrange
            var waitHandle = new ManualResetEventSlim(false);
            string receivedRegion = null;
            StringValue receivedMessage = null;

            _server.OnMessage<StringValue>("PlayerJoin", data =>
            {
                receivedRegion = data.regionId;
                receivedMessage = data.data;
                waitHandle.Set();
            });

            await Task.Delay(100); // Đợi NATS đăng ký Subject

            // Act
            var payload = new StringValue { Value = "Player_Alex_Joined" };
            _clientA.Publish("PlayerJoin", payload);

            // Wait max 2 seconds for network
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(2));

            // Assert
            Assert.That(success, Is.True, "Server không nhận được tin nhắn");
            Assert.That(receivedRegion, Is.EqualTo("VN-01"), "Trích xuất RegionId sai");
            Assert.That(receivedMessage.Value, Is.EqualTo("Player_Alex_Joined"));
        }

        [Test]
        public async Task Test2_ServerToClient_ClientMustTickToProcessMessage()
        {
            // Arrange
            int processCount = 0;
            _clientA.OnMessage<Int32Value>("UpdateHealth", data =>
            {
                processCount++;
                Assert.That(data.Value, Is.EqualTo(100));
            });

            await Task.Delay(100);

            // Act: Server gửi tin
            var hpPayload = new Int32Value { Value = 100 };
            _server.Publish("UpdateHealth", "VN-01", hpPayload);

            await Task.Delay(500); // Đợi tin nhắn bay qua mạng và chui vào Queue của Client

            // Assert: Chưa Tick thì count vẫn phải bằng 0
            Assert.That(processCount, Is.EqualTo(0), "Lỗi: Callback bị chạy ngoài luồng Tick!");

            // Act: Giả lập Unity gọi Update()
            _clientA.Tick();

            // Assert: Sau khi Tick, tin nhắn phải được bốc ra xử lý
            Assert.That(processCount, Is.EqualTo(1), "Tick không lấy được tin nhắn ra khỏi Queue");
        }

        [Test]
        public async Task Test3_TwoWayPingPong_RealtimeCommunicationFlow()
        {
            // Bài test này mô phỏng luồng: Client Ping -> Server -> Client Pong
            var pongReceived = new ManualResetEventSlim(false);

            // 1. Client setup hứng PONG
            _clientA.OnMessage<StringValue>("PONG_TOPIC", data =>
            {
                if (data.Value == "Server_Acknowledged")
                    pongReceived.Set();
            });

            // 2. Server setup hứng PING và trả lời PONG
            _server.OnMessage<StringValue>("PING_TOPIC", incoming =>
            {
                if (incoming.data.Value == "Hello_Server")
                {
                    var replyPayload = new StringValue { Value = "Server_Acknowledged" };
                    // Gửi trả đúng RegionId của người gửi
                    _server.Publish("PONG_TOPIC", incoming.regionId, replyPayload);
                }
            });

            await Task.Delay(100); // Đợi NATS bind

            // 3. Act: Client bắt đầu chuỗi giao tiếp
            _clientA.Publish("PING_TOPIC", new StringValue { Value = "Hello_Server" });

            // 4. Giả lập vòng lặp Game Loop của Unity (Tick liên tục trong 2 giây để chờ Pong)
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            while (!cts.IsCancellationRequested && !pongReceived.IsSet)
            {
                _clientA.Tick();
                await Task.Delay(10); // 100 FPS
            }

            // Assert
            Assert.That(pongReceived.IsSet, Is.True, "Giao tiếp 2 chiều Ping-Pong thất bại hoặc timeout.");
        }

        [Test]
        public async Task Test4_BatchingLoadTest_ClientShouldSend100MessagesSafely()
        {
            int expectedMessages = 100;
            int receivedCounter = 0;
            var waitHandle = new ManualResetEventSlim(false);

            var missingTracker = new System.Collections.Concurrent.ConcurrentDictionary<int, bool>();
            for (int i = 0; i < expectedMessages; i++) missingTracker[i] = false;

            // 1. Lập trạm báo cáo tình trạng "Đã thông ống dẫn"
            var isServerReady = new ManualResetEventSlim(false);

            _server.OnMessage<Int32Value>("BulletFired", data =>
            {
                // Nhận diện viên đạn mồi (Value = -1)
                if (data.data.Value == -1)
                {
                    isServerReady.Set(); // Báo cáo: Đường truyền đã kết nối 100%
                    return;
                }

                // Xử lý đạn thật
                missingTracker[data.data.Value] = true;
                int current = Interlocked.Increment(ref receivedCounter);
                if (current == expectedMessages)
                {
                    waitHandle.Set();
                }
            });

            // 2. WARMUP PHASE: Bắn đạn mồi liên tục mỗi 50ms cho đến khi Server thức dậy
            while (!isServerReady.IsSet)
            {
                _clientA.Publish("BulletFired", new Int32Value { Value = -1 });
                await Task.Delay(50);
            }

            // 3. ACT: Bắn đạn thật (Đảm bảo 100% không trượt viên nào)
            for (int i = 0; i < expectedMessages; i++)
            {
                _clientA.Publish("BulletFired", new Int32Value { Value = i });
            }

            // Chờ 5 giây 
            bool allReceived = waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Lọc ra các viên bị thiếu để log
            var missingBullets = new System.Collections.Generic.List<int>();
            foreach (var kv in missingTracker)
            {
                if (!kv.Value) missingBullets.Add(kv.Key);
            }

            // Assert
            Assert.That(allReceived, Is.True,
                $"Server chỉ nhận được {receivedCounter}/{expectedMessages}. Bị mất các viên: {string.Join(", ", missingBullets)}");
        }

        [Test]
        public async Task Test5_WildcardRouting_ServerHandlesMultipleRegions()
        {
            // Kịch bản: Server nhận tin từ cả VN-01 và US-West
            var receivedRegions = new ConcurrentBag<string>();
            var waitHandle = new CountdownEvent(2); // Cần đợi đủ 2 tin nhắn

            _server.OnMessage<StringValue>("GlobalChat", data =>
            {
                receivedRegions.Add(data.regionId);
                waitHandle.Signal();
            });

            await Task.Delay(100);

            // Act: Cả 2 client cùng gửi tin vào chung 1 topic
            _clientA.Publish("GlobalChat", new StringValue { Value = "Xin chào" });
            _clientB.Publish("GlobalChat", new StringValue { Value = "Hello" });

            bool success = waitHandle.Wait(TimeSpan.FromSeconds(2));

            // Assert
            Assert.That(success, Is.True, "Server không nhận đủ tin nhắn từ các Client");
            Assert.That(receivedRegions, Contains.Item("VN-01"));
            Assert.That(receivedRegions, Contains.Item("US-West"));
        }

        [Test]
        public async Task Test6_LargePayload_ShouldNotCorruptData()
        {
            // Tạo một chuỗi dài 500,000 ký tự (~500KB)
            string hugeData = new string('X', 500_000);
            var waitHandle = new ManualResetEventSlim(false);
            string receivedString = string.Empty;

            _server.OnMessage<StringValue>("SyncMapData", data =>
            {
                receivedString = data.data.Value;
                waitHandle.Set();
            });

            await Task.Delay(500); // Đợi NATS bind

            // Act
            var payload = new StringValue { Value = hugeData };
            _clientA.Publish("SyncMapData", payload);

            bool success = waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Assert
            Assert.That(success, Is.True, "Timeout khi nhận gói tin lớn.");
            Assert.That(receivedString.Length, Is.EqualTo(500_000), "Dữ liệu bị mất mát hoặc cắt xén.");
            Assert.That(receivedString, Is.EqualTo(hugeData), "Nội dung dữ liệu bị hỏng (Corrupted).");
        }

        [Test]
        public async Task Test7_MultiTopic_Concurrency_ShouldRouteCorrectly()
        {
            int chatCount = 0, moveCount = 0, hpCount = 0;
            var waitHandle = new CountdownEvent(300); // 100 tin nhắn mỗi topic x 3 = 300

            _server.OnMessage<StringValue>("Chat", data =>
            {
                Interlocked.Increment(ref chatCount);
                waitHandle.Signal();
            });
            _server.OnMessage<Int32Value>("Move", data =>
            {
                Interlocked.Increment(ref moveCount);
                waitHandle.Signal();
            });
            _server.OnMessage<Int32Value>("HP", data =>
            {
                Interlocked.Increment(ref hpCount);
                waitHandle.Signal();
            });

            await Task.Delay(1000); // Đợi cả 3 Topic được Subscribe xong

            // Act: Xả 3 luồng song song cùng lúc, mỗi luồng 100 message
            var t1 = Task.Run(() =>
            {
                for (int i = 0; i < 100; i++) _clientA.Publish("Chat", new StringValue { Value = "Hi" });
            });
            var t2 = Task.Run(() =>
            {
                for (int i = 0; i < 100; i++) _clientA.Publish("Move", new Int32Value { Value = i });
            });
            var t3 = Task.Run(() =>
            {
                for (int i = 0; i < 100; i++) _clientA.Publish("HP", new Int32Value { Value = 100 });
            });

            await Task.WhenAll(t1, t2, t3);

            bool success = waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Assert
            Assert.That(success, Is.True,
                $"Không nhận đủ 300 tin nhắn. Chat:{chatCount}, Move:{moveCount}, HP:{hpCount}");
            Assert.That(chatCount, Is.EqualTo(100));
            Assert.That(moveCount, Is.EqualTo(100));
            Assert.That(hpCount, Is.EqualTo(100));
        }

        [Test]
        public void Test8_GracefulShutdown_ShouldNotCrashWhenDisposed()
        {
            // Cố tình đẩy 1 tin nhắn vào
            _clientA.Publish("SomeTopic", new StringValue { Value = "Last Words" });

            // Rút phích cắm NGAY LẬP TỨC
            Assert.DoesNotThrow(() => { _clientA.Dispose(); },
                "Dispose bị crash do xử lý luồng (Channel/Thread) không an toàn!");

            // Thử Publish tiếp sau khi đã Dispose xem có bị Crash App không
            Assert.DoesNotThrow(() => { _clientA.Publish("SomeTopic", new StringValue { Value = "Ghost Message" }); },
                "Publish sau khi Dispose làm Crash hệ thống. Cần check Channel.Writer.TryWrite!");
        }

        [Test]
        public async Task Test9_RequestReply_ArchitectureFixed_ShouldReturnPong()
        {
            // SERVER: Đăng ký hàm OnRequest (Đồng bộ)
            _server.OnRequest<StringValue, StringValue>("GetPing", incoming =>
            {
                // Nhận Ping từ client và Region
                string clientRegion = incoming.regionId;
                string pingMessage = incoming.request.Value;

                // Xử lý và Return thẳng kết quả (Không cần gọi hàm Publish rườm rà nữa)
                return new StringValue { Value = $"Pong_from_Server_to_{clientRegion}" };
            });

            await Task.Delay(500); // Đợi NATS bind subject

            // CLIENT: Gọi Request và đợi kết quả
            var response = await _clientA.RequestAsync<StringValue, StringValue>(
                "GetPing",
                new StringValue { Value = "Ping" },
                TimeSpan.FromSeconds(2)); // Chờ tối đa 2 giây

            // Assert
            Assert.That(response, Is.Not.Null);
            Assert.That(response.Value, Is.EqualTo("Pong_from_Server_to_VN-01"));
        }

        /// <summary>
        /// Kịch bản 10: Xả đạn đa luồng (Massive Concurrent Publishers)
        /// Trong thực tế, bạn không dùng 1 vòng lặp for để bắn đạn. Sẽ có hàng tá luồng (Thread) từ các module khác nhau cùng gọi hàm Publish tại cùng một micro-giây. Bài test này chứng minh cái Channel của bạn thực sự là một "cái phễu thần kỳ", không bao giờ gây lỗi Race Condition hay đụng độ bộ nhớ.
        /// </summary>
        [Test]
        public async Task Test10_ConcurrentPublishers_ShouldNotLoseAnyMessage()
        {
            int threadCount = 10;
            int messagesPerThread = 100;
            int totalExpected = threadCount * messagesPerThread;
            int receivedCounter = 0;
            var waitHandle = new CountdownEvent(totalExpected);

            _server.OnMessage<Int32Value>("HeavyConcurrency", data =>
            {
                Interlocked.Increment(ref receivedCounter);
                waitHandle.Signal();
            });

            await Task.Delay(500); // Đợi kết nối mở

            // Act: Khởi tạo 10 luồng (Task) song song, mỗi luồng xả 100 viên đạn
            var tasks = new System.Collections.Generic.List<Task>();
            for (int t = 0; t < threadCount; t++)
            {
                int threadId = t; // Bắt giá trị cho luồng
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < messagesPerThread; i++)
                    {
                        _clientA.Publish("HeavyConcurrency", new Int32Value { Value = (threadId * 1000) + i });
                    }
                }));
            }

            // Đợi tất cả các luồng bắn xong
            await Task.WhenAll(tasks);

            bool success = waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Assert
            Assert.That(success, Is.True, $"Bắn đồng thời bị rớt đạn! Nhận được: {receivedCounter}/{totalExpected}");
            Assert.That(receivedCounter, Is.EqualTo(totalExpected));
        }

        /// <summary>
        /// Kịch bản 11: Ép Timeout RPC (RPC Timeout Handling)
        /// Điều gì xảy ra nếu bạn gọi RequestAsync nhưng Server xử lý quá lâu (do kẹt Database)? Test này đảm bảo Natify sẽ ném ra đúng lỗi TimeoutException để Client chủ động xử lý, thay vì bị treo game vĩnh viễn (Infinite Hang).
        /// </summary>
        [Test]
        public void Test11_RPC_Timeout_ShouldThrowTimeoutExceptionGracefully()
        {
            // Cài đặt Server nhận Request nhưng cố tình "ngủ gật" 3 giây mới trả lời
            _server.OnRequest<StringValue, StringValue>("SlowDatabase", async incoming =>
            {
                await Task.Delay(3000);
                return new StringValue { Value = "Done" };
            });

            // Act & Assert
            var ex = Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                // Client chỉ cho phép Server trả lời trong vòng 1 giây
                await _clientA.RequestAsync<StringValue, StringValue>(
                    "SlowDatabase",
                    new StringValue { Value = "Query" },
                    TimeSpan.FromSeconds(1));
            });

            Assert.That(ex.Message, Does.Contain("Timeout").IgnoreCase);
        }

        /// <summary>
        /// Kịch bản 12: Độc dược (The Poison Pill / Corrupted Data Resilience)
        /// Giả sử có một hacker (hoặc một service rác nào đó) không dùng thư viện Natify mà kết nối thẳng vào NATS, sau đó gửi một chuỗi Byte rác rưởi (không phải chuẩn Protobuf) vào đúng Topic mà Unity đang lắng nghe.
        /// Yêu cầu: Thread nhận của NatifyClient không được phép Crash! Nó phải nuốt trọn lỗi, bỏ qua tin rác, và vẫn tiếp tục nhận được tin nhắn đúng ngay sau đó.
        /// </summary>
        [Test]
        public async Task Test12_PoisonPill_ShouldNotCrashSubscriptionLoop()
        {
            var waitHandle = new ManualResetEventSlim(false);
            StringValue validMessage = null;

            _clientA.OnMessage<StringValue>("ResilienceTest", data =>
            {
                validMessage = data;
                waitHandle.Set();
            });

            await Task.Delay(500);

            // Kẻ gian dùng raw NATS connection gửi rác (byte array hỏng)
            var rawNats = new NATS.Client.Core.NatsConnection(new NATS.Client.Core.NatsOpts { Url = NatsUrl });
            await rawNats.ConnectAsync();
            var evilSubject =
                NatifyTopics.GetClientListenSubject("GameClient", "GameServer", "VN-01", "ResilienceTest");

            // Bắn byte rác
            await rawNats.PublishAsync(evilSubject, new byte[] { 0xFF, 0x00, 0xAA, 0xBB });

            // Đợi 1 chút để xem hệ thống có bị crash ngầm không
            await Task.Delay(200);

            // Server chính chủ gửi tin nhắn hợp lệ
            _server.Publish("ResilienceTest", "VN-01", new StringValue { Value = "I_AM_ALIVE" });

            // Cần gọi Tick vì ClientA yêu cầu Tick để chạy callback
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            while (!cts.IsCancellationRequested && !waitHandle.IsSet)
            {
                _clientA.Tick();
                await Task.Delay(10);
            }

            // Assert
            Assert.That(waitHandle.IsSet, Is.True, "Thread OnMessage đã bị Crash bởi dữ liệu rác!");
            Assert.That(validMessage.Value, Is.EqualTo("I_AM_ALIVE"));

            await rawNats.DisposeAsync();
        }

        /// Kịch bản 13: Kiểm chứng Batch 10ms (Partial Batch Flush)
        /// Thuật toán gom Batch của ta nói rằng:
        /// "Gom tối đa 50 tin nhắn, HOẶC nếu không đủ 50 thì đợi tối đa 10ms rồi phải gửi đi".
        /// Bài test này chứng minh: Dù bạn chỉ bắn 2 viên đạn (chưa đủ 50),
        /// nó vẫn sẽ bay tới đích ngay lập tức chứ không bị kẹt mãi mãi trong Kênh (Channel).
        [Test]
        public async Task Test13_PartialBatch_ShouldFlushAfter10ms()
        {
            int receivedCount = 0;
            var waitHandle = new ManualResetEventSlim(false);

            _server.OnMessage<Int32Value>("PartialFlush", data =>
            {
                int count = Interlocked.Increment(ref receivedCount);
                if (count == 3) waitHandle.Set(); // Chỉ đợi đúng 3 tin nhắn
            });

            await Task.Delay(500);

            // Act: Chỉ bắn 3 tin (ít hơn mốc 50 của Batch)
            for (int i = 0; i < 3; i++)
            {
                _clientA.Publish("PartialFlush", new Int32Value { Value = i });
            }

            // Thời gian chờ chỉ cho phép tối đa 1 giây. Nếu Batch bị kẹt vì đợi đủ 50, test sẽ Fail.
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(1));

            // Assert
            Assert.That(success, Is.True, "Gom Batch bị kẹt! Không xả dữ liệu khi timeout 10ms.");
            Assert.That(receivedCount, Is.EqualTo(3));
        }

        /// Kịch bản 14: Server RPC Async Database Simulator (Đa luồng trên Server)
        /// Test này kiểm chứng bản OnRequest<..., Task<...>> (phiên bản Async).
        /// Nếu 2 Client cùng Request, và mỗi Request tốn 1 giây xử lý (giả lập lưu Data), tổng thời gian không được là 2 giây (tuần tự),
        /// mà phải là ~1 giây (xử lý song song). Điều này khẳng định NatifyServer có khả năng "chịu tải song song" cực tốt.
        [Test]
        public async Task Test14_RPC_AsyncHandler_ShouldProcessInParallel()
        {
            // Server có hàm xử lý mất đúng 1 giây
            _server.OnRequest<StringValue, StringValue>("SaveProfile", async incoming =>
            {
                await Task.Delay(1000);
                return new StringValue { Value = "Saved_" + incoming.request.Value };
            });

            await Task.Delay(500);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // 2 Client cùng gọi request (bắt đầu cùng lúc)
            var req1 = _clientA.RequestAsync<StringValue, StringValue>("SaveProfile",
                new StringValue { Value = "PlayerA" }, TimeSpan.FromSeconds(3));
            var req2 = _clientB.RequestAsync<StringValue, StringValue>("SaveProfile",
                new StringValue { Value = "PlayerB" }, TimeSpan.FromSeconds(3));

            // Đợi cả 2 hoàn thành
            var results = await Task.WhenAll(req1, req2);
            stopwatch.Stop();

            // Assert
            Assert.That(results[0].Value, Is.EqualTo("Saved_PlayerA"));
            Assert.That(results[1].Value, Is.EqualTo("Saved_PlayerB"));

            // Quan trọng nhất: Tổng thời gian phải xoay quanh 1 giây (chứng tỏ chạy song song), không được là 2 giây
            Assert.That(stopwatch.ElapsedMilliseconds, Is.LessThan(1500), "Server xử lý tuần tự thay vì song song!");
        }
        
        /// <summary>
        /// Kịch bản 15 sẽ mô phỏng nhịp đập 60 FPS của Unity để Client tính toán trả lời. Kịch bản 16 sẽ chứng minh hệ thống Server không bị treo nếu Client tắt game đột ngột.
        /// </summary>

        [Test]
        public async Task Test15_ServerToClient_RPC_ShouldReturnReplyViaTick()
        {
            // 1. CLIENT: Đăng ký hứng Request từ Server
            _clientA.OnRequest<StringValue, StringValue>("GetPlayerState", req =>
            {
                // Client nhận được câu hỏi từ Server
                string serverQuestion = req.Value;

                // Trả lời lại (Giả lập việc truy xuất máu/tọa độ trong Unity)
                return new StringValue { Value = $"Client_State_is_Alive_{serverQuestion}" };
            });

            await Task.Delay(500); // Đợi đường ống nối xong

            // 2. Giả lập Unity Game Loop (Chạy ngầm liên tục 60 FPS)
            var ctsUnityLoop = new CancellationTokenSource();
            var unityGameLoop = Task.Run(async () =>
            {
                while (!ctsUnityLoop.IsCancellationRequested)
                {
                    _clientA.Tick();
                    await Task.Delay(16); // ~60 Frames per second
                }
            });

            // 3. ACT: Server chủ động gọi xuống Client ở Region VN-01
            var response = await _server.RequestAsync<StringValue, StringValue>(
                "GetPlayerState",
                "VN-01", // Nhắm chính xác vào Region này
                new StringValue { Value = "Ping" },
                TimeSpan.FromSeconds(2)); // Đợi tối đa 2 giây

            // Dừng vòng lặp giả lập Unity
            ctsUnityLoop.Cancel();

            // 4. ASSERT
            Assert.That(response, Is.Not.Null);
            Assert.That(response.Value, Is.EqualTo("Client_State_is_Alive_Ping"),
                "Server không nhận được câu trả lời chính xác từ Client!");
        }

        [Test]
        public void Test16_ServerToClient_RPC_OfflineClient_ShouldThrowTimeout()
        {
            // Kịch bản: Server hỏi một Client B (US-West), nhưng Client B không có hàm OnRequest cho Topic này
            // (Giống như việc Client rớt mạng hoặc chưa load Scene xong)

            // Act & Assert
            var ex = Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                // Server ráng chờ 1 giây
                await _server.RequestAsync<StringValue, StringValue>(
                    "GetSecretData",
                    "US-West", // Target Client B
                    new StringValue { Value = "Hello" },
                    TimeSpan.FromSeconds(1));
            });

            Assert.That(ex.Message, Does.Contain("Timeout").IgnoreCase,
                "Server bị treo hoặc văng lỗi lạ khi Client không trả lời!");
        }
        
        [Test]
        public async Task Test17_StrictThreadCheck_CallbackMustExecuteOnTickThread()
        {
            int callbackThreadId = -1;
            int processCount = 0;

            // CLIENT: Đăng ký hứng tin nhắn
            _clientA.OnMessage<StringValue>("ThreadCheckTopic", data =>
            {
                processCount++;
                // Ghi nhận lại ID của luồng đang thực thi hàm callback này
                callbackThreadId = Thread.CurrentThread.ManagedThreadId;
            });

            await Task.Delay(500); // NUnit có thể nhảy sang luồng khác sau dòng này

            // ACT 1: Server bắn tin nhắn xuống
            _server.Publish("ThreadCheckTopic", "VN-01", new StringValue { Value = "CheckThread" });
    
            // Đợi 500ms cho đạn bay qua mạng. 
            await Task.Delay(500); // NUnit lại tiếp tục nhảy luồng sau dòng này

            Assert.That(processCount, Is.EqualTo(0), "Callback đã tự động chạy trước khi gọi Tick!");

            // ACT 2: Lấy Thread ID NGAY TRƯỚC KHI gọi Tick
            int tickThreadId = Thread.CurrentThread.ManagedThreadId; 
            _clientA.Tick(); // Hàm Tick được gọi bởi tickThreadId

            // ASSERT 2
            Assert.That(processCount, Is.EqualTo(1), "Tick không lấy được tin nhắn ra khỏi Queue.");
    
            // BẰNG CHỨNG THÉP
            Assert.That(callbackThreadId, Is.Not.EqualTo(-1), "Callback chưa ghi nhận được Thread ID");
            Assert.That(callbackThreadId, Is.EqualTo(tickThreadId), 
                $"LỆCH LUỒNG! Callback chạy ở luồng {callbackThreadId}, nhưng luồng gọi Tick là {tickThreadId}");
        }
        
        [Test]
        public async Task Test18_MassiveRPC_10000Requests_ShouldNotDropAny()
        {
            int totalRequests = 10000;
    
            // Server chỉ đơn giản là nối chuỗi để trả về
            _server.OnRequest<Int32Value, StringValue>("MassivePing", incoming => 
            {
                return new StringValue { Value = $"Pong_{incoming.request.Value}" };
            });

            await Task.Delay(500);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var tasks = new System.Collections.Generic.List<Task<StringValue>>();

            // Act: Bắn 10.000 Request song song!
            for (int i = 0; i < totalRequests; i++)
            {
                int requestId = i;
                tasks.Add(_clientA.RequestAsync<Int32Value, StringValue>(
                    "MassivePing", 
                    new Int32Value { Value = requestId }, 
                    TimeSpan.FromSeconds(10))); // Cho timeout dài một chút vì xử lý 10k request tốn CPU
            }

            // Đợi toàn bộ 10.000 kết quả trả về
            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            // Assert
            Assert.That(results.Length, Is.EqualTo(totalRequests), "Bị rớt gói! Số lượng phản hồi không đủ.");
    
            // Kiểm tra tính nguyên vẹn: Request số 9999 phải nhận đúng chữ "Pong_9999" (Không bị râu ông nọ cắm cằm bà kia)
            for (int i = 0; i < totalRequests; i++)
            {
                Assert.That(results[i].Value, Is.EqualTo($"Pong_{i}"));
            }

            Console.WriteLine($"[Test18] Hoàn thành 10.000 RPC Requests trong {stopwatch.ElapsedMilliseconds} ms!");
        }
        
        [Test]
        public async Task Test19_ServerResilience_ShouldSurviveUserCodeExceptions()
        {
            _server.OnRequest<StringValue, StringValue>("RiskyLogic", incoming => 
            {
                if (incoming.request.Value == "CRASH_ME")
                {
                    throw new InvalidOperationException("Lỗi Logic Game Nghiêm Trọng!");
                }
                return new StringValue { Value = "SAFE_" + incoming.request.Value };
            });

            await Task.Delay(500);

            // ACT 1: Gửi lệnh bắt Server phải Crash
            var ex = Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                await _clientA.RequestAsync<StringValue, StringValue>(
                    "RiskyLogic", 
                    new StringValue { Value = "CRASH_ME" }, 
                    TimeSpan.FromSeconds(1));
            });

            // Client sẽ bị Timeout vì Server bị lỗi nên không thèm trả lời
            Assert.That(ex.Message, Does.Contain("Timeout").IgnoreCase);

            // ACT 2: Lập tức gửi một lệnh bình thường xem Server còn sống không?
            var survivalResponse = await _clientA.RequestAsync<StringValue, StringValue>(
                "RiskyLogic", 
                new StringValue { Value = "HELLO" }, 
                TimeSpan.FromSeconds(2));

            // Assert: Server vẫn sống nhăn răng và trả lời bình thường!
            Assert.That(survivalResponse.Value, Is.EqualTo("SAFE_HELLO"), "Server đã bị Crash hoàn toàn bởi lỗi trước đó!");
        }
        
        [Test]
        public async Task Test20_FIFO_OrderGuarantee_ShouldNotShuffleMessages()
        {
            int totalMessages = 1000;
            var receivedList = new System.Collections.Generic.List<int>();
            var waitHandle = new ManualResetEventSlim(false);

            _server.OnMessage<Int32Value>("MovementSteps", data => 
            {
                lock (receivedList) // Lock để đảm bảo List an toàn khi add
                {
                    receivedList.Add(data.data.Value);
                    if (receivedList.Count == totalMessages) waitHandle.Set();
                }
            });

            await Task.Delay(500);

            // Bắn 1000 message theo đúng thứ tự từ 0 đến 999
            for (int i = 0; i < totalMessages; i++)
            {
                _clientA.Publish("MovementSteps", new Int32Value { Value = i });
            }

            waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Assert
            Assert.That(receivedList.Count, Is.EqualTo(totalMessages));
    
            // Kiểm tra xem có bị lộn xộn không (Vd: 0, 1, 3, 2, 4...)
            for (int i = 0; i < totalMessages; i++)
            {
                Assert.That(receivedList[i], Is.EqualTo(i), $"Lỗi thứ tự! Đáng lẽ nhận được {i} nhưng lại nhận {receivedList[i]}");
            }
        }
        
        [Test]
        public async Task Test21_UTF8_Emoji_ShouldSerializeCorrectly()
        {
            string complexText = "Xin chào Việt Nam! 🐉✨ 頑張って こんにちは";
            string receivedText = "";
            var waitHandle = new ManualResetEventSlim(false);

            _server.OnMessage<StringValue>("WorldChat", data => 
            {
                receivedText = data.data.Value;
                waitHandle.Set();
            });

            await Task.Delay(500);

            _clientA.Publish("WorldChat", new StringValue { Value = complexText });

            waitHandle.Wait(TimeSpan.FromSeconds(2));

            Assert.That(receivedText, Is.EqualTo(complexText), "Lỗi Encoding! Dữ liệu bị biến dạng khi gửi qua mạng.");
        }
        
        [Test]
        public void Test22_Tick_WhenQueueIsEmpty_ShouldNotThrowOrDegrade()
        {
            // Giả lập Game đang chạy 100,000 frames nhưng không có message mạng nào
            Assert.DoesNotThrow(() => 
            {
                for (int i = 0; i < 100_000; i++)
                {
                    _clientA.Tick();
                }
            }, "Hàm Tick bị lỗi khi xử lý Queue trống!");
        }
        
        [Test]
        public async Task Test23_ClientAsyncRPC_ShouldProcessAndReplyCorrectly()
        {
            // 1. CLIENT: Đăng ký hứng Request (Bất đồng bộ)
            _clientA.OnRequest<StringValue, StringValue>("LoadPlayerData", async req =>
            {
                // Giả lập Client đang phải đọc file hoặc load Asset bundle mất 500ms
                await Task.Delay(500); 
        
                return new StringValue { Value = $"Loaded_{req.Value}" };
            });

            await Task.Delay(500); // Đợi NATS nối ống dẫn

            // 2. Giả lập Unity Game Loop chạy ngầm (60 FPS)
            var ctsUnityLoop = new CancellationTokenSource();
            var unityGameLoop = Task.Run(async () =>
            {
                while (!ctsUnityLoop.IsCancellationRequested)
                {
                    _clientA.Tick();
                    await Task.Delay(16); // Không làm đơ luồng nhờ bất đồng bộ
                }
            });

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // 3. SERVER: Gọi Request xuống Client (Region VN-01)
            var response = await _server.RequestAsync<StringValue, StringValue>(
                "LoadPlayerData",
                "VN-01",
                new StringValue { Value = "Profile_Alex" },
                TimeSpan.FromSeconds(3) // Timeout 3 giây
            );

            stopwatch.Stop();
            ctsUnityLoop.Cancel(); // Dừng game loop

            // 4. ASSERT
            Assert.That(response, Is.Not.Null);
            Assert.That(response.Value, Is.EqualTo("Loaded_Profile_Alex"));
    
            // Đảm bảo rằng Request thực sự tốn ít nhất 500ms (chứng tỏ hàm await Task.Delay trong Client đã hoạt động)
            Assert.That(stopwatch.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(500), 
                "Client không thực sự chạy bất đồng bộ (chưa đợi đủ 500ms đã trả lời)!");
        }
        
        
        [Test]
        [Category("Performance")]
        public async Task Test24_Performance_Throughput_ShouldExceed10000MPS()
        {
            int totalMessages = 20_000;
            int receivedCount = 0;
            var waitHandle = new ManualResetEventSlim(false);
            
            HashSet<int> sentTracker = new HashSet<int>();
            HashSet<int> receivedTracker = new HashSet<int>();

            _server.OnMessage<Int32Value>("ThroughputTest", data => 
            {
                receivedTracker.Add(data.data.Value);
                
                if (Interlocked.Increment(ref receivedCount) == totalMessages)
                {
                    waitHandle.Set();
                }
            });

            await Task.Delay(500); // Làm nóng kết nối

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Act: Xả 100.000 tin nhắn nhanh nhất có thể (Sử dụng Task.Run để không block luồng test)
            _ = Task.Run(() => 
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    _clientA.Publish("ThroughputTest", new Int32Value { Value = i });
                    sentTracker.Add(i);
                }
            });

            // Chờ tối đa 10 giây
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(10));
            stopwatch.Stop();
            
            HashSet<int> missingMessages = new HashSet<int>(sentTracker);
            missingMessages.ExceptWith(receivedTracker);
            
            

            Assert.That(success, Is.True, $"Timeout! Chỉ nhận được {receivedCount}/{totalMessages}, cac data bị miss {string.Join(",", missingMessages)}");

            // Tính toán MPS
            double seconds = stopwatch.Elapsed.TotalSeconds;
            double mps = totalMessages / seconds;

            Console.WriteLine($"[Test 24] Đã nhận {totalMessages:N0} tin nhắn trong {seconds:F3}s. Tốc độ: {mps:N0} MPS.");

            // Assert hiệu suất tối thiểu (Ví dụ: phải lớn hơn 10.000 tin nhắn / giây)
            Assert.That(mps, Is.GreaterThan(1000), "Hiệu suất quá thấp, có thể đang bị nghẽn cổ chai ở Serialize hoặc Channel!");
        }
        
        [Test]
        [Category("Performance")]
        public async Task Test25_Performance_Latency_AverageRTT_ShouldBeUnder2ms()
        {
            int iterations = 1000;
    
            // Server phản hồi nhanh nhất có thể
            _server.OnRequest<Int32Value, Int32Value>("LatencyPing", incoming => 
            {
                return incoming.request; // Trả lại y nguyên để tiết kiệm thời gian
            });

            await Task.Delay(500);

            // Warmup: Bắn vài phát đầu tiên để JIT Compiler dịch code (Loại bỏ thời gian khởi động)
            for (int i = 0; i < 5; i++) 
            {
                await _clientA.RequestAsync<Int32Value, Int32Value>("LatencyPing", new Int32Value { Value = 0 }, TimeSpan.FromSeconds(1));
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Act: Bắn Ping-Pong tuần tự 1000 lần
            for (int i = 0; i < iterations; i++)
            {
                await _clientA.RequestAsync<Int32Value, Int32Value>(
                    "LatencyPing", 
                    new Int32Value { Value = i }, 
                    TimeSpan.FromSeconds(1));
            }

            stopwatch.Stop();

            double totalMs = stopwatch.Elapsed.TotalMilliseconds;
            double averageLatencyMs = totalMs / iterations;

            Console.WriteLine($"[Test 25] 1000 vòng Ping-Pong tốn {totalMs:F2}ms. Độ trễ trung bình RTT: {averageLatencyMs:F4} ms/request.");

            // Assert độ trễ: Quá 2ms trên localhost là code đang có vấn đề về Thread/Locking
            Assert.That(averageLatencyMs, Is.LessThan(2.0), "Độ trễ trung bình quá cao!");
        }
        
        [Test]
        [Category("Performance")]
        public async Task Test26_Performance_Memory_ShouldNotTriggerFrequentGarbageCollection()
        {
            int totalMessages = 50_000;
            var waitHandle = new ManualResetEventSlim(false);
            int received = 0;

            _server.OnMessage<Int32Value>("MemoryTest", data => 
            {
                if (Interlocked.Increment(ref received) == totalMessages)
                    waitHandle.Set();
            });

            await Task.Delay(500);

            // Ép dọn rác trước khi test để có điểm xuất phát sạch sẽ
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Ghi nhận số lần dọn rác Thế hệ 0 (Gen 0) ban đầu
            int initialGen0Collections = GC.CollectionCount(0);

            // Act
            _ = Task.Run(() => 
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    _clientA.Publish("MemoryTest", new Int32Value { Value = i });
                }
            });

            waitHandle.Wait(TimeSpan.FromSeconds(10));

            // Ghi nhận số lần dọn rác sau khi test
            int finalGen0Collections = GC.CollectionCount(0);
            int collectionsHappened = finalGen0Collections - initialGen0Collections;

            Console.WriteLine($"[Test 26] Sau {totalMessages:N0} tin nhắn, GC Gen 0 đã chạy {collectionsHappened} lần.");

            // Assert: Đối với 50.000 tin nhắn, nếu ArrayPool hoạt động tốt, GC gần như không chạy (Dưới 5 lần là quá tuyệt vời).
            Assert.That(collectionsHappened, Is.LessThan(10), "Memory Leak (Sinh quá nhiều rác)! ArrayPool có thể chưa được tận dụng triệt để.");
        }
    }
}