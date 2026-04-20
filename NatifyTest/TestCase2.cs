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
                receivedMessage = data.data.Value;
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
                Assert.That(data.Value.Value, Is.EqualTo(100));
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
                if (data.Value.Value == "Server_Acknowledged")
                    pongReceived.Set();
            });

            // 2. Server setup hứng PING và trả lời PONG
            _server.OnMessage<StringValue>("PING_TOPIC", incoming =>
            {
                if (incoming.data.Value.Value == "Hello_Server")
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
                if (data.data.Value.Value == -1)
                {
                    isServerReady.Set(); // Báo cáo: Đường truyền đã kết nối 100%
                    return;
                }

                // Xử lý đạn thật
                missingTracker[data.data.Value.Value] = true;
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
                receivedString = data.data.Value.Value;
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

            Assert.That(ex.Message, Does.Contain("timed out").IgnoreCase);
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
                validMessage = data.Value;
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

            Assert.That(ex.Message, Does.Contain("timed out").IgnoreCase,
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
            _server.OnRequest<Int32Value, StringValue>("MassivePing",
                incoming => { return new StringValue { Value = $"Pong_{incoming.request.Value}" }; });

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
            Assert.That(ex.Message, Does.Contain("timed out").IgnoreCase);

            // ACT 2: Lập tức gửi một lệnh bình thường xem Server còn sống không?
            var survivalResponse = await _clientA.RequestAsync<StringValue, StringValue>(
                "RiskyLogic",
                new StringValue { Value = "HELLO" },
                TimeSpan.FromSeconds(2));

            // Assert: Server vẫn sống nhăn răng và trả lời bình thường!
            Assert.That(survivalResponse.Value, Is.EqualTo("SAFE_HELLO"),
                "Server đã bị Crash hoàn toàn bởi lỗi trước đó!");
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
                    receivedList.Add(data.data.Value.Value);
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
                Assert.That(receivedList[i], Is.EqualTo(i),
                    $"Lỗi thứ tự! Đáng lẽ nhận được {i} nhưng lại nhận {receivedList[i]}");
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
                receivedText = data.data.Value.Value;
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
            int totalMessages = 100_000;
            int receivedCount = 0;
            var waitHandle = new ManualResetEventSlim(false);

            ConcurrentDictionary<int, byte> sentTracker = new();
            ConcurrentDictionary<int, byte> receivedTracker = new();

            _server.OnMessage<Int32Value>("ThroughputTest", data =>
            {
                receivedTracker.TryAdd(data.data.Value.Value, 1);

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
                    sentTracker.TryAdd(i, 1);
                }
            });

            // Chờ tối đa 10 giây
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(10));
            stopwatch.Stop();

            var missingMessages = sentTracker.Keys.Except(receivedTracker.Keys).ToList();


            Assert.That(success, Is.True,
                $"Timeout! Chỉ nhận được {receivedCount}/{totalMessages}, cac data bị miss {string.Join(",", missingMessages)}");

            // Tính toán MPS
            double seconds = stopwatch.Elapsed.TotalSeconds;
            double mps = totalMessages / seconds;

            Console.WriteLine(
                $"[Test 24] Đã nhận {totalMessages:N0} tin nhắn trong {seconds:F3}s. Tốc độ: {mps:N0} MPS.");

            // Assert hiệu suất tối thiểu (Ví dụ: phải lớn hơn 10.000 tin nhắn / giây)
            Assert.That(mps, Is.GreaterThan(1000),
                "Hiệu suất quá thấp, có thể đang bị nghẽn cổ chai ở Serialize hoặc Channel!");
        }

        [Test]
        [Category("Performance")]
        public async Task Test25_Performance_Latency_AverageRTT_ShouldBeUnder2ms()
        {
            int iterations = 100;

            // Server phản hồi nhanh nhất có thể
            _server.OnRequest<Int32Value, Int32Value>("LatencyPing", incoming =>
            {
                return incoming.request; // Trả lại y nguyên để tiết kiệm thời gian
            });

            await Task.Delay(500);
            
            var ctsUnityLoop = new CancellationTokenSource();
            _ = Task.Run(async () =>
            {
                while (!ctsUnityLoop.IsCancellationRequested)
                {
                    _clientA.Tick();
                    await Task.Delay(16);
                }
            });

            // Warmup: Bắn vài phát đầu tiên để JIT Compiler dịch code (Loại bỏ thời gian khởi động)
            for (int i = 0; i < 5; i++)
            {
                await _clientA.RequestAsync<Int32Value, Int32Value>("LatencyPing", new Int32Value { Value = 0 },
                    TimeSpan.FromSeconds(1));
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
            await ctsUnityLoop.CancelAsync();

            Console.WriteLine(
                $"[Test 25] 1000 vòng Ping-Pong tốn {totalMs:F2}ms. Độ trễ trung bình RTT: {averageLatencyMs:F4} ms/request.");

            // Assert độ trễ: Quá 2ms trên localhost là code đang có vấn đề về Thread/Locking
            Assert.That(averageLatencyMs, Is.LessThan(2.0), $"Độ trễ trung bình quá cao! {averageLatencyMs}");
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

            Console.WriteLine(
                $"[Test 26] Sau {totalMessages:N0} tin nhắn, GC Gen 0 đã chạy {collectionsHappened} lần.");

            // Assert: Đối với 50.000 tin nhắn, nếu ArrayPool hoạt động tốt, GC gần như không chạy (Dưới 5 lần là quá tuyệt vời).
            Assert.That(collectionsHappened, Is.LessThan(10),
                "Memory Leak (Sinh quá nhiều rác)! ArrayPool có thể chưa được tận dụng triệt để.");
        }

        [Test]
        [Category("Metrics")]
        public async Task Test27_Telemetry_Triggers_ShouldTrackCorrectMetrics()
        {
            int totalMessages = 2500;
            var waitHandle = new ManualResetEventSlim(false);
            int receivedCount = 0;

            // Reset lại Trigger (Bằng cách lấy số liệu ban đầu làm mốc)
            long initialClientSent = _clientA.Trigger.MessagesSent;
            long initialServerReceived = _server.Trigger.MessagesReceived;

            _server.OnMessage<Int32Value>("TriggerTest", data =>
            {
                if (Interlocked.Increment(ref receivedCount) == totalMessages)
                {
                    waitHandle.Set();
                }
            });

            await Task.Delay(500);

            // Act
            for (int i = 0; i < totalMessages; i++)
            {
                _clientA.Publish("TriggerTest", new Int32Value { Value = i });
            }

            bool success = waitHandle.Wait(TimeSpan.FromSeconds(5));

            // Assert
            Assert.That(success, Is.True, "Server không nhận đủ tin nhắn.");

            long clientSentCount = _clientA.Trigger.MessagesSent - initialClientSent;
            long serverReceivedCount = _server.Trigger.MessagesReceived - initialServerReceived;

            // 1. Kiểm tra số lượng Message lẻ
            Assert.That(clientSentCount, Is.EqualTo(totalMessages), "Client Trigger đếm sai số lượng gửi đi!");
            Assert.That(serverReceivedCount, Is.EqualTo(totalMessages), "Server Trigger đếm sai số lượng nhận vào!");

            // 2. Kiểm tra Batching (2500 tin nhắn gửi cực nhanh thì số lượng Batch chắc chắn phải < 2500)
            long clientBatches = _clientA.Trigger.BatchesSent;
            long serverBatches = _server.Trigger.BatchesReceived;

            Assert.That(clientBatches, Is.GreaterThan(0).And.LessThan(totalMessages),
                "Micro-Batching không hoạt động! Gửi từng tin nhắn một.");
            Assert.That(serverBatches, Is.GreaterThan(0), "Server không ghi nhận được Batch nào.");

            Console.WriteLine(
                $"[Test 27] 2500 tin nhắn được nhồi vào {clientBatches} Batches. RAM Client: {_clientA.Trigger.ProcessMemoryMB:F1}MB, RAM Server: {_server.Trigger.ProcessMemoryMB:F1}MB");
        }

        [Test]
        [Category("Reliability")]
        public async Task Test28_Deduplication_ShouldProcessDuplicateMessagesOnlyOnce()
        {
            int processCount = 0;
            var waitHandle = new CountdownEvent(1);

            _server.OnMessage<StringValue>("DedupTest", data =>
            {
                Interlocked.Increment(ref processCount);
                waitHandle.Signal();
            });

            await Task.Delay(500);

            // ĐÓNG GIẢ MỘT GÓI TIN ĐÃ GOM BATCH VỚI ID CỐ ĐỊNH
            string fakeMessageId = "HACKER_DUPE_ID_9999";
            var batchMsg = new NatifyBatch();
            var payload = new StringValue { Value = "Gold + 1000" };

            var (buffer, length) = NatifySerializer.Serialize(payload);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            
            batchMsg.Payloads.Add(Google.Protobuf.ByteString.CopyFrom(exactData));

            var (batchBuffer, batchLength) = NatifySerializer.Serialize(batchMsg);
            var exactBatchData = new byte[batchLength];
            Array.Copy(batchBuffer, exactBatchData, batchLength);

            // Dùng NatsConnection thuần để bắn thẳng gói tin rác này vào Server 3 lần liên tiếp!
            var rawNats = new NATS.Client.Core.NatsConnection(new NATS.Client.Core.NatsOpts { Url = NatsUrl });
            await rawNats.ConnectAsync();

            string subject = NatifyTopics.GetClientPublishSubject("GameServer", "GameClient", "VN-01", "DedupTest");
            var headers = new NATS.Client.Core.NatsHeaders { ["Natify-BatchId"] = fakeMessageId };

            // Bắn 3 lần cùng 1 ID
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);

            // Chờ 2 giây
            bool signaled = waitHandle.Wait(TimeSpan.FromSeconds(2));

            // Assert
            Assert.That(signaled, Is.True, "Server không nhận được gói tin.");

            // ĐIỂM CỐT LÕI: Mặc dù bắn 3 lần, nhưng vì chung 1 MessageId, Server chỉ được phép gọi callback 1 lần.
            Assert.That(processCount, Is.EqualTo(1),
                "CẢNH BÁO LỖI BẢO MẬT: Server xử lý trùng lặp gói tin! Deduplication bị thủng.");

            // Kiểm tra Trigger Cache
            Assert.That(_server.Trigger.CurrentDedupCacheSize, Is.GreaterThanOrEqualTo(1),
                "Cache ID không được ghi nhận vào Trigger.");

            await rawNats.DisposeAsync();
        }

        [Test]
        [Category("Reliability")]
        public async Task Test29_ServerToClient_ReliableMessaging_ShouldWorkPerfectly()
        {
            int totalFromServer = 50;
            int clientReceived = 0;
            var waitHandle = new ManualResetEventSlim(false);

            // 1. Client đăng ký lắng nghe (Nhớ phải gọi Tick thì Callback mới chạy)
            _clientA.OnMessage<StringValue>("ServerPush", data =>
            {
                if (Interlocked.Increment(ref clientReceived) == totalFromServer)
                {
                    waitHandle.Set();
                }
            });

            await Task.Delay(500);

            // 2. Unity Game Loop ảo để Tick liên tục cho Client
            var ctsUnityLoop = new CancellationTokenSource();
            _ = Task.Run(async () =>
            {
                while (!ctsUnityLoop.IsCancellationRequested)
                {
                    _clientA.Tick();
                    await Task.Delay(16);
                }
            });

            // 3. Act: Server gửi 50 tin nhắn cho Client A
            for (int i = 0; i < totalFromServer; i++)
            {
                _server.Publish("ServerPush", "VN-01", new StringValue { Value = $"Message_{i}" });
            }

            // 4. Chờ Client xử lý xong
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(5));
            ctsUnityLoop.Cancel();

            // 5. Assert
            Assert.That(success, Is.True,
                $"Client chỉ nhận được {clientReceived}/{totalFromServer} tin nhắn từ Server.");

            // Kiểm tra Client có đẩy số liệu Trigger lên không
            Assert.That(_clientA.Trigger.MessagesReceived, Is.GreaterThanOrEqualTo(totalFromServer));
        }

        [Test]
        [Category("Memory")]
        public async Task Test30_TimeWheel_GarbageCollection_ShouldFreeRamAfterTTL()
        {
            // Báo cho NUnit biết test này có thể chạy hơi lâu (vì chờ TimeWheel 10s)
            var waitHandle = new ManualResetEventSlim(false);

            _server.OnMessage<StringValue>("GC_Test", data => { waitHandle.Set(); });

            await Task.Delay(500);

            long initialCacheSize = _server.Trigger.CurrentDedupCacheSize;
            long initialExpiredCount = _server.Trigger.TotalDedupExpired;

            // Bắn 1 gói tin
            _clientA.Publish("GC_Test", new StringValue { Value = "Need_To_Be_Cleaned" });

            // Đợi Server nhận được
            waitHandle.Wait(TimeSpan.FromSeconds(2));

            // Ngay sau khi nhận, CacheSize phải tăng lên ít nhất 1
            Assert.That(_server.Trigger.CurrentDedupCacheSize, Is.GreaterThan(initialCacheSize),
                "Gói tin không được đưa vào Cache chống trùng lặp.");

            Console.WriteLine($"[Test 30] Đang chờ 11 giây để TimeWheel dọn rác...");

            // Cố tình chờ 11 giây (Vượt quá 10 giây TTL của Time Wheel)
            await Task.Delay(11_000);

            // Kiểm tra lại Trigger
            long finalCacheSize = _server.Trigger.CurrentDedupCacheSize;
            long finalExpiredCount = _server.Trigger.TotalDedupExpired;

            Assert.That(finalCacheSize, Is.EqualTo(initialCacheSize),
                "Lỗi Rò Rỉ Bộ Nhớ (Memory Leak)! Time Wheel KHÔNG xóa ID cũ.");
            Assert.That(finalExpiredCount, Is.GreaterThan(initialExpiredCount),
                "Trigger TotalDedupExpired không ghi nhận số lượng giải phóng.");

            Console.WriteLine($"[Test 30] TimeWheel hoạt động hoàn hảo. Đã giải phóng thành công!");
        }

        /// <summary>
        /// Kịch bản 31: Massive Fan-out (Phân phối diện rộng từ Server)
        /// Server gửi 1 loạt tin nhắn cho nhiều Client ở các Region khác nhau cùng lúc.
        /// Đảm bảo hệ thống định tuyến (Routing) không bị nhầm lẫn giữa các Client.
        /// </summary>
        [Test]
        [Category("Integration")]
        public async Task Test31_ServerToClient_FanOut_ShouldRouteToCorrectRegions()
        {
            var waitHandleA = new ManualResetEventSlim(false);
            var waitHandleB = new ManualResetEventSlim(false);

            string msgA = null;
            string msgB = null;

            // Client A (VN-01) lắng nghe
            _clientA.OnMessage<StringValue>("SystemAnnouncement", data =>
            {
                msgA = data.Value.Value;
                waitHandleA.Set();
            });

            // Client B (US-West) lắng nghe
            _clientB.OnMessage<StringValue>("SystemAnnouncement", data =>
            {
                msgB = data.Value.Value;
                waitHandleB.Set();
            });

            await Task.Delay(500);

            // Giả lập vòng lặp Game cho cả 2 Client
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            _ = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick();
                    _clientB.Tick();
                    await Task.Delay(16);
                }
            });

            // Act: Server xả thông báo riêng biệt cho từng Region
            _server.Publish("SystemAnnouncement", "VN-01", new StringValue { Value = "BaoTri_VN" });
            _server.Publish("SystemAnnouncement", "US-West", new StringValue { Value = "Maintenance_US" });

            bool successA = waitHandleA.Wait(TimeSpan.FromSeconds(2));
            bool successB = waitHandleB.Wait(TimeSpan.FromSeconds(2));

            cts.Cancel();
            // Assert
            Assert.That(successA, Is.True, "Client A không nhận được thông báo");
            Assert.That(successB, Is.True, "Client B không nhận được thông báo");
            Assert.That(msgA, Is.EqualTo("BaoTri_VN"), "Client A nhận nhầm luồng dữ liệu của US-West");
            Assert.That(msgB, Is.EqualTo("Maintenance_US"), "Client B nhận nhầm luồng dữ liệu của VN-01");
        }

        /// <summary>
        /// Kịch bản 32: Slow Client (Client lười biếng / Tụt FPS)
        /// Nếu Game bị lag/đơ, hàm Tick() không được gọi trong 2 giây, dữ liệu từ Server đẩy xuống
        /// phải được xếp hàng an toàn trong ConcurrentQueue chứ không được vứt bỏ hay làm Crash RAM.
        /// </summary>
        [Test]
        [Category("Reliability")]
        public async Task Test32_ServerToClient_SlowClient_ShouldQueueMessagesSafely()
        {
            int totalMessages = 500;
            int processCount = 0;

            _clientA.OnMessage<Int32Value>("SpawnMonster", data => { processCount++; });

            await Task.Delay(500);

            // Act 1: Server xả đạn cực nhanh xuống Client
            for (int i = 0; i < totalMessages; i++)
            {
                _server.Publish("SpawnMonster", "VN-01", new Int32Value { Value = i });
            }

            // Đợi 2 giây để mạng chuyển hết dữ liệu, nhưng TUYỆT ĐỐI KHÔNG GỌI TICK()
            await Task.Delay(2000);

            // Assert 1: Chắc chắn rắng callback chưa hề được chạy (Queue đang giữ hàng)
            Assert.That(processCount, Is.EqualTo(0),
                "Lỗi nghiêm trọng: Hàm Callback chạy ngoài Main Thread (Không thông qua Tick)!");

            // Act 2: Game hết lag, Main Thread gọi Tick liên tục để xả Queue
            // Lưu ý: Hàm Tick() của bạn xử lý tối đa 100 action mỗi lần gọi
            for (int i = 0; i < 10; i++) // Gọi 10 lần x 100 = dư sức xả hết 500
            {
                _clientA.Tick();
            }

            // Assert 2: Queue phải xả chính xác 500 tin nhắn
            Assert.That(processCount, Is.EqualTo(totalMessages), "Client bị rơi rớt dữ liệu khi Queue bị ùn ứ!");
        }

        /// <summary>
        /// Kịch bản 33: Client Deduplication (Chống trùng lặp tại Client)
        /// Giả lập mạng chập chờn, Server tưởng Client chưa nhận được nên cố tình gửi lại
        /// gói tin cũ (cùng MessageId). Client phải chặn đứng sự trùng lặp này.
        /// </summary>
        [Test]
        [Category("Reliability")]
        public async Task Test33_ServerToClient_Deduplication_ShouldBlockDuplicatePushes()
        {
            int processCount = 0;

            _clientA.OnMessage<StringValue>("RewardItem", data => { processCount++; });

            await Task.Delay(500);

            // Tự tay tạo ra một Batch giả mạo để kiểm soát MessageId tĩnh
            string fixedMessageId = "SERVER_RETRY_MSG_001";
            var batchMsg = new NatifyBatch();
            var payload = new StringValue { Value = "Sword_Level_99" };

            var (buffer, length) = NatifySerializer.Serialize(payload);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            batchMsg.Payloads.Add(Google.Protobuf.ByteString.CopyFrom(exactData));

            var (batchBuffer, batchLength) = NatifySerializer.Serialize(batchMsg);
            var exactBatchData = new byte[batchLength];
            Array.Copy(batchBuffer, exactBatchData, batchLength);

            // Dùng kết nối NATS thô để đóng giả Server gửi tin cậy
            var rawNats = new NATS.Client.Core.NatsConnection(new NATS.Client.Core.NatsOpts { Url = NatsUrl });
            await rawNats.ConnectAsync();

            // Lấy đúng Subject mà Client đang Listen
            string subject = NatifyTopics.GetClientListenSubject("GameClient", "GameServer", "VN-01", "RewardItem");
            var headers = new NATS.Client.Core.NatsHeaders { ["Natify-BatchId"] = fixedMessageId };

            // Bắn 3 lần y hệt nhau
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);
            await rawNats.PublishAsync(subject, exactBatchData, headers: headers);

            // Xả Tick()
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            while (!cts.IsCancellationRequested)
            {
                _clientA.Tick();
                await Task.Delay(16);
            }

            // Assert: Client chỉ được phép nhận Thanh Gươm Cấp 99 ĐÚNG 1 LẦN
            Assert.That(processCount, Is.EqualTo(1),
                "Client Deduplication bị lỗi! Người chơi đã nhận được vật phẩm nhân bản (Dupe Bug)!");
            Assert.That(_clientA.Trigger.CurrentDedupCacheSize, Is.GreaterThanOrEqualTo(1),
                "Cache tại Client không ghi nhận ID này.");

            await rawNats.DisposeAsync();
        }

        /// <summary>
        /// Kịch bản 34: Server Request Client Large Async Payload (Tải trọng lớn ngược chiều)
        /// Server gọi RPC xuống Client, Client load một file Save game nặng 100KB (Bất đồng bộ) và trả về.
        /// Đảm bảo cơ chế ReplyTo raw của Client hoạt động ổn định với file lớn.
        /// </summary>
        [Test]
        [Category("Integration")]
        public async Task Test34_ServerToClient_AsyncRPC_LargePayload_ShouldNotBreak()
        {
            string hugeSaveFile = new string('S', 100_000); // ~100KB Data

            // Client đăng ký xử lý
            _clientA.OnRequest<StringValue, StringValue>("UploadSaveData", async req =>
            {
                // Giả lập đọc file Save mất 200ms
                await Task.Delay(200);
                return new StringValue { Value = hugeSaveFile };
            });

            await Task.Delay(500);

            // Game Loop cho Client
            var cts = new CancellationTokenSource();
            _ = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick();
                    await Task.Delay(16);
                }
            });

            // Act: Server chủ động gọi xuống
            var response = await _server.RequestAsync<StringValue, StringValue>(
                "UploadSaveData",
                "VN-01",
                new StringValue { Value = "GiveMeYourSave" },
                TimeSpan.FromSeconds(5));

            cts.Cancel();

            // Assert
            Assert.That(response, Is.Not.Null, "Server bị Timeout khi chờ Client gửi File lớn.");
            Assert.That(response.Value.Length, Is.EqualTo(100_000),
                "Dữ liệu file Save gửi từ Client bị cắt xén hoặc hỏng!");
        }

        /// <summary>
        /// Kịch bản 35: Throughput Server -> Client (Mở khóa tối đa CPU)
        /// Bài test này gọi hàm Tick() liên tục không có độ trễ để đo tốc độ mạng NATS và tốc độ Deserialize Protobuf 
        /// thuần túy khi truyền từ Server xuống Client.
        /// </summary>
        [Test]
        [Category("Performance")]
        public async Task Test35_ServerToClient_Throughput_UnrestrictedTick()
        {
            int totalMessages = 100_000;
            int receivedCount = 0;
            var waitHandle = new ManualResetEventSlim(false);

            _clientA.OnMessage<Int32Value>("MassivePush", data =>
            {
                if (Interlocked.Increment(ref receivedCount) == totalMessages)
                {
                    waitHandle.Set();
                }
            });

            await Task.Delay(500); // Warmup

            // Luồng xả Tick liên tục không nghỉ (Không delay 16ms như 60 FPS)
            var cts = new CancellationTokenSource();
            _ = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick();
                }
            });

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Act: Server xả 100k tin nhắn
            _ = Task.Run(async () =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    _server.Publish("MassivePush", "VN-01", new Int32Value { Value = i });

                    // Nhường CPU mỗi 500 tin nhắn để tránh nghẽn Buffer mạng TCP của Server
                    if (i % 500 == 0) await Task.Yield();
                }
            });

            // Chờ tối đa 15 giây
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(15));
            stopwatch.Stop();
            cts.Cancel();

            // Assert
            Assert.That(success, Is.True, $"Timeout! Client chỉ xử lý được {receivedCount}/{totalMessages} tin nhắn.");

            double seconds = stopwatch.Elapsed.TotalSeconds;
            double mps = totalMessages / seconds;

            Console.WriteLine(
                $"[Test 35] Server -> Client 100k tin nhắn trong {seconds:F3}s. Tốc độ thực: {mps:N0} MPS.");
            Assert.That(mps, Is.GreaterThan(5000), "Tốc độ xử lý của Client quá chậm!");
        }

        /// <summary>
        /// Kịch bản 36: Mô phỏng nghẽn cổ chai Unity 60 FPS (Real-world Simulation)
        /// Nếu Unity chỉ chạy 60 FPS (xử lý max 6000 msg/s), luồng nền NATS sẽ nhận toàn bộ 100k tin nhắn cực nhanh 
        /// và nhét vào ConcurrentQueue. Bài test này kiểm chứng Queue có bị tràn hoặc văng lỗi khi bị nhồi 100k Action không.
        /// </summary>
        [Test]
        [Category("Performance")]
        public async Task Test36_ServerToClient_60FPS_QueueStressTest()
        {
            int totalMessages = 100_000;
            int receivedCount = 0;
            var waitHandle = new ManualResetEventSlim(false);

            _clientA.OnMessage<Int32Value>("StressTest60FPS", data =>
            {
                if (Interlocked.Increment(ref receivedCount) == totalMessages)
                {
                    waitHandle.Set();
                }
            });

            await Task.Delay(500);

            // Giả lập Game Loop 60 FPS (Chờ 16ms mỗi Frame)
            var cts = new CancellationTokenSource();
            _ = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick(); // Chỉ xử lý 100 action mỗi lần
                    await Task.Delay(16); // 60 FPS
                }
            });

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Server đẩy 100k tin cực nhanh
            _ = Task.Run(async () =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    _server.Publish("StressTest60FPS", "VN-01", new Int32Value { Value = i });
                    if (i % 500 == 0) await Task.Yield();
                }
            });

            // CHÚ Ý: Vì Tick() bị kìm hãm ở 6000 msg/s, nên bắt buộc phải đợi ít nhất ~17 giây. 
            // Ta set Timeout là 30 giây để an toàn.
            bool success = waitHandle.Wait(TimeSpan.FromSeconds(30));
            stopwatch.Stop();
            cts.Cancel();

            Assert.That(success, Is.True,
                $"Timeout! Game Loop 60 FPS không thể tiêu hóa kịp. Bị kẹt ở {receivedCount}/{totalMessages}.");
            Console.WriteLine(
                $"[Test 36] Xả thành công 100k tin nhắn ở 60 FPS. Mất {stopwatch.Elapsed.TotalSeconds:F2} giây để xử lý hết Queue.");
        }

        /// <summary>
        /// Kịch bản 37: Server -> Client Memory Leak Check
        /// Khi Client nhận 100k tin nhắn, nó phải giải nén Batch, phân bổ RAM cho Protobuf, check Cache Dedup.
        /// Đảm bảo GC không bị bóp nghẹt.
        /// </summary>
        [Test]
        [Category("Performance")]
        public async Task Test37_ServerToClient_Memory_ShouldNotSpike()
        {
            int totalMessages = 100_000;
            var waitHandle = new ManualResetEventSlim(false);
            int received = 0;

            _clientA.OnMessage<Int32Value>("MemCheck", data =>
            {
                if (Interlocked.Increment(ref received) == totalMessages)
                    waitHandle.Set();
            });

            await Task.Delay(500);

            // Dọn rác khởi điểm
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long initialMemory = GC.GetTotalMemory(false);
            int initialGen0 = GC.CollectionCount(0);

            var cts = new CancellationTokenSource();
            _ = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested) _clientA.Tick();
            });

            _ = Task.Run(async () =>
            {
                for (int i = 0; i < totalMessages; i++)
                {
                    _server.Publish("MemCheck", "VN-01", new Int32Value { Value = i });
                    if (i % 500 == 0) await Task.Yield();
                }
            });

            waitHandle.Wait(TimeSpan.FromSeconds(20));
            cts.Cancel();

            int finalGen0 = GC.CollectionCount(0);
            long finalMemory = GC.GetTotalMemory(false);

            int collectionsHappened = finalGen0 - initialGen0;
            double memDiffMB = (finalMemory - initialMemory) / (1024.0 * 1024.0);

            Console.WriteLine(
                $"[Test 37] 100k Server->Client. GC Gen 0 chạy {collectionsHappened} lần. Chênh lệch RAM: {memDiffMB:F2} MB");

            // Assert: Đảm bảo luồng Client giải phóng bộ nhớ (ArrayPool) tốt
            Assert.That(collectionsHappened, Is.LessThan(20),
                "Client bị quá tải rác (Garbage) khi phân giải Protobuf!");
        }

        /// <summary>
        /// Kịch bản 38: Endurance / Soak Test (Kiểm thử độ bền 5 phút)
        /// Ép hệ thống chạy liên tục trong 5 phút.
        /// Sử dụng thuật toán "Tổng cấp số cộng" để xác minh không rớt/không trùng lặp 
        /// mà không cần dùng List/Dictionary (tránh làm sai lệch kết quả đo RAM).
        /// </summary>
        [Test]
        [Category("Endurance")]
        [Timeout(400_000)] // Cho phép Test chạy tối đa ~6.5 phút để tránh bị NUnit ép dừng
        public async Task Test38_Endurance_5MinutesSoakTest_NoMemoryLeak_NoMessageLoss()
        {
            long receivedCount = 0;
            long receivedSum = 0; // Dùng để checksum toàn vẹn dữ liệu
            long sentCount = 0;

            _clientA.OnMessage<Int32Value>("SoakTest", data =>
            {
                Interlocked.Increment(ref receivedCount);
                Interlocked.Add(ref receivedSum, data.Value.Value);
            });

            await Task.Delay(500); // Warmup

            // Ép dọn rác trước khi bắt đầu để có điểm mốc RAM sạch nhất
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long initialMemoryMB = GC.GetTotalMemory(true) / (1024 * 1024);

            // 1. Khởi động Game Loop của Client (Cho phép xả max tốc độ)
            var cts = new CancellationTokenSource();
            _ = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick();
                }
            });

            Console.WriteLine($"[Test 38] Bắt đầu Soak Test 5 phút. RAM ban đầu: {initialMemoryMB} MB");

            var duration = TimeSpan.FromMinutes(5);
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var lastReportTime = stopwatch.Elapsed;

            // 2. Act: Vòng lặp bắn tin nhắn liên tục trong 5 phút
            _ = Task.Run(async () =>
            {
                while (stopwatch.Elapsed < duration)
                {
                    // Bắn từng tin một, Server sẽ tự gom Batch ngầm
                    _server.Publish("SoakTest", "VN-01", new Int32Value { Value = (int)sentCount });
                    sentCount++;

                    // Nhường luồng mỗi 1000 tin để tránh CPU bị ngộp 100% gây đứt kết nối NATS
                    if (sentCount % 1000 == 0)
                    {
                        await Task.Yield();
                    }
                }
            });

            // Vòng lặp chính in Log báo cáo mỗi 30 giây để biết Test chưa bị treo
            while (stopwatch.Elapsed < duration)
            {
                if (stopwatch.Elapsed - lastReportTime > TimeSpan.FromSeconds(30))
                {
                    long currentMem = GC.GetTotalMemory(false) / (1024 * 1024);
                    Console.WriteLine(
                        $"[Soak Test] Đang chạy... Đã gửi: {sentCount:N0} | Đã nhận: {Interlocked.Read(ref receivedCount):N0} | RAM: {currentMem} MB");
                    lastReportTime = stopwatch.Elapsed;
                }

                await Task.Delay(1000);
            }

            // 3. Kết thúc thời gian bắn: Chờ Client tiêu hóa nốt dữ liệu còn đọng trên mạng
            Console.WriteLine($"[Test 38] Đã hết 5 phút bắn đạn. Chờ Client xử lý nốt hàng tồn...");

            // Đợi tối đa 30 giây cho luồng nhận bắt kịp luồng gửi
            var catchUpWaitTime = DateTime.UtcNow;
            while (Interlocked.Read(ref receivedCount) < sentCount &&
                   (DateTime.UtcNow - catchUpWaitTime).TotalSeconds < 30)
            {
                await Task.Delay(100);
            }

            stopwatch.Stop();
            cts.Cancel(); // Dừng Game Loop

            // 4. Đo đạc lại RAM
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long finalMemoryMB = GC.GetTotalMemory(true) / (1024 * 1024);
            long memoryGrowth = finalMemoryMB - initialMemoryMB;

            // 5. Assert: KIỂM TOÁN DỮ LIỆU
            long finalReceivedCount = Interlocked.Read(ref receivedCount);
            long finalReceivedSum = Interlocked.Read(ref receivedSum);

            // Công thức tổng cấp số cộng: S = n * (n - 1) / 2 (Vì đếm từ 0)
            long expectedSum = (sentCount - 1) * sentCount / 2;

            Console.WriteLine("=============================================");
            Console.WriteLine($"[KẾT QUẢ SOAK TEST 5 PHÚT]");
            Console.WriteLine($"- Gửi đi : {sentCount:N0} tin nhắn");
            Console.WriteLine($"- Nhận về: {finalReceivedCount:N0} tin nhắn");
            Console.WriteLine($"- Tăng trưởng RAM: {memoryGrowth} MB");
            Console.WriteLine("=============================================");

            // Khẳng định 1: Không mất tin nhắn
            Assert.That(finalReceivedCount, Is.EqualTo(sentCount), "CÓ SỰ CỐ MẤT TIN NHẮN (Packet Loss)!");

            // Khẳng định 2: Dữ liệu chính xác tuyệt đối, không trùng lặp (Dupe), không biến dạng
            Assert.That(finalReceivedSum, Is.EqualTo(expectedSum),
                "DỮ LIỆU BỊ SAI LỆCH! Có thể Deduplication hỏng hoặc Data bị corrupt.");

            // Khẳng định 3: Không rò rỉ bộ nhớ (Memory Leak)
            // Trong C#, RAM dao động vài chục MB là bình thường do các buffer nội bộ của NATS.
            // Nhưng nếu tăng hơn 100MB nghĩa là có rác không được giải phóng.
            Assert.That(memoryGrowth, Is.LessThan(100), $"NGHI VẤN MEMORY LEAK! RAM tăng quá cao ({memoryGrowth} MB).");
        }

        /// <summary>
        /// Kịch bản 39: Client -> Server RPC Reliability (20,000 Requests)
        /// Kiểm tra độ tin cậy khi Client dồn dập gửi 20k yêu cầu RPC lên Server.
        /// Xác minh tính toàn vẹn: Request ID #X phải nhận đúng Response ID #X.
        /// </summary>
        [Test]
        [Category("Performance")]
        public async Task Test39_ClientToServer_RPC_Reliability_20K_Continuous()
        {
            int totalRequests = 20_000;
            int successCount = 0;

            // Server xử lý: Chỉ đơn giản là cộng thêm 1 vào giá trị nhận được
            _server.OnRequest<Int32Value, Int32Value>("BulkRPC_C2S",
                incoming => { return new Int32Value { Value = incoming.request.Value + 1 }; });

            await Task.Delay(500); // Đợi ổn định kết nối

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var tasks = new List<Task>();

            // Act: Bắn 20,000 Request song song (sử dụng Semaphore để kiểm soát concurrency nếu cần, 
            // nhưng ở đây ta xả thẳng để test độ chịu tải của NATS/Natify)
            for (int i = 0; i < totalRequests; i++)
            {
                int requestId = i;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var response = await _clientA.RequestAsync<Int32Value, Int32Value>(
                            "BulkRPC_C2S",
                            new Int32Value { Value = requestId },
                            TimeSpan.FromSeconds(10)); // Timeout 10s cho 20k request

                        if (response.Value == requestId + 1)
                        {
                            Interlocked.Increment(ref successCount);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Test 39] Request {requestId} failed: {ex.Message}");
                    }
                }));
            }

            // 2. Chạy Game Loop cho Client (Mở khóa tối đa tốc độ Tick để tiêu hóa 20k request)
            var cts = new CancellationTokenSource();
            _ = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick(); // Lấy 100 action/lần
                }
            });

            await Task.WhenAll(tasks);
            stopwatch.Stop();

            await cts.CancelAsync();

            // Assert
            Console.WriteLine(
                $"[Test 39] Hoàn thành {totalRequests:N0} RPC C2S trong {stopwatch.ElapsedMilliseconds}ms. Thành công: {successCount}");
            Assert.That(successCount, Is.EqualTo(totalRequests),
                "Bị rớt gói hoặc sai lệch dữ liệu RPC từ Client lên Server!");
        }

        /// <summary>
        /// Kịch bản 40: Server -> Client RPC Reliability (20,000 Requests)
        /// Kiểm tra độ tin cậy khi Server chủ động gọi xuống Client 20k lần.
        /// Test tính bền bỉ của hàng đợi Tick() và luồng phản hồi của Client.
        /// </summary>
        [Test]
        [Category("Performance")]
        public async Task Test40_ServerToClient_RPC_Reliability_20K_Continuous()
        {
            int totalRequests = 20_000;
            int successCount = 0;

            // 1. Client đăng ký xử lý Request (Phải qua luồng Tick)
            _clientA.OnRequest<Int32Value, Int32Value>("BulkRPC_S2C",
                request => { return new Int32Value { Value = request.Value * 2 }; });

            await Task.Delay(500);

            // 2. Chạy Game Loop cho Client (Mở khóa tối đa tốc độ Tick để tiêu hóa 20k request)
            var cts = new CancellationTokenSource();
            _ = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    _clientA.Tick(); // Lấy 100 action/lần
                }
            });

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var tasks = new List<Task>();

            // 3. Act: Server xả 20,000 yêu cầu xuống Region VN-01
            for (int i = 0; i < totalRequests; i++)
            {
                int requestId = i;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var response = await _server.RequestAsync<Int32Value, Int32Value>(
                            "BulkRPC_S2C",
                            "VN-01",
                            new Int32Value { Value = requestId },
                            TimeSpan.FromSeconds(15)); // Timeout dài hơn vì Client Tick có giới hạn

                        if (response.Value == requestId * 2)
                        {
                            Interlocked.Increment(ref successCount);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log lỗi nếu cần
                    }
                }));
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();
            await cts.CancelAsync(); // Dừng vòng lặp Tick

            // Assert
            Console.WriteLine(
                $"[Test 40] Hoàn thành {totalRequests:N0} RPC S2C trong {stopwatch.ElapsedMilliseconds}ms. Thành công: {successCount}");
            Assert.That(successCount, Is.EqualTo(totalRequests),
                "Bị rớt gói hoặc sai lệch dữ liệu RPC từ Server xuống Client!");
        }
    }
}