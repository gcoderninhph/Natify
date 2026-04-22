namespace NatifyTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Google.Protobuf;
    using Google.Protobuf.WellKnownTypes;
    using Natify;
    using NUnit.Framework;
    using NATS.Client.Core;

    [TestFixture]
    public class NatifyClientFastTests
    {
        private const string NatsUrl = "nats://127.0.0.1:4222";

        private NatifyServer CreateServer(string name = "TestServer", string expectedClient = "TestClientFast") => new NatifyServer(NatsUrl, name, "QG1", expectedClient);
        private NatifyClientFast CreateClientFast(string name = "TestClientFast", string group = "Group1", string expectedServer = "TestServer") => new NatifyClientFast(NatsUrl, name, group, "Region1", expectedServer);

        // 1. Basic PUB/SUB
        [Test]
        public async Task Test01_BasicPublish_ServerReceives()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var tcs = new TaskCompletionSource<string>();

            server.OnMessage<StringValue>("Topic1", data => tcs.TrySetResult(data.data.Value.Value));
            client.Publish("Topic1", new StringValue { Value = "Hello" });

            Assert.That(await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo("Hello"));
        }

        // 2. Reverse PUB/SUB
        [Test]
        public async Task Test02_ServerPublish_ClientReceives()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var tcs = new TaskCompletionSource<string>();

            client.OnMessage<StringValue>("Topic2", data => tcs.TrySetResult(data.Value.Value));
            server.Publish("Topic2", "Region1", new StringValue { Value = "World" });

            Assert.That(await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo("World"));
        }

        // 3. Basic REQ/REP
        [Test]
        public async Task Test03_BasicRequest_ServerReplies()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();

            server.OnRequest<StringValue, StringValue>("ReqTopic1", req => new StringValue { Value = req.request.Value + " Reply" });
            
            var reply = await client.RequestAsync<StringValue, StringValue>("ReqTopic1", new StringValue { Value = "Ping" }, TimeSpan.FromSeconds(5));
            Assert.That(reply.Value, Is.EqualTo("Ping Reply"));
        }

        // 4. Reverse REQ/REP
        [Test]
        public async Task Test04_ServerRequest_ClientReplies()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();

            client.OnRequest<StringValue, StringValue>("ReqTopic2", req => new StringValue { Value = req.Value + " ClientReply" });
            
            var reply = await server.RequestAsync<StringValue, StringValue>("ReqTopic2", "Region1", new StringValue { Value = "PingServer" }, TimeSpan.FromSeconds(5));
            Assert.That(reply.Value, Is.EqualTo("PingServer ClientReply"));
        }

        // 5. Async Submit
        [Test]
        public async Task Test05_ClientAsyncCallback_Works()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var tcs = new TaskCompletionSource<string>();

            client.OnMessage<StringValue>("TopicAsync", async data => 
            {
                await Task.Delay(10);
                tcs.TrySetResult(data.Value.Value);
            });
            server.Publish("TopicAsync", "Region1", new StringValue { Value = "Async" });

            Assert.That(await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo("Async"));
        }

        // 6. Timeout handling
        [Test]
        public void Test06_RequestTimeout_ThrowsException()
        {
            using var client = CreateClientFast();
            // Server not running, so no one replies
            var ex = Assert.ThrowsAsync<TimeoutException>(async () => 
                await client.RequestAsync<StringValue, StringValue>("NoReplyTop", new StringValue(), TimeSpan.FromMilliseconds(200)));
            Assert.That(ex.Message, Does.Contain("timed out"));
        }

        // 7. Large Data
        [Test]
        public async Task Test07_Publish_LargePayload()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var largeString = new string('A', 50000); // 50KB payload
            var tcs = new TaskCompletionSource<string>();

            server.OnMessage<StringValue>("LargeP", data => tcs.TrySetResult(data.data.Value.Value));
            client.Publish("LargeP", new StringValue { Value = largeString });

            var res = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(res.Length, Is.EqualTo(50000));
        }

        // 8. Multiple messages batching test
        [Test]
        public async Task Test08_MultiplePUBs_AreGrouped()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            int limit = 100;
            var countdown = new CountdownEvent(limit);

            server.OnMessage<StringValue>("MultiPub", _ => countdown.Signal());
            
            for (int i=0; i<limit; i++) 
            {
                client.Publish("MultiPub", new StringValue { Value = i.ToString() });
            }

            Assert.That(countdown.Wait(TimeSpan.FromSeconds(5)), Is.True);
        }

        // 9. Load Balancing Multiple Clients (Queueing on Server)
        [Test]
        public void Test09_MultipleClients_SameGroup_LoadBalancing()
        {
            using var server1 = CreateServer("TestServer");
            using var server2 = CreateServer("TestServer");
            using var client = CreateClientFast();
            
            // Note: Server currently has hardcoded QG1 for queue group, so they load balance
            var count1 = 0;
            var count2 = 0;

            server1.OnMessage<StringValue>("LB", _ => Interlocked.Increment(ref count1));
            server2.OnMessage<StringValue>("LB", _ => Interlocked.Increment(ref count2));

            for (int i=0; i<100; i++) client.Publish("LB", new StringValue { Value = "1" });
            Thread.Sleep(1000); // Wait delivery

            Assert.That(count1 + count2, Is.EqualTo(100));
            // Usually load balances, but at least shouldn't be 200 (duplicate)
        }

        // 10. Performance - High Throughput PUB
        [Test]
        public void Test1M_Performance_10K_PUB()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            int limit = 1_000_000;
            var countdown = new CountdownEvent(limit);

            server.OnMessage<StringValue>("PerfPUB", _ => countdown.Signal());

            var sw = Stopwatch.StartNew();
            for (int i=0; i<limit; i++) client.Publish("PerfPUB", new Empty());
            
            Assert.That(countdown.Wait(TimeSpan.FromSeconds(10)), Is.True);
            sw.Stop();
            Console.WriteLine($"1m PUB took {sw.ElapsedMilliseconds}ms");
        }

        // 11. Performance - High Throughput REQ/REP
        [Test]
        public async Task Test11_Performance_1K_REQREP()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            int limit = 1_000_000;

            server.OnRequest<Empty, Empty>("PerfREQ", _ => new Empty());

            var tasks = new List<Task>();
            var sw = Stopwatch.StartNew();
            for (int i=0; i<limit; i++) 
            {
                tasks.Add(client.RequestAsync<Empty, Empty>("PerfREQ", new Empty(), TimeSpan.FromSeconds(10)));
            }

            await Task.WhenAll(tasks);
            sw.Stop();
            Console.WriteLine($"1K REQ/REP took {sw.ElapsedMilliseconds}ms");
        }

        // 12. Thread Safety - Concurrent Pub
        [Test]
        public void Test12_ConcurrentPublish()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var localClient = client;
            int threads = 10;
            int msgsPerThread = 500;
            var countdown = new CountdownEvent(threads * msgsPerThread);

            server.OnMessage<Empty>("ConcPub", _ => countdown.Signal());

            Parallel.For(0, threads, _ => 
            {
                for (int j=0; j<msgsPerThread; j++) localClient.Publish("ConcPub", new Empty());
            });

            Assert.That(countdown.Wait(TimeSpan.FromSeconds(10)), Is.True);
        }

        // 13. Duplicate Messages Test (De-dup logic)
        [Test]
        public async Task Test13_DuplicateMessages_Deduplicated()
        {
            using var client = CreateClientFast();
            int counter = 0;
            client.OnMessage<Empty>("Dupe", _ => Interlocked.Increment(ref counter));

            // Simulate raw NATS message with same BatchId manually
            await using var nats = new NatsConnection(new NatsOpts { Url = NatsUrl });
            await nats.ConnectAsync();
            
            var batch = new NatifyBatch();
            batch.Payloads.Add(new Empty().ToByteString());
            batch.ReqId.Add("");
            batch.RepId.Add("");
            batch.MsgType.Add("PUB");
            
            var (buf, len) = NatifySerializer.Serialize(batch);
            var payload = buf.Take(len).ToArray();
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);

            string subject = NatifyTopics.GetClientListenSubject("TestClientFast", "TestServer", "Region1", "Dupe");
            var headers = new NatsHeaders { ["Natify-BatchId"] = "Batch123" };
            
            // Send 3 times
            await nats.PublishAsync(subject, payload, headers: headers);
            await nats.PublishAsync(subject, payload, headers: headers);
            await nats.PublishAsync(subject, payload, headers: headers);

            await Task.Delay(1000);
            Assert.That(counter, Is.EqualTo(1)); // Should only be processed once
        }

        // 14. Server down/up retry logic.
        [Test]
        public async Task Test14_RetryUnackedMessages()
        {
            using var client = CreateClientFast();
            // Publish while server is off (or just ignoring ACKs)
            client.Publish("RetryTopic", new Empty());
            
            // Wait a sec so it retries a few times
            await Task.Delay(500);
            
            using var server = CreateServer();
            var tcs = new TaskCompletionSource<bool>();
            server.OnMessage<Empty>("RetryTopic", _ => tcs.TrySetResult(true));
            
            Assert.That(await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.True);
        }

        // 15. Stress Test: Multiple pending large Requests
        [Test]
        public async Task Test15_ConcurrentLargeRequests()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var localClient = client;
            var largePayload = new string('B', 10000); // 10KB

            server.OnRequest<StringValue, StringValue>("ConcLarge", async req => 
            {
                await Task.Delay(10); // simulate work
                return new StringValue { Value = req.request.Value };
            });

            var tasks = Enumerable.Range(0, 50).Select(_ => 
            {
                return localClient.RequestAsync<StringValue, StringValue>("ConcLarge", new StringValue { Value = largePayload }, TimeSpan.FromSeconds(10));
            }).ToList();
            var results = await Task.WhenAll(tasks);
            
            Assert.That(results.All(r => r.Value.Length == 10000), Is.True);
        }

        // 16. Server PUB Concurrent
        [Test]
        public void Test16_ServerConcurrentPublishToClient()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var localServer = server;
            var countdown = new CountdownEvent(1000);

            client.OnMessage<Empty>("SrvPub", _ => countdown.Signal());

            Parallel.For(0, 1000, _ => 
            {
                localServer.Publish("SrvPub", "Region1", new Empty());
            });

            Assert.That(countdown.Wait(TimeSpan.FromSeconds(5)), Is.True);
        }

        // 17. Null/Empty Message Publish
        [Test]
        public async Task Test17_EmptyMessagePublish()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            var tcs = new TaskCompletionSource<bool>();

            server.OnMessage<Empty>("EmptyP", _ => tcs.TrySetResult(true));
            client.Publish("EmptyP", new Empty());

            Assert.That(await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.True);
        }

        // 18. Async Request Exception Handling
        [Test]
        public void Test18_ServerThrows_ClientRetriesOrTimesOut()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();

            server.OnRequest("ErrReq", new Func<(string regionId, Empty request), Empty>(_ => throw new Exception("Fake Error")));

            Assert.ThrowsAsync<TimeoutException>(async () => 
                await client.RequestAsync<Empty, Empty>("ErrReq", new Empty(), TimeSpan.FromSeconds(1)));
        }

        // 19. Multiple Clients Broadcasting (QueueGroup diff)
        [Test]
        public void Test19_MultipleClients_DifferentGroup_Sub()
        {
            using var server = CreateServer("TestServer", "ClientMultiple");
            using var c1 = CreateClientFast("ClientMultiple", "Grp1");
            using var c2 = CreateClientFast("ClientMultiple", "Grp2");

            int rcv1 = 0, rcv2 = 0;
            c1.OnMessage<Empty>("BrCast", _ => Interlocked.Increment(ref rcv1));
            c2.OnMessage<Empty>("BrCast", _ => Interlocked.Increment(ref rcv2));

            // Wait subscription settling
            Thread.Sleep(200);

            server.Publish("BrCast", "Region1", new Empty());

            Thread.Sleep(500);
            Assert.That(rcv1, Is.EqualTo(1));
            Assert.That(rcv2, Is.EqualTo(1));
        }

        // 20. Dispose cleans up active tasks
        [Test]
        public void Test20_DisposeClientFast_CleansUp()
        {
            var client = CreateClientFast();
            client.Publish("Disp", new Empty());
            client.Dispose();
            
            // Publish after dispose should not crash but exit early
            client.Publish("Disp2", new Empty());
            
            Assert.ThrowsAsync<ObjectDisposedException>(() => client.RequestAsync<Empty, Empty>("XYZ", new Empty(), TimeSpan.FromSeconds(1)));
        }

        // 21. Performance Data Correctness - 200K REQ/REP
        [Test]
        public async Task Test21_Performance_200K_REQREP_Correctness()
        {
            using var server = CreateServer();
            using var client = CreateClientFast();
            int limit = 200000;

            server.OnRequest<StringValue, StringValue>("Perf200K", req => 
            {
                return new StringValue { Value = req.request.Value + "_OK" };
            });

            var semaphore = new SemaphoreSlim(10000); // Giới hạn số lượng Request chạy đồng thời để tránh cạn kiệt tài nguyên
            var tasks = new List<Task<string>>();
            var sw = Stopwatch.StartNew();

            for (int i = 0; i < limit; i++) 
            {
                await semaphore.WaitAsync();
                var index = i;
                tasks.Add(Task.Run(async () => 
                {
                    try 
                    {
                        var rep = await client.RequestAsync<StringValue, StringValue>(
                            "Perf200K", 
                            new StringValue { Value = $"Req_{index}" }, 
                            TimeSpan.FromSeconds(30));
                        return rep.Value;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            var results = await Task.WhenAll(tasks);
            sw.Stop();
            Console.WriteLine($"200K REQ/REP took {sw.ElapsedMilliseconds}ms");

            Assert.That(results.Length, Is.EqualTo(limit));
            // Kiểm tra tính đúng đắn ngẫu nhiên vài dòng
            Assert.That(results[0], Is.EqualTo("Req_0_OK"));
            Assert.That(results[limit / 2], Is.EqualTo($"Req_{limit / 2}_OK"));
            Assert.That(results[limit - 1], Is.EqualTo($"Req_{limit - 1}_OK"));
        }
    }
}
