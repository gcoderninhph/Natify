# Natify — Complete API Reference

> **Version:** 1.0.2  
> **Target:** netstandard2.0 (C# 9.0)  
> **Dependencies:** Google.Protobuf 3.34.1, NATS.Net 2.7.3, Microsoft.Bcl.AsyncInterfaces 10.0.6  
> **NuGet:** Local package only (Natify.1.0.2.nupkg)

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Protobuf Schema](#2-protobuf-schema)
3. [Core Types](#3-core-types)
4. [NatifyTopics — Subject Routing](#4-natifytopics--subject-routing)
5. [NatifySerializer](#5-natifyserializer)
6. [NatifyClient (Unity)](#6-natifyclient-unity)
7. [NatifyClientFast (Backend)](#7-natifyclientfast-backend)
8. [NatifyServer](#8-natifyserver)
9. [Triggers System](#9-triggers-system)
10. [TimedSortedSet — Hierarchical Time Wheel](#10-timedsortedset--hierarchical-time-wheel)
11. [Lifecycle & Dispose](#11-lifecycle--dispose)
12. [Reliability Guarantees](#12-reliability-guarantees)
13. [Performance](#13-performance)
14. [Unity-Specific Behavior](#14-unity-specific-behavior)
15. [Usage Examples](#15-usage-examples)

---

## 1. Architecture

Natify is a **micro-batching** communication layer on top of NATS Core. It wraps individual protobuf messages into `NatifyBatch` packets before publishing.

```
Publish(message) → Channel (funnel) → BatchWorker (50ms / 1000 msg / 50KB) → NATS
Subscribe(subject) → NATS → Deserialize NatifyBatch → Iterate inner messages → Callback
```

### Client Variants

| Variant | Target | Tick() | Callback Thread |
|----------|--------|:---:|-----------------|
| `NatifyClient` | Unity | Required | Main thread (via Tick) |
| `NatifyClientFast` | Backend | None | ThreadPool |

---

## 2. Protobuf Schema

```protobuf
syntax = "proto3";
package Natify;

message NatifyBatch {
  repeated bytes payloads       = 1;
  repeated string reqId         = 2;
  repeated string msgType       = 3;  // PUB, REQ, REP
  repeated string repId         = 4;
  string formInstanceId         = 5;
}
```

---

## 3. Core Types

### `Data<T>`

```csharp
public readonly struct Data<T>
{
    public readonly T Value;
    public readonly string InstanceId;  // Sender GUID
    public readonly string ReqId;       // Request ID
    public readonly string RepId;       // Reply-correlation ID
}
```

### `UnackedMessage`

```csharp
public class UnackedMessage
{
    public string Subject { get; set; }
    public byte[] Payload { get; set; }
    public string BatchId { get; set; }
    public DateTime LastSent { get; set; }
    public int RetryCount { get; set; }
}
```

---

## 4. NatifyTopics — Subject Routing

```csharp
public static class NatifyTopics
```

| Direction | Pattern |
|-----------|---------|
| Client→Server publish | `NatifyServer.{server}.{client}.{region}.{topic}` |
| Server→Client publish | `NatifyClient.{client}.{server}.{region}.{topic}` |
| Server subscribes | `NatifyServer.{server}.{client}.*.{topic}` (wildcard) |
| Client subscribes | `NatifyClient.{client}.{server}.{region}.{topic}` (exact) |
| ACK Client→Server | `NatifyServer.{server}.{client}.{region}.ACK.{batchId}` |
| ACK Server→Client | `NatifyClient.{client}.{server}.{region}.ACK.{batchId}` |

```csharp
public static string ExtractRegionIdFromServerSubject(string subject)
```

---

## 5. NatifySerializer

```csharp
public static class NatifySerializer
{
    // Uses ArrayPool<byte> — caller MUST return buffer
    public static (byte[] Buffer, int Length) Serialize<T>(T message) where T : IMessage;
    public static T Deserialize<T>(byte[] data, int length) where T : IMessage, new();
}
```

---

## 6. NatifyClient (Unity)

```csharp
public class NatifyClient : IDisposable
```

### Constructor
```csharp
public NatifyClient(string url, string clientName, string groupName,
    string regionId, string serverNameToConnect)
```

Connects to NATS, starts BatchWorker, RetryWorker, ACK Listener.

### Public API

```csharp
// One-way publish (returns immediately)
public void Publish<T>(string topic, T message) where T : IMessage

// Subscribe (callback via Tick on main thread)
public void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new()
public void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new()

// RPC request (Client → Server)
public async Task<TRes> RequestAsync<TReq, TRes>(
    string topic, TReq requestData, TimeSpan timeout)
    where TReq : IMessage where TRes : IMessage, new()

// Handle server requests (sync)
public void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
    where TReq : IMessage, new() where TRep : IMessage

// Handle server requests (async)
public void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
    where TReq : IMessage, new() where TRep : IMessage

// MUST call every frame in Unity
public void Tick()
```

### Batching Parameters
| Param | Value |
|-------|-------|
| MaxCount | 1000 msg/batch |
| MaxSize | 50 KB |
| MaxWait | 50ms |
| AckTimeout | 100ms |
| MaxRetries | 10 |

### Dedup Flow
1. Extract `Natify-BatchId` header
2. Send ACK immediately
3. `_processedMessages.TryAdd` → if duplicate, skip processing
4. Time wheel TTL: 10 seconds

---

## 7. NatifyClientFast (Backend)

```csharp
public class NatifyClientFast : IDisposable
```

### Differences from NatifyClient

| Feature | NatifyClient | NatifyClientFast |
|---------|:---:|:---:|
| Main-thread queue | Yes | No |
| `Tick()` | Required | Does not exist |
| Callbacks run on | Main thread | ThreadPool |

All other APIs identical. No main-thread queuing — callbacks fire inline in NATS subscription loop.

---

## 8. NatifyServer

```csharp
public class NatifyServer : IDisposable
```

### Constructor
```csharp
public NatifyServer(string url, string serverName, string groupName,
    string clientNameToConnect)
```

### Public API

```csharp
// Publish to client — MUST specify regionId
public void Publish<T>(string topic, string regionId, T message) where T : IMessage

// Subscribe (receives regionId from wildcard)
public void OnMessage<T>(string topic,
    Action<(string regionId, Data<T> data)> callback) where T : IMessage, new()

public void OnMessage<T>(string topic,
    Func<(string regionId, Data<T> data), Task> callback) where T : IMessage, new()

// RPC to client
public async Task<TRes> RequestAsync<TReq, TRes>(
    string topic, string regionId, TReq requestData, TimeSpan timeout)
    where TReq : IMessage where TRes : IMessage, new()

// Handle client requests
public void OnRequest<TReq, TRep>(string topic,
    Func<(string regionId, TReq request), TRep> handler)
    where TReq : IMessage, new() where TRep : IMessage

public void OnRequest<TReq, TRep>(string topic,
    Func<(string regionId, TReq request), Task<TRep>> handlerAsync)
    where TReq : IMessage, new() where TRep : IMessage
```

---

## 9. Triggers System

### NatifyClientTriggers / NatifyServerTriggers

```csharp
public class NatifyClientTriggers : IDisposable
public class NatifyServerTriggers : IDisposable
```

### Telemetry Counters (thread-safe, Interlocked)
| Property | Client | Server |
|----------|:---:|:---:|
| `BytesSent` | Yes | Yes |
| `BytesReceived` | Yes | Yes |
| `MessagesSent` | Yes | Yes |
| `MessagesReceived` | Yes | Yes |
| `BatchesSent` | Yes | — |
| `BatchesReceived` | — | Yes |
| `ErrorsCount` | Yes | Yes |
| `CurrentDedupCacheSize` | Yes | Yes |
| `TotalDedupExpired` | Yes | Yes |
| `ProcessMemoryMB` | Yes | Yes |

### Trigger API
```csharp
// Register (evaluated every 500ms)
public Guid RegisterTrigger(
    Func<T, bool> condition,    // T = NatifyClientTriggers or NatifyServerTriggers
    Action<T> action,
    bool oneTime = false)

public void RemoveTrigger(Guid ruleId)
```

---

## 10. TimedSortedSet — Hierarchical Time Wheel

```csharp
public class TimedSortedSet<TKey, TValue> : IDisposable where TKey : notnull
```

### Architecture
| Wheel | Slots | Range | Covers |
|-------|-------|-------|--------|
| W0 (ms) | 256 × 10ms | 2.56s | ~2.5s |
| W1 (sec) | 64 × 2.56s | 2.73min | ~2.7min |
| W2 (min) | 64 × 2.73min | 2.9hr | ~2.9h |

Items cascade W2→W1→W0. All operations O(1).

### API
```csharp
public void AddOrUpdate(TKey key, TValue value, long expireTime)
public bool Remove(TKey key)
public int Count { get; }

public event Action<IReadOnlyList<(TKey Key, TValue Value)>> OnExpired;
public event Action<IReadOnlyList<(TKey Key, TValue Value)>> OnRemoved;
```

Shared single background thread ticks every 10ms.

---

## 11. Lifecycle & Dispose

7-step graceful shutdown:
1. `_isDisposed = true` — blocks new publishes
2. Channel `Complete()` + wait 2s for BatchWorker
3. Wait 2s for pending ACKs
4. `_cts.Cancel()`
5. Wait 1s for worker shutdown
6. `_connection.DisposeAsync()`
7. Dispose wheel, trigger, CTS

---

## 12. Reliability Guarantees

- **At-Least-Once**: ACK + retry every 100ms, max 10 retries
- **Deduplication**: Batch ID + 10s TTL time wheel
- **RPC timeout**: `CancellationTokenSource.CancelAfter(timeout)` → `TimeoutException`
- **Graceful shutdown**: Flush pending batches, wait for ACKs, drain workers

---

## 13. Performance

- Micro-batching: 50ms / 1000 msg / 50KB window
- Zero-allocation serialization: `ArrayPool<byte>.Shared.Rent()`
- Lock-free channel for publish path
- ConcurrentDictionary for dedup/ACK tracking
- O(1) time wheel for TTL expiry

---

## 14. Unity-Specific Behavior

- `#if UNITY_5_3_OR_NEWER` for `Debug.LogError` vs `Console.WriteLine`
- `Tick()` dequeues up to 100 callbacks/frame on main thread
- Use `NatifyClient` (not `NatifyClientFast`) in Unity
- `RequestAsync` replies skip main-thread queue (internal `requiresMainThread = false`)

---

## 15. Usage Examples

### Unity — Publish + Subscribe
```csharp
var client = new NatifyClient("nats://localhost:4222", "Player1", "game", "asia-east", "GameServer");

client.OnMessage<PlayerPosition>("positions", data =>
    Debug.Log($"Pos: {data.Value.X}, {data.Value.Y}"));

void Update() { client.Tick(); }

client.Publish("moves", new PlayerMove { X = 1, Y = 2 });
```

### Unity — RPC
```csharp
try
{
    var res = await client.RequestAsync<PingReq, PingRes>(
        "ping", new PingReq(), TimeSpan.FromSeconds(3));
}
catch (TimeoutException) { /* timeout */ }
```

### Backend — NatifyClientFast
```csharp
var backend = new NatifyClientFast("nats://nats:4222", "Matchmaker", "backend", "asia-east", "GameServer");

backend.OnMessage<MatchResult>("match_found", data =>
    Console.WriteLine($"MatchId: {data.Value.MatchId}"));

backend.Publish("join_queue", new JoinQueueReq { PlayerId = "p1" });
```

### Server — Publish + Subscribe
```csharp
var server = new NatifyServer("nats://localhost:4222", "GameServer", "game", "Player1");

server.OnMessage<PlayerAction>("actions", tuple =>
    Console.WriteLine($"[{tuple.regionId}] Action: {tuple.data.Value.ActionType}"));

server.Publish("spawn_enemy", "asia-east", new SpawnEnemy { EnemyId = "boss_1" });
```

### Server — Handle Client Requests
```csharp
server.OnRequest<SaveReq, SaveRes>("save", async tuple =>
{
    await db.SaveAsync(tuple.regionId, tuple.request.PlayerId, tuple.request.Data);
    return new SaveRes { Success = true };
});
```

### Triggers — Memory Alert
```csharp
server.Trigger.RegisterTrigger(
    condition: t => t.ProcessMemoryMB > 500,
    action: t => Console.WriteLine($"HIGH MEM: {t.ProcessMemoryMB:F1}MB"));

server.Trigger.RegisterTrigger(
    condition: t => t.ErrorsCount > t.MessagesReceived * 0.01 && t.MessagesReceived > 100,
    action: t => Console.WriteLine($"ERROR RATE >1%"),
    oneTime: true);
```

### Graceful Shutdown
```csharp
// Unity: void OnDestroy() { client.Dispose(); }
// Server: server.Dispose();
```

---

## Appendix A — Thread Safety

| Component | Thread-Safe | Mechanism |
|-----------|:---:|-----------|
| Publish | Yes | Channel (lock-free) |
| Callbacks (Client) | Main thread | `ConcurrentQueue` + `Tick()` |
| Callbacks (ClientFast/Server) | ThreadPool | User must handle |
| RequestAsync | Yes | `TaskCompletionSource` |
| Triggers | Yes | `ConcurrentDictionary` + `Interlocked` |
| Time Wheel | Yes | `ReaderWriterLockSlim` |
| Dispose | Call once | Idempotent |

## Appendix B — Constructor Params

| Param | Client | Server |
|-------|--------|--------|
| `url` | NATS URL | NATS URL |
| `clientName` | Self identity | Target client |
| `groupName` | Queue group | Queue group |
| `regionId` | Self region | (from subject) |
| `serverNameToConnect` | Target server | Self identity (`serverName`) |

## Appendix C — Version History

| Version | Changes |
|---------|---------|
| 1.0.0 | Initial |
| 1.0.1 | — |
| 1.0.2 | Current: `OnRemoved` event on Time Wheel, trigger enhancements |
