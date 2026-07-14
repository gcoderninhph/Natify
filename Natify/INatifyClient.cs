using System;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Natify
{
    /// <summary>
    /// Client-side interface for Natify micro-batching communication over NATS.
    /// Provides publish/subscribe, request/reply, triggers telemetry, and graceful shutdown.
    /// Use <see cref="Create"/> for Unity (main-thread callbacks via Tick) or <see cref="CreateFast"/> for backend (ThreadPool callbacks).
    /// </summary>
    public interface INatifyClient : IAsyncDisposable
    {
        /// <summary>
        /// Telemetry and trigger-based monitoring for this client instance.
        /// </summary>
        NatifyClientTriggers Trigger { get; }

        /// <summary>
        /// Creates a fast client variant (backend/console). Callbacks execute directly on the ThreadPool — no Tick() needed.
        /// </summary>
        /// <param name="url">NATS server URL (e.g. "nats://localhost:4222").</param>
        /// <param name="clientName">Unique name for this client. Must not contain '.'.</param>
        /// <param name="groupName">NATS queue group name for horizontal scaling.</param>
        /// <param name="regionId">Region identifier of this client (e.g. "VN", "US"). Must not contain '.'.</param>
        /// <param name="serverNameToConnect">Exact server name this client communicates with. Must not contain '.'.</param>
        /// <param name="config">Optional micro-batching tuning parameters.</param>
        /// <returns>A connected <see cref="INatifyClient"/> instance.</returns>
        public static Task<INatifyClient> CreateFast(string url, string clientName, string groupName,
            string regionId,
            string serverNameToConnect, Config? config = null)
        {
            return NatifyClientFast.Create(url, clientName, groupName, regionId, serverNameToConnect, config);
        }

        /// <summary>
        /// Creates a client variant for Unity. Callbacks are queued and dispatched on the main thread via <see cref="Tick"/>.
        /// </summary>
        /// <param name="url">NATS server URL (e.g. "nats://localhost:4222").</param>
        /// <param name="clientName">Unique name for this client. Must not contain '.'.</param>
        /// <param name="groupName">NATS queue group name for horizontal scaling.</param>
        /// <param name="regionId">Region identifier of this client (e.g. "VN", "US"). Must not contain '.'.</param>
        /// <param name="serverNameToConnect">Exact server name this client communicates with. Must not contain '.'.</param>
        /// <param name="config">Optional micro-batching tuning parameters.</param>
        /// <returns>A connected <see cref="INatifyClient"/> instance.</returns>
        public static Task<INatifyClient> Create(string url, string clientName, string groupName,
            string regionId,
            string serverNameToConnect, Config? config = null)
        {
            return NatifyClient.Create(url, clientName, groupName, regionId, serverNameToConnect, config);
        }

        /// <summary>
        /// Dequeues and invokes pending callbacks on the main thread (Unity).
        /// Processes up to 100 actions per frame. Has no effect for the Fast variant.
        /// Must be called every frame in Update() for Unity clients.
        /// </summary>
        void Tick();

        /// <summary>
        /// Publishes a protobuf message to the server. Messages are micro-batched and sent asynchronously.
        /// </summary>
        /// <typeparam name="T">Protobuf message type implementing <see cref="IMessage"/>.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="message">The protobuf message to publish.</param>
        void Publish<T>(string topic, T message) where T : IMessage;

        /// <summary>
        /// Subscribes to messages of type <typeparamref name="T"/> on the given topic.
        /// Callback is invoked on the main thread (Unity variant) or ThreadPool (Fast variant).
        /// </summary>
        /// <typeparam name="T">Protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="callback">Synchronous handler receiving the deserialized message envelope.</param>
        void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new();

        /// <summary>
        /// Subscribes to messages of type <typeparamref name="T"/> on the given topic with an async handler.
        /// Callback is invoked on the main thread (Unity variant) or ThreadPool (Fast variant).
        /// </summary>
        /// <typeparam name="T">Protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="callback">Asynchronous handler receiving the deserialized message envelope.</param>
        void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new();

        /// <summary>
        /// Sends a request and awaits a reply from the server. Supports timeout.
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type.</typeparam>
        /// <typeparam name="TRes">Expected reply protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="requestData">The request message to send.</param>
        /// <param name="timeout">Maximum time to wait for a reply.</param>
        /// <returns>The deserialized reply message.</returns>
        /// <exception cref="TimeoutException">Thrown when no reply is received within the timeout.</exception>
        Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new();

        /// <summary>
        /// Registers a synchronous handler for incoming requests on the given topic.
        /// The handler receives the request message directly (no regionId since server owns routing context).
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <typeparam name="TRep">Reply protobuf message type.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="handler">Function that processes the request and returns a reply.</param>
        void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage;

        /// <summary>
        /// Registers an asynchronous handler for incoming requests on the given topic.
        /// The handler receives the request message directly (no regionId since server owns routing context).
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <typeparam name="TRep">Reply protobuf message type.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="handlerAsync">Async function that processes the request and returns a reply.</param>
        void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage;
    }
}
