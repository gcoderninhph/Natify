using System;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Natify
{
    /// <summary>
    /// Server-side interface for Natify micro-batching communication over NATS.
    /// Provides publish/subscribe with region-aware routing, request/reply, triggers telemetry, and graceful shutdown.
    /// Use <see cref="CreateAsync"/> to instantiate.
    /// </summary>
    public interface INatifyServer : IAsyncDisposable
    {
        /// <summary>
        /// Telemetry and trigger-based monitoring for this server instance.
        /// </summary>
        NatifyServerTriggers Trigger { get; }

        /// <summary>
        /// Creates and connects a new server instance. Connects to NATS and starts background workers (batch, retry, ACK listener).
        /// </summary>
        /// <param name="url">NATS server URL (e.g. "nats://localhost:4222").</param>
        /// <param name="serverName">Unique name for this server. Must not contain '.'.</param>
        /// <param name="groupName">NATS queue group name for horizontal scaling.</param>
        /// <param name="clientNameToConnect">Exact client name this server communicates with. Must not contain '.'. Cannot be wildcard "*" because it is used in both subscribe and publish subjects.</param>
        /// <param name="config">Optional micro-batching tuning parameters.</param>
        /// <returns>A connected <see cref="INatifyServer"/> instance.</returns>
        public static Task<INatifyServer> CreateAsync(string url, string serverName, string groupName,
            string clientNameToConnect, Config? config = null)
        {
            return NatifyServer.CreateAsync(url, serverName, groupName, clientNameToConnect, config);
        }

        /// <summary>
        /// Publishes a protobuf message to a specific client region. Messages are micro-batched and sent asynchronously.
        /// </summary>
        /// <typeparam name="T">Protobuf message type implementing <see cref="IMessage"/>.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="regionId">Target region of the client to send to (e.g. "VN", "US").</param>
        /// <param name="message">The protobuf message to publish.</param>
        void Publish<T>(string topic, string regionId, T message) where T : IMessage;

        /// <summary>
        /// Subscribes to messages of type <typeparamref name="T"/> on the given topic from any client region.
        /// Callback receives both the client's region identifier and the deserialized message envelope.
        /// </summary>
        /// <typeparam name="T">Protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="callback">Synchronous handler receiving (regionId, data) tuple.</param>
        void OnMessage<T>(string topic, Action<(string regionId, Data<T> data)> callback) where T : IMessage, new();

        /// <summary>
        /// Subscribes to messages of type <typeparamref name="T"/> on the given topic from any client region with an async handler.
        /// Callback receives both the client's region identifier and the deserialized message envelope.
        /// </summary>
        /// <typeparam name="T">Protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="callback">Asynchronous handler receiving (regionId, data) tuple.</param>
        void OnMessage<T>(string topic, Func<(string regionId, Data<T> data), Task> callback) where T : IMessage, new();

        /// <summary>
        /// Sends a request to a specific client region and awaits a reply. Supports timeout.
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type.</typeparam>
        /// <typeparam name="TRes">Expected reply protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="regionId">Target client region (e.g. "VN", "US").</param>
        /// <param name="requestData">The request message to send.</param>
        /// <param name="timeout">Maximum time to wait for a reply.</param>
        /// <returns>The deserialized reply message.</returns>
        /// <exception cref="TimeoutException">Thrown when no reply is received within the timeout.</exception>
        Task<TRes> RequestAsync<TReq, TRes>(string topic, string regionId, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new();

        /// <summary>
        /// Registers a synchronous handler for incoming requests from clients on the given topic.
        /// The handler receives the client's regionId and the request message as a tuple.
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <typeparam name="TRep">Reply protobuf message type.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="handler">Function that receives (regionId, request) and returns a reply.</param>
        void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage;

        /// <summary>
        /// Registers an asynchronous handler for incoming requests from clients on the given topic.
        /// The handler receives the client's regionId and the request message as a tuple.
        /// </summary>
        /// <typeparam name="TReq">Request protobuf message type. Must have a parameterless constructor.</typeparam>
        /// <typeparam name="TRep">Reply protobuf message type.</typeparam>
        /// <param name="topic">Logical topic name (no prefix needed).</param>
        /// <param name="handlerAsync">Async function that receives (regionId, request) and returns a reply.</param>
        void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage;
    }
}
