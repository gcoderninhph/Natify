using System;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Natify
{
    public interface INatifyClient : IAsyncDisposable
    {
        NatifyClientTriggers Trigger { get; }

        public static Task<INatifyClient> CreateFast(string url, string clientName, string groupName,
            string regionId,
            string serverNameToConnect, Config? config = null)
        {
            return NatifyClientFast.Create(url, clientName, groupName, regionId, serverNameToConnect, config);
        }

        public static Task<INatifyClient> Create(string url, string clientName, string groupName,
            string regionId,
            string serverNameToConnect, Config? config = null)
        {
            return NatifyClient.Create(url, clientName, groupName, regionId, serverNameToConnect, config);
        }

        /// <summary>
        /// Dequeues and invokes pending callbacks on the main thread (Unity).
        /// Has no effect for the Fast variant.
        /// </summary>
        void Tick();

        void Publish<T>(string topic, T message) where T : IMessage;
        void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new();
        void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new();

        Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new();

        void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage;

        void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage;
    }
}
