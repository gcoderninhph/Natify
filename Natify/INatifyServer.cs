using System;
using System.Threading.Tasks;
using Google.Protobuf;

#nullable enable

namespace Natify
{
    public interface INatifyServer : IAsyncDisposable
    {
        NatifyServerTriggers Trigger { get; }

        public static Task<INatifyServer> CreateAsync(string url, string serverName, string groupName,
            string clientNameToConnect, Config? config = null)
        {
            return NatifyServer.CreateAsync(url, serverName, groupName, clientNameToConnect, config);
        }

        void Publish<T>(string topic, string regionId, T message) where T : IMessage;

        void OnMessage<T>(string topic, Action<(string regionId, Data<T> data)> callback) where T : IMessage, new();

        void OnMessage<T>(string topic, Func<(string regionId, Data<T> data), Task> callback) where T : IMessage, new();

        Task<TRes> RequestAsync<TReq, TRes>(string topic, string regionId, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new();

        void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage;

        void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage;
    }
}
