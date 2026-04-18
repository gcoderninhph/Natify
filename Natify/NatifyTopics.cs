using System;

namespace Natify
{
    public static class NatifyTopics
    {
        public static string GetClientListenSubject(string clientName, string serverName, string regionId, string topic)
        {
            return $"NatifyClient.{clientName}.{serverName}.{regionId}.{topic}";
        }

        public static string GetServerListenSubject(string serverName, string clientName, string topic)
        {
            return $"NatifyServer.{serverName}.{clientName}.*.{topic}";
        }

        public static string GetClientPublishSubject(string serverName, string clientName, string regionId, string topic)
        {
            return $"NatifyServer.{serverName}.{clientName}.{regionId}.{topic}";
        }

        public static string GetServerPublishSubject(string clientName, string serverName, string regionId, string topic)
        {
            return $"NatifyClient.{clientName}.{serverName}.{regionId}.{topic}";
        }

        public static string ExtractRegionIdFromServerSubject(string subject)
        {
            var parts = subject.Split('.');
            return parts.Length >= 4 ? parts[3] : string.Empty;
        }
    }
}