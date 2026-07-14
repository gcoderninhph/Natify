using System;

namespace Natify
{
    public static class NatifyTopics
    {
        private static void ValidateNoDot(string value, string paramName)
        {
            if (value == null) throw new ArgumentNullException(paramName);
            if (value.Contains('.')) throw new ArgumentException($"'{paramName}' must not contain '.' character", paramName);
        }

        private static void ValidateTopic(string topic, string paramName)
        {
            if (topic == null) throw new ArgumentNullException(paramName);
        }

        public static string GetClientListenSubject(string clientName, string serverName, string regionId, string topic)
        {
            ValidateNoDot(clientName, nameof(clientName));
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(regionId, nameof(regionId));
            ValidateTopic(topic, nameof(topic));
            return $"NatifyClient.{clientName}.{serverName}.{regionId}.{topic}";
        }

        public static string GetClientReplySubject(string clientName, string serverName, string instanceId)
        {
            ValidateNoDot(clientName, nameof(clientName));
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(instanceId, nameof(instanceId));
            return $"NatifyClient.{clientName}.{serverName}.{instanceId}";
        }

        public static string GetServerListenSubject(string serverName, string clientName, string topic)
        {
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(clientName, nameof(clientName));
            ValidateTopic(topic, nameof(topic));
            return $"NatifyServer.{serverName}.{clientName}.*.{topic}";
        }

        public static string GetServerReplySubject(string serverName, string clientName, string instanceId)
        {
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(clientName, nameof(clientName));
            ValidateNoDot(instanceId, nameof(instanceId));
            return $"NatifyServer.{serverName}.{clientName}.{instanceId}";
        }

        public static string GetClientPublishSubject(string serverName, string clientName, string regionId,
            string topic)
        {
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(clientName, nameof(clientName));
            ValidateNoDot(regionId, nameof(regionId));
            ValidateTopic(topic, nameof(topic));
            return $"NatifyServer.{serverName}.{clientName}.{regionId}.{topic}";
        }

        public static string GetServerPublishSubject(string clientName, string serverName, string regionId,
            string topic)
        {
            ValidateNoDot(clientName, nameof(clientName));
            ValidateNoDot(serverName, nameof(serverName));
            ValidateNoDot(regionId, nameof(regionId));
            ValidateTopic(topic, nameof(topic));
            return $"NatifyClient.{clientName}.{serverName}.{regionId}.{topic}";
        }


        public static string ExtractRegionIdFromServerSubject(string subject)
        {
            if (subject == null) throw new ArgumentNullException(nameof(subject));
            var parts = subject.Split('.');
            return parts.Length >= 4 ? parts[3] : string.Empty;
        }
    }
}