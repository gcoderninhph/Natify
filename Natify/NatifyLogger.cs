using System;

namespace Natify
{
    public static class NatifyLogger
    {
        public static event Action<string>? OnInfo;
        public static event Action<string>? OnWarning;
        public static event Action<string>? OnError;

        internal static void Info(string message) => OnInfo?.Invoke(message);
        internal static void Warning(string message) => OnWarning?.Invoke(message);
        internal static void Error(string message) => OnError?.Invoke(message);
    }
}
