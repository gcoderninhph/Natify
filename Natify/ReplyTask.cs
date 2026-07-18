using Google.Protobuf;

namespace Natify;

public struct ReplyTask(TaskCompletionSource<ByteString> task, CancellationTokenSource ct)
{
    public TaskCompletionSource<ByteString> Task { get; } = task;
    public CancellationTokenSource Ct { get; } = ct;
}