namespace Natify;

public struct BatchMessage(string subject, RentedBuffer payload, string messageType, string reqId, string repId)
{
    public string Subject { get; } = subject;
    public RentedBuffer Payload { get; } = payload;
    public string MessageType { get; } = messageType;
    public string ReqId { get; } = reqId;
    public string RepId { get; } = repId;
}