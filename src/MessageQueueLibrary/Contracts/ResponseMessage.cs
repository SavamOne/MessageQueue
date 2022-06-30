namespace MessageQueueLibrary.Contracts;

public class ResponseMessage<TKey, TValue>
{
	public TKey Key { get; init; }
	
	public TValue Value { get; init; }
}