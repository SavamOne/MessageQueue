namespace MessageQueueLibrary.Contracts;

public interface IBatchMessageExecutor<TKey, TValue>
{
	Task ProcessMessages(IReadOnlyCollection<ResponseMessage<TKey, TValue>> messages);
}