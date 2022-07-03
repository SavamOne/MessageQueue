namespace MessageQueueLibrary.Contracts;

public interface IMessageStatusProcessor
{
	Task<MessageStatus> CanExecuteOperation(IUniqueValue value);

	Task SetCompleted(IUniqueValue value, bool success);
	
	Task SetCompleted(ICollection<IUniqueValue> value, bool success);
}