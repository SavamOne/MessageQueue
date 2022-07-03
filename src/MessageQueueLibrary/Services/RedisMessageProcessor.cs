using MessageQueueLibrary.Contracts;
using StackExchange.Redis;

namespace MessageQueueLibrary.Services;

public class RedisMessageProcessor : IMessageStatusProcessor, IDisposable
{
	private readonly ConnectionMultiplexer redis;
	private readonly IDatabase database;
	
	public RedisMessageProcessor()
	{
		redis = ConnectionMultiplexer.Connect("localhost");
		database = redis.GetDatabase();
	}

	public async Task<MessageStatus> CanExecuteOperation(IUniqueValue value)
	{
		RedisKey key = CreateKey(value);
		ITransaction transaction = database.CreateTransaction();
		
		transaction.AddCondition(Condition.StringNotEqual(key, CreateValue(MessageStatus.Completed)));
		transaction.AddCondition(Condition.StringNotEqual(key, CreateValue(MessageStatus.InProcess)));
		
		var statusTask = transaction.StringSetAsync(key, CreateValue(MessageStatus.InProcess), TimeSpan.FromSeconds(10));
		var executeTransactionTask = transaction.ExecuteAsync();
		var transactionResult = database.Wait(executeTransactionTask);

		if (!transactionResult)
		{
			string? result = await database.StringGetAsync(key);
			if (!string.IsNullOrEmpty(result) && Enum.TryParse(result, out MessageStatus statusValue))
			{
				return statusValue;
			}
		}

		// Когда сообщение находится в состоянии Faulted, то в редисе оно будет заменяться на InProcess,
		// чтобы упавшую задачу мог забрать только один consumer. Однако метод будет все равно возвращать как New.
		// TODO: Не критично, но неплохо, чтобы в случае Faulted в редисе менялось на InProcess, а метод возврашал Faulted. 
		return MessageStatus.New;
	}
	
	public async Task SetCompleted(IUniqueValue value, bool success)
	{
		await database.StringSetAsync(CreateKey(value), CreateValue(success ? MessageStatus.Completed : MessageStatus.Faulted));
	}
	
	public Task SetCompleted(ICollection<IUniqueValue> values, bool success)
	{
		var kvp = new KeyValuePair<RedisKey, RedisValue>[values.Count];
		
		RedisValue status = CreateValue(success ? MessageStatus.Completed : MessageStatus.Faulted);
		
		int i = 0;
		foreach (IUniqueValue uniqueValue in values)
		{
			kvp[i++] = new KeyValuePair<RedisKey, RedisValue>(CreateKey(uniqueValue), status);
		}
		
		return database.StringSetAsync(kvp);
	}

	public void Dispose()
	{
		redis.Dispose();
	}

	private static RedisKey CreateKey(IUniqueValue value)
	{
		return $"messageQueueValue.{value.Id:B}";
	}
	
	private static RedisValue CreateValue(MessageStatus messageStatus)
	{
		return messageStatus.ToString("G");
	}
}