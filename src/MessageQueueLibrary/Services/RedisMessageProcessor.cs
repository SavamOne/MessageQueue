using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MessageQueueLibrary.Services;

public class RedisMessageProcessor : IMessageStatusProcessor, IDisposable
{
	private readonly ConnectionMultiplexer redis;
	private readonly IDatabase database;
	private readonly RedisConnectionOptions redisOptions;
	private readonly MessageStatusOptions options;

	public RedisMessageProcessor(
		MessageStatusParameters messageStatusParameters,
		IOptionsSnapshot<RedisConnectionOptions> redisOptionsSnapshot,
		IOptionsSnapshot<MessageStatusOptions> statusOptionsSnapshot)
	{
		redisOptions = redisOptionsSnapshot.Get(messageStatusParameters.RedisOptionsName);
		options = statusOptionsSnapshot.Get(messageStatusParameters.MessageStatusProcessorOptionsName);
		
		redis = ConnectionMultiplexer.Connect(redisOptions.Configuration);
		database = redis.GetDatabase();
	}

	public async Task<MessageStatus> CanExecuteOperation(IUniqueValue value)
	{
		RedisKey key = CreateKey(value);
		ITransaction transaction = database.CreateTransaction();
		
		transaction.AddCondition(Condition.StringNotEqual(key, CreateValue(MessageStatus.Completed)));
		transaction.AddCondition(Condition.StringNotEqual(key, CreateValue(MessageStatus.InProcess)));
		
		// Выставляем TTL на случай, если операция нестандартно долго в этом статусе (операция зависла, упал сервер), чтобы выполнить заново.
		_ = transaction.StringSetAsync(key, CreateValue(MessageStatus.InProcess), options.InProcessTimeout);
		
		bool transactionResult = await transaction.ExecuteAsync();

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
		await database.StringSetAsync(CreateKey(value), CreateValue(success ? MessageStatus.Completed : MessageStatus.Faulted), options.CompletionTimeout);
	}
	
	public async Task SetCompleted(ICollection<IUniqueValue> values, bool success)
	{
		RedisValue status = CreateValue(success ? MessageStatus.Completed : MessageStatus.Faulted);
		
		ITransaction transaction = database.CreateTransaction();
		
		foreach (IUniqueValue uniqueValue in values)
		{
			// Выставляем TTL, чтобы не засорять редис. По идее должен совпадать с TTL на кафке.
			_ = transaction.StringSetAsync(CreateKey(uniqueValue), status, options.CompletionTimeout);
		}

		await transaction.ExecuteAsync();
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