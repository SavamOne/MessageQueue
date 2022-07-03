namespace MessageQueueLibrary.Options;

public class MessageStatusParameters
{
	public MessageStatusParameters(string redisOptionsName, string messageStatusProcessorOptionsName)
	{
		RedisOptionsName = redisOptionsName;
		MessageStatusProcessorOptionsName = messageStatusProcessorOptionsName;
	}

	public string RedisOptionsName { get; }
	
	public string MessageStatusProcessorOptionsName { get; }
}