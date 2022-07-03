using Confluent.Kafka;
using MessageQueueLibrary.Contracts;

namespace MessageQueueLibrary.Registration;

public class KafkaBatchConsumerBuilder<TKey, TValue>
{
	public string KafkaConsumerOptionsName { get; set; } = null!;

	public string? MessageStatusOptionsName { get; set; }

	public string? RedisOptionsName { get; set; }
	
	public Func<IServiceProvider, IBatchMessageExecutor<TKey, TValue>> BatchExecutorFactory { get; set; } = null!;
	
	public IDeserializer<TKey>? KeySerializer { get; set; }
	
	public IDeserializer<TValue>? ValueDeserializer { get; set; }
}