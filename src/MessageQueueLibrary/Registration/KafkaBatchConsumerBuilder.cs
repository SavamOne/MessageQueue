using Confluent.Kafka;
using MessageQueueLibrary.Contracts;

namespace MessageQueueLibrary.Registration;

public class KafkaBatchConsumerBuilder<TKey, TValue>
{
	public string TopicName { get; set; } = null!;
	
	public int ConsumerCount { get; set; } = 1;

	public int BatchSize { get; set; } = 5;

	public TimeSpan BatchWaitTimeout { get; set; } = TimeSpan.FromSeconds(10);

	public Func<IServiceProvider, IBatchMessageExecutor<TKey, TValue>> BatchExecutorFactory { get; set; } = null!;
	
	public ConsumerConfig ConsumerConfig { get; set; } = null!;
	
	public IDeserializer<TKey>? KeySerializer { get; set; }
	
	public IDeserializer<TValue>? ValueDeserializer { get; set; }
}