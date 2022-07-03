using Confluent.Kafka;

namespace MessageQueueLibrary.Options;

public class KafkaBatchConsumerParameters<TKey, TValue>
{
	public KafkaBatchConsumerParameters(string topicName,
		int consumerCount,
		int batchSize,
		ConsumerConfig consumerConfig,
		TimeSpan batchWaitTimeout,
		IDeserializer<TKey>? keyDeserializer,
		IDeserializer<TValue>? valueDeserializer)
	{
		if (string.IsNullOrEmpty(topicName))
		{
			throw new ArgumentNullException(nameof(topicName));
		}
		if (consumerCount < 1)
		{
			throw new ArgumentOutOfRangeException(nameof(consumerCount));
		}
		if (batchSize < 1)
		{
			throw new ArgumentOutOfRangeException(nameof(batchSize));
		}
		if (batchWaitTimeout <= TimeSpan.Zero)
		{
			throw new ArgumentOutOfRangeException(nameof(batchWaitTimeout));
		}
		
		TopicName = topicName;
		ConsumerCount = consumerCount;
		BatchSize = batchSize;
		ConsumerConfig = consumerConfig ?? throw new ArgumentNullException(nameof(consumerConfig));
		BatchWaitTimeout = batchWaitTimeout;
		ValueDeserializer = valueDeserializer;
		KeyDeserializer = keyDeserializer;
	}

	public string TopicName { get; }
	
	public int ConsumerCount { get; }

	public int BatchSize { get; }
	
	public TimeSpan BatchWaitTimeout { get; }

	public ConsumerConfig ConsumerConfig { get; }
	
	public IDeserializer<TKey>? KeyDeserializer { get; }
	
	public IDeserializer<TValue>? ValueDeserializer { get; }
}