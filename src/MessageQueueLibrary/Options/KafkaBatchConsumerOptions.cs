using Confluent.Kafka;
using MessageQueueLibrary.Contracts;

namespace MessageQueueLibrary.Options;

public class KafkaBatchConsumerOptions<TKey, TValue>
{
	public KafkaBatchConsumerOptions(
		string topicName, 
		int consumerCount, 
		int batchSize,
		Func<IServiceProvider, IBatchMessageExecutor<TKey, TValue>> batchExecutorFactory,
		ConsumerConfig consumerConfig,
		TimeSpan batchWaitTimeout)
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
		BatchExecutorFactory = batchExecutorFactory ?? throw new ArgumentNullException(nameof(batchExecutorFactory));
		ConsumerConfig = consumerConfig ?? throw new ArgumentNullException(nameof(consumerConfig));
		BatchWaitTimeout = batchWaitTimeout;
	}

	public string TopicName { get; }
	
	public int ConsumerCount { get; }

	public int BatchSize { get; }
	
	public TimeSpan BatchWaitTimeout { get; }
	
	public Func<IServiceProvider, IBatchMessageExecutor<TKey, TValue>> BatchExecutorFactory { get; } 
	
	public ConsumerConfig ConsumerConfig { get; }
}