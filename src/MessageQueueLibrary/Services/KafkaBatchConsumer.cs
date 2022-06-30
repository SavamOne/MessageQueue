using Confluent.Kafka;
using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaConsumer<TKey, TValue> : IDisposable
{
	private readonly KafkaBatchConsumerOptions<TKey, TValue> consumerOptions;
	private readonly IBatchMessageExecutor<TKey, TValue> executor;
	private readonly ILogger<KafkaConsumer<TKey, TValue>> logger;
	private readonly IConsumer<TKey, TValue> consumer;

	public KafkaConsumer(
		KafkaBatchConsumerOptions<TKey, TValue> consumerOptions,
		IBatchMessageExecutor<TKey, TValue> executor,
		ILogger<KafkaConsumer<TKey, TValue>> logger)
	{
		this.consumerOptions = consumerOptions;
		this.executor = executor;
		this.logger = logger;
		
		consumer = new ConsumerBuilder<TKey, TValue>(consumerOptions.ConsumerConfig).Build();
	}

	public async Task Execute(CancellationToken cancellationToken)
	{
		consumer.Subscribe(consumerOptions.TopicName);

		while (!cancellationToken.IsCancellationRequested)
		{
			try
			{
				using CancellationTokenSource batchWaiterCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
				batchWaiterCts.CancelAfter(consumerOptions.BatchWaitTimeout);
				
				(List<ResponseMessage<TKey, TValue>> responseMessages, List<TopicPartitionOffset> topicPartitionOffsets) = ConsumeBatch(batchWaiterCts.Token);
				
				await executor.ProcessMessages(responseMessages);
				
				consumer.Commit(topicPartitionOffsets);
			}
			catch (ConsumeException consumeException)
			{
				logger.LogError(consumeException, "Unknown consume error");
			}
			catch (Exception e)
			{
				logger.LogError(e, "Unknown error");
				throw;
			}
		}
	}
	
	private (List<ResponseMessage<TKey, TValue>>, List<TopicPartitionOffset>) ConsumeBatch(CancellationToken token)
	{
		List<ResponseMessage<TKey, TValue>> messages = new(consumerOptions.BatchSize);
		List<TopicPartitionOffset> topicPartitionOffsets = new(consumerOptions.BatchSize);
		
		while (!token.IsCancellationRequested && messages.Count < consumerOptions.BatchSize)
		{
			ConsumeResult<TKey, TValue>? result;
			
			try
			{
				result = consumer.Consume(token);
			}
			catch (OperationCanceledException)
			{
				logger.LogDebug("Batch operation timeout or consume cancel");
				break;
			}
			
			if (result is null)
			{
				continue;
			}
				
			messages.Add(new ResponseMessage<TKey, TValue>
			{
				Key = result.Message.Key,
				Value =  result.Message.Value
			});
			
			topicPartitionOffsets.Add(result.TopicPartitionOffset);
		}

		return (messages, topicPartitionOffsets);
	}
	
	public void Dispose()
	{
		consumer.Close();
		consumer.Dispose();
	}
}