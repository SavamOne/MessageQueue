using Confluent.Kafka;
using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumer<TKey, TValue> : IDisposable
{
	private readonly KafkaBatchConsumerOptions<TKey, TValue> consumerOptions;
	private readonly IBatchMessageExecutor<TKey, TValue> executor;
	private readonly ILogger<KafkaBatchConsumer<TKey, TValue>> logger;
	private readonly IConsumer<TKey, TValue> consumer;
	
	public KafkaBatchConsumer(
		KafkaBatchConsumerOptions<TKey, TValue> consumerOptions,
		IBatchMessageExecutor<TKey, TValue> executor,
		ILogger<KafkaBatchConsumer<TKey, TValue>> logger)
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
				
				List<ConsumeResult<TKey, TValue>> consumeResults = ConsumeBatch(batchWaiterCts.Token);

				var items = GetToProcessAndToCommit(consumeResults);
				
				await executor.ProcessMessages(items.ToProcess);
				consumer.Commit(items.ToCommit);
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
	
	protected virtual (IReadOnlyCollection<TopicPartitionOffset> ToCommit, IReadOnlyCollection<ResponseMessage<TKey, TValue>> ToProcess) GetToProcessAndToCommit(List<ConsumeResult<TKey, TValue>> consumeResults)
	{
		var messagesToProcess = new ResponseMessage<TKey, TValue>[consumeResults.Count];
		var topicPartitionOffsetsToCommit = new TopicPartitionOffset[consumeResults.Count];

		for (int index = 0; index < consumeResults.Count; index++)
		{
			ConsumeResult<TKey, TValue> consumeResult = consumeResults[index];
			
			messagesToProcess[index] = new ResponseMessage<TKey, TValue>()
			{
				Key = consumeResult.Message.Key,
				Value = consumeResult.Message.Value
			};
			
			topicPartitionOffsetsToCommit[index] = consumeResult.TopicPartitionOffset;
		}
		return (topicPartitionOffsetsToCommit, messagesToProcess);
	}

	private List<ConsumeResult<TKey, TValue>> ConsumeBatch(CancellationToken token)
	{
		List<ConsumeResult<TKey, TValue>> consumeResults = new(consumerOptions.BatchSize);
		
		while (!token.IsCancellationRequested && consumeResults.Count < consumerOptions.BatchSize)
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
			
			if (result is not null)
			{
				consumeResults.Add(result);
			}
		}

		return consumeResults;
	}
	
	public void Dispose()
	{
		consumer.Close();
		consumer.Dispose();
	}
}