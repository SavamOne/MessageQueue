using Confluent.Kafka;
using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumer<TKey, TValue> : IDisposable
{
	protected readonly IConsumer<TKey, TValue> Consumer;
	protected readonly IBatchMessageExecutor<TKey, TValue> Executor;
	
	private readonly ILogger<KafkaBatchConsumer<TKey, TValue>> logger;
	private readonly KafkaConnectionOptions options;

	public KafkaBatchConsumer(
		KafkaConsumerParameters<TKey, TValue> consumerParameters,
		IOptionsSnapshot<KafkaConnectionOptions> optionsSnapshot,
		IBatchMessageExecutor<TKey, TValue> executor,
		ILogger<KafkaBatchConsumer<TKey, TValue>> logger)
	{
		this.logger = logger;
		options = optionsSnapshot.Get(consumerParameters.KafkaConsumerOptionsName);
		Executor = executor;

		ConsumerConfig consumerConfig = new()
		{
			BootstrapServers = options.BootstrapServers,
			GroupId = options.ConsumerGroupId,
			EnableAutoCommit = false,
			AutoOffsetReset = AutoOffsetReset.Earliest,
		};

		Consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
		   .SetKeyDeserializer(consumerParameters.KeyDeserializer)
		   .SetValueDeserializer(consumerParameters.ValueDeserializer)
		   .Build();
	}

	public async Task Execute(CancellationToken cancellationToken)
	{
		Consumer.Subscribe(options.TopicName);

		while (!cancellationToken.IsCancellationRequested)
		{
			try
			{
				using CancellationTokenSource batchWaiterCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
				batchWaiterCts.CancelAfter(options.BatchWaitTimeout);
				
				List<ConsumeResult<TKey, TValue>> consumeResults = ConsumeBatch(batchWaiterCts.Token);

				await ProcessAndCommit(consumeResults);
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
	
	protected virtual async Task ProcessAndCommit(List<ConsumeResult<TKey, TValue>> consumeResults)
	{
		var messagesToProcess = new List<ResponseMessage<TKey, TValue>>(consumeResults.Count);
		var topicPartitionOffsets = new List<TopicPartitionOffset>(consumeResults.Count);

		foreach (var consumeResult in consumeResults)
		{
			messagesToProcess.Add(new ResponseMessage<TKey, TValue>
			{
				Key = consumeResult.Message.Key,
				Value = consumeResult.Message.Value
			});
			topicPartitionOffsets.Add(consumeResult.TopicPartitionOffset);
		}
		
		await Executor.ProcessMessages(messagesToProcess);
		Consumer.Commit(topicPartitionOffsets);
	}
	
	private List<ConsumeResult<TKey, TValue>> ConsumeBatch(CancellationToken token)
	{
		List<ConsumeResult<TKey, TValue>> consumeResults = new(options.BatchSize);
		
		while (!token.IsCancellationRequested && consumeResults.Count < options.BatchSize)
		{
			ConsumeResult<TKey, TValue>? result;
			
			try
			{
				result = Consumer.Consume(token);
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
		Consumer.Close();
		Consumer.Dispose();
	}
}