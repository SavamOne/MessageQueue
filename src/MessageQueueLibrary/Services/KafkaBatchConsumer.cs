using Confluent.Kafka;
using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumer<TKey, TValue> : IDisposable
{
	protected readonly IConsumer<TKey, TValue> Consumer;
	protected readonly KafkaBatchConsumerParameters<TKey, TValue> ConsumerParameters;
	protected readonly IBatchMessageExecutor<TKey, TValue> Executor;
	protected readonly ILogger<KafkaBatchConsumer<TKey, TValue>> Logger;
	
	public KafkaBatchConsumer(
		KafkaBatchConsumerParameters<TKey, TValue> consumerParameters,
		IBatchMessageExecutor<TKey, TValue> executor,
		ILogger<KafkaBatchConsumer<TKey, TValue>> logger)
	{
		ConsumerParameters = consumerParameters;
		Executor = executor;
		Logger = logger;
		
		Consumer = new ConsumerBuilder<TKey, TValue>(consumerParameters.ConsumerConfig)
		   .SetKeyDeserializer(consumerParameters.KeyDeserializer)
		   .SetValueDeserializer(consumerParameters.ValueDeserializer)
		   .Build();
	}

	public async Task Execute(CancellationToken cancellationToken)
	{
		Consumer.Subscribe(ConsumerParameters.TopicName);

		while (!cancellationToken.IsCancellationRequested)
		{
			try
			{
				using CancellationTokenSource batchWaiterCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
				batchWaiterCts.CancelAfter(ConsumerParameters.BatchWaitTimeout);
				
				List<ConsumeResult<TKey, TValue>> consumeResults = ConsumeBatch(batchWaiterCts.Token);

				await ProcessAndCommit(consumeResults);
			}
			catch (ConsumeException consumeException)
			{
				Logger.LogError(consumeException, "Unknown consume error");
			}
			catch (Exception e)
			{
				Logger.LogError(e, "Unknown error");
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
		List<ConsumeResult<TKey, TValue>> consumeResults = new(ConsumerParameters.BatchSize);
		
		while (!token.IsCancellationRequested && consumeResults.Count < ConsumerParameters.BatchSize)
		{
			ConsumeResult<TKey, TValue>? result;
			
			try
			{
				result = Consumer.Consume(token);
			}
			catch (OperationCanceledException)
			{
				Logger.LogDebug("Batch operation timeout or consume cancel");
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