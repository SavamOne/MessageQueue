using Confluent.Kafka;
using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaBatchUniqueConsumer<TKey, TValue> : KafkaBatchConsumer<TKey, TValue> where TValue : IUniqueValue
{
	private readonly IMessageStatusProcessor messageStatusProcessor;

	public KafkaBatchUniqueConsumer(KafkaBatchConsumerParameters<TKey, TValue> consumerParameters,
		IBatchMessageExecutor<TKey, TValue> executor,
		IMessageStatusProcessor messageStatusProcessor,
		ILogger<KafkaBatchUniqueConsumer<TKey, TValue>> logger)
		: base(consumerParameters, executor, logger)
	{
		this.messageStatusProcessor = messageStatusProcessor;
	}

	protected override async Task ProcessAndCommit(List<ConsumeResult<TKey, TValue>> consumeResults)
	{
		var topicPartitionOffsetsToCommit = new List<TopicPartitionOffset>();
		var valuesToComplete = new List<IUniqueValue>();
		var messagesToExecute = new List<ResponseMessage<TKey, TValue>>();
		
		foreach (var consumeResult in consumeResults)
		{
			MessageStatus result = await messageStatusProcessor.CanExecuteOperation(consumeResult.Message.Value);
			
			if(result is MessageStatus.New or MessageStatus.Faulted)
			{
				messagesToExecute.Add(new ResponseMessage<TKey, TValue>
				{
					Key = consumeResult.Message.Key,
					Value = consumeResult.Message.Value
				});
				topicPartitionOffsetsToCommit.Add(consumeResult.TopicPartitionOffset);
				valuesToComplete.Add(consumeResult.Message.Value);
			}
			else if(result is MessageStatus.Completed)
			{
				valuesToComplete.Add(consumeResult.Message.Value);
			}
		}

		try
		{
			await Executor.ProcessMessages(messagesToExecute);
			await messageStatusProcessor.SetCompleted(valuesToComplete, true);
			Consumer.Commit(topicPartitionOffsetsToCommit);
		}
		catch (Exception)
		{
			await messageStatusProcessor.SetCompleted(valuesToComplete, false);
			throw;
		}
		
	}
}