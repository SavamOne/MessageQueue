using System.Text;
using MessageQueueLibrary.Contracts;
using Microsoft.Extensions.Logging;

namespace DemoApp.BatchExecutors;

public class BatchLogger<TKey, TValue> : IBatchMessageExecutor<TKey, TValue>
{
	private readonly ILogger<BatchLogger<TKey, TValue>> logger;

	public BatchLogger(ILogger<BatchLogger<TKey, TValue>> logger)
	{
		this.logger = logger;
	}
	
	public Task ProcessMessages(IReadOnlyCollection<ResponseMessage<TKey, TValue>> messages)
	{
		var builder = new StringBuilder();
		builder.AppendFormat("{0}: ", messages.Count);
		foreach (var message in messages)
		{
			builder.AppendFormat("({0}={1}) ", message.Key, message.Value);
		}
		logger.LogInformation("{0}",builder.ToString());
		
		return Task.CompletedTask;
	}
}