using MessageQueueLibrary.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumerBackgroundService<TKey, TValue> : BackgroundService
{
	private readonly KafkaBatchConsumerParameters<TKey, TValue> consumerParameters;
	private readonly IServiceProvider serviceProvider;
	private readonly ILogger<KafkaBatchConsumerBackgroundService<TKey, TValue>> logger;

	public KafkaBatchConsumerBackgroundService(KafkaBatchConsumerParameters<TKey, TValue> consumerParameters, 
		IServiceProvider serviceProvider, 
		ILogger<KafkaBatchConsumerBackgroundService<TKey, TValue>> logger)
	{
		this.consumerParameters = consumerParameters;
		this.serviceProvider = serviceProvider;
		this.logger = logger;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		Task[] consumersTasks = new Task[consumerParameters.ConsumerCount];
		
		for (int i = 0; i < consumerParameters.ConsumerCount; i++)
		{
			consumersTasks[i] = Task.Run(() => ConsumeMessages(stoppingToken), stoppingToken);;
		}

		await Task.WhenAll(consumersTasks);
	}

	private async Task ConsumeMessages(CancellationToken stoppingToken)
	{
		using IServiceScope scope = serviceProvider.CreateScope();
		using KafkaBatchConsumer<TKey, TValue> batchConsumer = scope.ServiceProvider.GetRequiredService<KafkaBatchConsumer<TKey, TValue>>();
		await batchConsumer.Execute(stoppingToken);
	}

}