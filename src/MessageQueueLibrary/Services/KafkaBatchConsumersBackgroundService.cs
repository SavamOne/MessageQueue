using MessageQueueLibrary.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumerBackgroundService<TKey, TValue> : BackgroundService
{
	private readonly KafkaBatchConsumerOptions<TKey, TValue> consumerOptions;
	private readonly IServiceProvider serviceProvider;
	private readonly ILogger<KafkaBatchConsumerBackgroundService<TKey, TValue>> logger;

	public KafkaBatchConsumerBackgroundService(KafkaBatchConsumerOptions<TKey, TValue> consumerOptions, 
		IServiceProvider serviceProvider, 
		ILogger<KafkaBatchConsumerBackgroundService<TKey, TValue>> logger)
	{
		this.consumerOptions = consumerOptions;
		this.serviceProvider = serviceProvider;
		this.logger = logger;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		Task[] consumersTasks = new Task[consumerOptions.ConsumerCount];
		
		for (int i = 0; i < consumerOptions.ConsumerCount; i++)
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