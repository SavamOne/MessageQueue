using MessageQueueLibrary.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MessageQueueLibrary.Services;

public class KafkaBatchConsumerBackgroundService<TKey, TValue> : BackgroundService
{
	private readonly IServiceProvider serviceProvider;
	private readonly KafkaConnectionOptions options;

	public KafkaBatchConsumerBackgroundService(
		IOptionsMonitor<KafkaConnectionOptions> optionsMonitor,
		KafkaConsumerParameters<TKey, TValue> consumerParameters, 
		IServiceProvider serviceProvider)
	{
		options = optionsMonitor.Get(consumerParameters.KafkaConsumerOptionsName);
		this.serviceProvider = serviceProvider;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		Task[] consumersTasks = new Task[options.ConsumerCount];
		
		for (int i = 0; i < options.ConsumerCount; i++)
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