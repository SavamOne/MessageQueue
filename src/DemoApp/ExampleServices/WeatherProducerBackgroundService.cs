using Confluent.Kafka;
using DemoApp.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DemoApp.ExampleServices;

public class WeatherProducerBackgroundService : BackgroundService
{
	private static readonly string[] Cities =
	{
		"Москва",
		"Санкт-Петербург",
		"Сочи",
		"Новосибирск",
		"Тюмень",
		"Томск",
		"Самара"
	};
	
	private readonly KafkaOptions options;

	public WeatherProducerBackgroundService(IOptionsSnapshot<KafkaOptions> snapshot)
	{
		options = snapshot.Get(TopicName);
	}
	
	public static string TopicName => "WeatherTopicOptions";

	protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
	{
		ProducerConfig config = new()
		{
			BootstrapServers = options.BootstrapServers,
		};

		using IProducer<string, int> producer = new ProducerBuilder<string, int>(config).Build();

		while (!stoppingToken.IsCancellationRequested)
		{
			var message = new Message<string, int>
			{
				Key = Cities[Random.Shared.Next(0, Cities.Length)],
				Value = Random.Shared.Next(0, 30)
			};

			await producer.ProduceAsync(options.TopicName, message, stoppingToken);
			await Task.Delay(TimeSpan.FromMilliseconds(600), stoppingToken);
		}
	}, stoppingToken);
}