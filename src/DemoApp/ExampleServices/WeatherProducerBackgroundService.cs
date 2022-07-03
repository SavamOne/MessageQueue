using Confluent.Kafka;
using MessageQueueLibrary.Options;
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
	
	private readonly KafkaConnectionOptions options;

	public WeatherProducerBackgroundService(IOptionsSnapshot<KafkaConnectionOptions> snapshot)
	{
		options = snapshot.Get(OptionsName);
	}
	
	public static string OptionsName => "WeatherKafkaOptions";

	protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
	{
		ProducerConfig config = new()
		{
			BootstrapServers = options.BootstrapServers,
		};
		
		using IProducer<string, WeatherValue> producer = new ProducerBuilder<string, WeatherValue>(config).SetValueSerializer(new WeatherValueSerializer()).Build();

		while (!stoppingToken.IsCancellationRequested)
		{
			var message = new Message<string, WeatherValue>
			{
				Key = Cities[Random.Shared.Next(0, Cities.Length)],
				Value = new WeatherValue
				{
					Id = Random.Shared.Next(0, 10) != 5 ? Guid.NewGuid() : Guid.Empty,
					Value = Random.Shared.Next(0, 30)
				}
			};

			await producer.ProduceAsync(options.TopicName, message, stoppingToken);
			await Task.Delay(TimeSpan.FromMilliseconds(600), stoppingToken);
		}
	}, stoppingToken);
}