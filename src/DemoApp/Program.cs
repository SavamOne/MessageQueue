using DemoApp.BatchExecutors;
using DemoApp.ExampleServices;
using MessageQueueLibrary.Registration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DemoApp;

public static class Program
{
	public static async Task Main()
	{
		IHostBuilder hostBuilder = Host.CreateDefaultBuilder();
		
		hostBuilder.ConfigureServices((hostContext, serviceCollection) =>
		{
			serviceCollection.AddLogging();
			serviceCollection.AddHostedService<WeatherProducerBackgroundService>();
			serviceCollection.AddSingleton<BatchLogger<string, WeatherValue>>();
			serviceCollection.AddUniqueKafkaBatchConsumers<string, WeatherValue>(hostContext.Configuration, builder =>
			{
				builder.BatchExecutorFactory = provider => provider.GetRequiredService<BatchLogger<string, WeatherValue>>();
				builder.ValueDeserializer = new WeatherValueSerializer();
				
				builder.KafkaConsumerOptionsName = WeatherProducerBackgroundService.OptionsName;
				builder.RedisOptionsName = "WeatherRedisConnectionOptions";
				builder.MessageStatusOptionsName = "WeatherStatusOptions";
			});
		});

		IHost host = hostBuilder.Build();

		await host.RunAsync();
	}
}