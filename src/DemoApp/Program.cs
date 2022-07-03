using System.Text.Json;
using Confluent.Kafka;
using DemoApp.BatchExecutors;
using DemoApp.ExampleServices;
using DemoApp.Options;
using MessageQueueLibrary.Registration;
using Microsoft.Extensions.Configuration;
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
			
			IConfigurationSection section = hostContext.Configuration.GetSection(WeatherProducerBackgroundService.TopicName);
			serviceCollection.AddOptions<KafkaOptions>(WeatherProducerBackgroundService.TopicName).Bind(section);
			serviceCollection.AddHostedService<WeatherProducerBackgroundService>();
			
			serviceCollection.AddSingleton<BatchLogger<string, WeatherValue>>();
			serviceCollection.AddUniqueKafkaBatchConsumers<string, WeatherValue>(builder =>
			{
				KafkaOptions weatherTopicOptions = section.Get<KafkaOptions>();
				
				builder.BatchSize = 5;
				builder.BatchWaitTimeout = TimeSpan.FromSeconds(10);
				builder.BatchExecutorFactory = provider => provider.GetRequiredService<BatchLogger<string, WeatherValue>>();
				builder.TopicName = weatherTopicOptions.TopicName;
				builder.ConsumerCount = 3;
				builder.ConsumerConfig = new ConsumerConfig
				{
					BootstrapServers = weatherTopicOptions.BootstrapServers,
					GroupId = weatherTopicOptions.ConsumerGroupId,
					AutoOffsetReset = AutoOffsetReset.Earliest,
				};
				builder.ValueDeserializer = new WeatherValueSerializer();
			});
		});

		IHost host = hostBuilder.Build();

		await host.RunAsync();
	}
}