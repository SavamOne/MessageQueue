using MessageQueueLibrary.Options;
using MessageQueueLibrary.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MessageQueueLibrary.Registration;

public static class KafkaBatchConsumerExtensions
{
	public static IServiceCollection AddKafkaBatchConsumers<TKey, TValue>(this IServiceCollection serviceCollection, Action<KafkaBatchConsumerBuilder<TKey, TValue>> action)
	{
		var builder = new KafkaBatchConsumerBuilder<TKey, TValue>();

		action(builder);
		
		KafkaBatchConsumerOptions<TKey, TValue> options = new(builder.TopicName,
			builder.ConsumerCount,
			builder.BatchSize,
			builder.BatchExecutorFactory,
			builder.ConsumerConfig,
			builder.BatchWaitTimeout);
		
		// HACK: Коммитом занимается самостоятельно KafkaBatchConsumer.
		// По-хорошему для ConsumerConfig нужно сделать отдельную read-only обертку, где не будет этой настройки
		if (options.ConsumerConfig.EnableAutoCommit == true)
		{
			throw new ArgumentException("ConsumerConfig.EnableAutoCommit = true is not allowed.");
		}
		options.ConsumerConfig.EnableAutoCommit = false;

		serviceCollection.AddHostedService(serviceProvider =>
		{
			var logger = serviceProvider.GetRequiredService<ILogger<KafkaBatchConsumerBackgroundService<TKey, TValue>>>();
			return new KafkaBatchConsumerBackgroundService<TKey, TValue>(options, serviceProvider, logger);
		});

		return serviceCollection;
	}
}