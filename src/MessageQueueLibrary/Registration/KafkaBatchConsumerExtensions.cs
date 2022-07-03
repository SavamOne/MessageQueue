using MessageQueueLibrary.Options;
using MessageQueueLibrary.Services;
using Microsoft.Extensions.DependencyInjection;

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
			builder.ConsumerConfig,
			builder.BatchWaitTimeout);

		// HACK: Коммитом занимается самостоятельно KafkaBatchConsumer.
		// По-хорошему для ConsumerConfig нужно сделать отдельную read-only обертку, где не будет этой настройки
		if (options.ConsumerConfig.EnableAutoCommit == true)
		{
			throw new ArgumentException("ConsumerConfig.EnableAutoCommit = true is not allowed.");
		}
		options.ConsumerConfig.EnableAutoCommit = false;

		serviceCollection.AddSingleton(options);
		serviceCollection.AddScoped<KafkaBatchConsumer<TKey, TValue>, KafkaBatchConsumer<TKey, TValue>>();
		serviceCollection.AddScoped(sp => builder.BatchExecutorFactory(sp));
		serviceCollection.AddHostedService<KafkaBatchConsumerBackgroundService<TKey, TValue>>();

		// serviceCollection.AddSingleton<KafkaConsumer<TKey, TValue>, KafkaUniqueConsumer<TKey, TValue>>();

		return serviceCollection;
	}
}