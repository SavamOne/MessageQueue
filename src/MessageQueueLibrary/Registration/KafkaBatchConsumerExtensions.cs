using MessageQueueLibrary.Contracts;
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

		if (builder.BatchExecutorFactory is null)
		{
			throw new ArgumentNullException(nameof(builder.BatchExecutorFactory));
		}
		
		// HACK: Коммитом занимается самостоятельно KafkaBatchConsumer.
		// По-хорошему для ConsumerConfig нужно сделать отдельную read-only обертку, где не будет этой настройки
		if (builder.ConsumerConfig.EnableAutoCommit == true)
		{
			throw new ArgumentException("ConsumerConfig.EnableAutoCommit = true is not allowed.");
		}
		builder.ConsumerConfig.EnableAutoCommit = false;
		
		KafkaBatchConsumerParameters<TKey, TValue> parameters = new(builder.TopicName,
			builder.ConsumerCount,
			builder.BatchSize,
			builder.ConsumerConfig,
			builder.BatchWaitTimeout,
			builder.KeySerializer,
			builder.ValueDeserializer);

		serviceCollection.AddSingleton(parameters);
		serviceCollection.AddScoped<KafkaBatchConsumer<TKey, TValue>, KafkaBatchConsumer<TKey, TValue>>();
		serviceCollection.AddScoped(sp => builder.BatchExecutorFactory(sp));
		serviceCollection.AddHostedService<KafkaBatchConsumerBackgroundService<TKey, TValue>>();
		
		return serviceCollection;
	}
	
	public static IServiceCollection AddUniqueKafkaBatchConsumers<TKey, TValue>(this IServiceCollection serviceCollection, Action<KafkaBatchConsumerBuilder<TKey, TValue>> action)
		where TValue : IUniqueValue
	{
		serviceCollection.AddKafkaBatchConsumers(action);
		
		serviceCollection.AddScoped<IMessageStatusProcessor, RedisMessageProcessor>();
		serviceCollection.AddScoped<KafkaBatchConsumer<TKey, TValue>, KafkaBatchUniqueConsumer<TKey, TValue>>();
		
		return serviceCollection;
	}
}