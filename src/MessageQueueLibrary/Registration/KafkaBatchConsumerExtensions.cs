using MessageQueueLibrary.Contracts;
using MessageQueueLibrary.Options;
using MessageQueueLibrary.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessageQueueLibrary.Registration;

public static class KafkaBatchConsumerExtensions
{
	public static IServiceCollection AddKafkaBatchConsumers<TKey, TValue>(this IServiceCollection serviceCollection, 
		IConfiguration configuration,
		Action<KafkaBatchConsumerBuilder<TKey, TValue>> action)
	{
		_ = RegisterCommonDependencies(serviceCollection, configuration, action);
		
		serviceCollection.AddScoped<KafkaBatchConsumer<TKey, TValue>>();
		
		return serviceCollection;
	}
	
	public static IServiceCollection AddUniqueKafkaBatchConsumers<TKey, TValue>(this IServiceCollection serviceCollection, 
		IConfiguration configuration, 
		Action<KafkaBatchConsumerBuilder<TKey, TValue>> action)
		where TValue : IUniqueValue
	{
		var builder = RegisterCommonDependencies(serviceCollection, configuration, action);

		serviceCollection.AddOptions<RedisConnectionOptions>(builder.RedisOptionsName)
		   .Bind(configuration.GetSection(builder.RedisOptionsName))
		   .ValidateDataAnnotations();

		serviceCollection.AddOptions<MessageStatusOptions>(builder.MessageStatusOptionsName)
		   .Bind(configuration.GetSection(builder.MessageStatusOptionsName))
		   .Validate(options => options.CompletionTimeout > TimeSpan.Zero && options.InProcessTimeout > TimeSpan.Zero);
		
		serviceCollection.AddSingleton(_ => new MessageStatusParameters(builder.RedisOptionsName!, 
			builder.MessageStatusOptionsName!));
		
		serviceCollection.AddScoped<IMessageStatusProcessor, RedisMessageProcessor>();
		serviceCollection.AddScoped<KafkaBatchConsumer<TKey, TValue>, KafkaBatchUniqueConsumer<TKey, TValue>>();
		
		return serviceCollection;
	}

	private static KafkaBatchConsumerBuilder<TKey, TValue> RegisterCommonDependencies<TKey, TValue>(IServiceCollection serviceCollection, IConfiguration configuration, Action<KafkaBatchConsumerBuilder<TKey, TValue>> action)
	{
		var builder = new KafkaBatchConsumerBuilder<TKey, TValue>();

		action(builder);

		if (builder.BatchExecutorFactory is null)
		{
			throw new ArgumentNullException(nameof(builder.BatchExecutorFactory));
		}

		if (string.IsNullOrEmpty(builder.KafkaConsumerOptionsName))
		{
			throw new ArgumentNullException(nameof(builder.KafkaConsumerOptionsName));
		}
		
		serviceCollection.AddOptions<KafkaConnectionOptions>(builder.KafkaConsumerOptionsName)
		   .Bind(configuration.GetSection(builder.KafkaConsumerOptionsName))
		   .ValidateDataAnnotations()
		   .Validate(options => options.BatchWaitTimeout > TimeSpan.Zero);

		serviceCollection.AddSingleton(_ => new KafkaConsumerParameters<TKey, TValue>(builder.KeySerializer, 
			builder.ValueDeserializer, 
			builder.KafkaConsumerOptionsName!));
		
		serviceCollection.AddScoped(sp => builder.BatchExecutorFactory(sp));
		serviceCollection.AddHostedService<KafkaBatchConsumerBackgroundService<TKey, TValue>>();

		return builder;
	}
}