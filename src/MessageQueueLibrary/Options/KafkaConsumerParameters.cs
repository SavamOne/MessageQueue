using Confluent.Kafka;

namespace MessageQueueLibrary.Options;

public class KafkaConsumerParameters<TKey, TValue>
{
	public KafkaConsumerParameters(
		IDeserializer<TKey>? keyDeserializer,
		IDeserializer<TValue>? valueDeserializer,
		string kafkaConsumerOptionsName)
	{
		ValueDeserializer = valueDeserializer;
		KafkaConsumerOptionsName = kafkaConsumerOptionsName;
		KeyDeserializer = keyDeserializer;
	}

	public IDeserializer<TKey>? KeyDeserializer { get; }
	
	public IDeserializer<TValue>? ValueDeserializer { get; }
	
	public string KafkaConsumerOptionsName { get; }
}