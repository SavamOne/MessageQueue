using System.Text.Json;
using Confluent.Kafka;

namespace DemoApp.ExampleServices;

public class WeatherValueSerializer : ISerializer<WeatherValue>, IDeserializer<WeatherValue>
{
	public byte[] Serialize(WeatherValue data, SerializationContext context)
	{
		return JsonSerializer.SerializeToUtf8Bytes(data);
	}
		
	public WeatherValue Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
	{
		return JsonSerializer.Deserialize<WeatherValue>(data)!;
	}
}