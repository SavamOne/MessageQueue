using MessageQueueLibrary.Contracts;

namespace DemoApp.ExampleServices;

public record WeatherValue : IUniqueValue
{ 
	public Guid Id { get; init; } = Guid.NewGuid();
	
	public int Value { get; init; }
}