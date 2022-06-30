namespace DemoApp.Options;

public class KafkaOptions
{
	public string TopicName { get; set; }
	
	public string BootstrapServers { get; set; }
	
	public string ConsumerGroupId { get; set; }
}