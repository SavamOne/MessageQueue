using System.ComponentModel.DataAnnotations;

namespace MessageQueueLibrary.Options;

public class KafkaConnectionOptions
{
	[Required]
	public string TopicName { get; set; } = null!;

	[Required]
	public string BootstrapServers { get; set; } = null!;

	[Required]
	public string ConsumerGroupId { get; set; } = null!;

	[Range(0, int.MaxValue)]
	public int ConsumerCount { get; set; } = 1;

	[Range(0, int.MaxValue)]
	public int BatchSize { get; set; } = 5;
	
	public TimeSpan BatchWaitTimeout { get; set; } = TimeSpan.FromSeconds(10);
}