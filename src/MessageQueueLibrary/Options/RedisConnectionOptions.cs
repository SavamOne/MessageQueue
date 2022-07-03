using System.ComponentModel.DataAnnotations;

namespace MessageQueueLibrary.Options;

public class RedisConnectionOptions
{
	[Required]
	public string Configuration { get; set; } = null!;
}