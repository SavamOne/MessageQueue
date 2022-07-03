namespace MessageQueueLibrary.Options;

public class MessageStatusOptions
{
	public TimeSpan InProcessTimeout { get; set; }
	
	public TimeSpan CompletionTimeout { get; set; }
}