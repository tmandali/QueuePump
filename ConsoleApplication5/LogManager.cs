namespace QueueProcessor
{
    using Microsoft.Extensions.Logging;
    static class LogManager
    {
        static ILoggerFactory LoggerFactory { get; } = new LoggerFactory();
        public static ILogger GetLogger<T>() => LoggerFactory.AddConsole().CreateLogger<T>();
    }
}