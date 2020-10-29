namespace DurableTask.Netherite.AzureFunctions.Tests.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    public class LogEntry
    {
        public LogEntry(
            string category,
            LogLevel level,
            EventId eventId,
            Exception exception,
            string message)
        {
            this.Category = category;
            this.LogLevel = level;
            this.EventId = eventId;
            this.Exception = exception;
            this.Message = message;
            this.Timestamp = DateTime.Now;
        }

        public string Category { get; }

        public DateTime Timestamp { get; }

        public EventId EventId { get; }

        public LogLevel LogLevel { get; }

        public Exception Exception { get; }

        public string Message { get; }

        public override string ToString()
        {
            string output = $"{this.Timestamp:o} [{this.Category}] {this.Message}";
            if (this.Exception != null)
            {
                output += Environment.NewLine + this.Exception.ToString();
            }

            return output;
        }
    }
}
