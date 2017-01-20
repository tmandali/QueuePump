using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueueProcessor.Logging
{
    static class LogLevelReader
    {
        public static LogLevel GetDefaultLogLevel(LogLevel fallback = LogLevel.Info)
        {
            //var logging = ConfigurationManager.GetSection(typeof(Config.Logging).Name) as Config.Logging;
            //if (logging != null)
            //{
            //    var threshold = logging.Threshold;
            //    LogLevel logLevel;
            //    if (!Enum.TryParse(threshold, true, out logLevel))
            //    {
            //        var logLevels = string.Join(", ", Enum.GetNames(typeof(LogLevel)));
            //        var message = $"The value of '{threshold}' is invalid as a loglevel. Must be one of {logLevels}.";
            //        throw new Exception(message);
            //    }
            //    return logLevel;
            //}
            return fallback;
        }
    }
}
