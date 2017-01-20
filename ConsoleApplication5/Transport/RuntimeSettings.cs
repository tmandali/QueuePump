namespace QueueProcessor.Transport
{
    using System;

    /// <summary>
    /// Controls how the message pump should behave.
    /// </summary>
    public class RuntimeSettings
    {
        /// <summary>
        /// Constructs the settings. NServiceBus will pick a suitable default for `MaxConcurrency`.
        /// </summary>
        public RuntimeSettings()
        {
            MaxConcurrency = Math.Max(2, Environment.ProcessorCount);
        }

        /// <summary>
        /// Constructs the settings.
        /// </summary>
        /// <param name="maxConcurrency">The maximum concurrency to allow.</param>
        public RuntimeSettings(int maxConcurrency)
        {
            Guard.AgainstNegativeAndZero(nameof(maxConcurrency), maxConcurrency);

            MaxConcurrency = maxConcurrency;
        }

        /// <summary>
        /// The maximum number of messages that should be in flight at any given time.
        /// </summary>
        public int MaxConcurrency { get; private set; }

        /// <summary>
        /// Use default settings.
        /// </summary>
        public static RuntimeSettings Default => new RuntimeSettings();
    }
}
