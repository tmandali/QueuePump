using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public class MessagesFiller
    {
        Func<TableBasedQueue, Task<SqlConnection>> openConnection;
        const int DefaultFillBatchSize = 10000;
        static TimeSpan DefaultFillTaskDelay = TimeSpan.FromMinutes(1);


        public MessagesFiller(Func<TableBasedQueue, Task<SqlConnection>> openConnection, TimeSpan? fillTaskDelay, int? fillBatchSize)
        {
            this.openConnection = openConnection;

            FillTaskDelay = fillTaskDelay ?? DefaultFillTaskDelay;
            FillBatchSize = fillBatchSize ?? DefaultFillBatchSize;
        }

        public TimeSpan FillTaskDelay { get; }
        int FillBatchSize { get; }

        public async Task Fill(TableBasedQueue queue, CancellationToken cancellationToken)
        {
            //Logger.DebugFormat("Starting a new expired message purge task for table {0}.", queue);

            var totalCreatedRowsCount = 0;

            try
            {
                using (var connection = await openConnection(queue).ConfigureAwait(false))
                {
                    var continueCreateing = true;

                    while (continueCreateing && !cancellationToken.IsCancellationRequested)
                    {
                        var createdRowsCount = await queue.FillBatchOfChangeTracingMessages(connection, FillBatchSize).ConfigureAwait(false);

                        totalCreatedRowsCount += createdRowsCount;
                        continueCreateing = createdRowsCount == FillBatchSize;
                    }
                }

                //Logger.DebugFormat("{0} expired messages were successfully purged from table {1}", totalPurgedRowsCount, queue);
            }
            catch
            {
                //Logger.WarnFormat("Purging expired messages from table {0} failed after purging {1} messages.", queue, totalPurgedRowsCount);
                throw;
            }
        }
    }
}
