using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication5
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncMain().GetAwaiter().GetResult();
            Console.WriteLine("Tamamlandı ...");
            Console.ReadLine();
        }

        async static Task AsyncMain()
        {
            var connection = @"Data Source=.\SQLEXPRESS;Initial Catalog=TestDb;Integrated Security=True";

            List<TableQueue> tableQ = new List<TableQueue>() {
                 new TableQueue(connection,"Tb1"),
                 new TableQueue(connection,"table 2"),
                 new TableQueue(connection,"table 3"),
                 new TableQueue(connection,"table 4"),
                 new TableQueue(connection,"table 5"),
            };

            var prs = new Processor();
            prs.Init(tableQ, 2, TimeSpan.Zero, ex => Console.WriteLine(ex));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }
    }

    class Processor
    {
        Task iterator;
        CancellationTokenSource cts;
        IEnumerable<TableQueue> tableQ;
        int maxConcurrency = 0;
        TimeSpan retryLoop;
        Action<Exception> ex;

        public void Init(IEnumerable<TableQueue> tableQ, int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
        {
            this.maxConcurrency = maxConcurrency;
            this.tableQ = tableQ.TakeWhile(x => !cts.IsCancellationRequested);
            this.retryLoop = retryLoop;
            this.ex = ex;
        }

        public void Start()
        {
            cts = new CancellationTokenSource();
            iterator = Task.Run(Receive, CancellationToken.None);
        }

        public async Task Stop()
        {
            cts.Cancel();

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var finishedTask = await Task.WhenAny(iterator, timeoutTask);

            if (finishedTask == timeoutTask)
                //throw new OperationCanceledException(cts.Token);

            cts.Dispose();
        }

        async Task Receive()
        {
            while (!cts.IsCancellationRequested)
            {
                tableQ
                    .AsParallel()
                    .WithCancellation(cts.Token)
                    .WithDegreeOfParallelism(maxConcurrency)
                    .ForAll(q => q.Receive(cts.Token).ContinueWith(x => {
                        if (x.IsFaulted) ex(x.Exception);
                    }).GetAwaiter().GetResult());

                await Task.Delay(retryLoop, cts.Token);
            }
        }
    }

    class TableQueue
    {
        public string TableName { get; }

        private SqlConnectionFactory connectionFactory;

        public TableQueue(string connection, string tableName)
        {
            this.TableName = tableName;
            this.connectionFactory = SqlConnectionFactory.Default(connection);
        }

        public async Task Receive(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                dynamic message = null;

                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
                {
                    message = await TryReceive(connection, transaction, ct).ConfigureAwait(false);

                    if (message == null)
                    {
                        transaction.Commit();
                        return;
                    }

                    if (await TryProcess(message).ConfigureAwait(false))
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        transaction.Rollback();
                    }
                }
            }
        }

        private Task<bool> TryProcess(dynamic message)
        {
            return Task.FromResult<bool>(true);
        }

        async Task<dynamic> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
        {
            var commandText = ""; //Format(Sql.ReceiveText, schemaName, tableName);

            using (var command = new SqlCommand(commandText, connection, transaction))
            {
                return await ReadMessage(command).ConfigureAwait(false);
            }
        }

        async Task<dynamic> ReadMessage(SqlCommand command)
        {
            using (var dataReader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    return null;
                }

                var readResult = await Read(dataReader).ConfigureAwait(false);

                return readResult;
            }
        }

        async Task<dynamic> Read(IDataReader dr)
        {
            return null;
        }
    }
}