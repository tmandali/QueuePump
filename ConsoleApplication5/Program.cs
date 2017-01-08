using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

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

            List<ITableQueue> tableQ = new List<ITableQueue>() {
                 new TableQueue<Tb1>(connection,"Test"),
                 //new TableQueue<Tb1>(connection,"Tb1"),
                 //new TableQueue<Tb1>(connection,"Tb1"),
                 //new TableQueue<Tb1>(connection,"Tb1"),
                 //new TableQueue<Tb1>(connection,"Tb1"),
            };

            var prs = new Processor();
            prs.Init(tableQ, 2, TimeSpan.Zero, ex => Console.WriteLine(ex));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }
    }

    class Tb1
    {
        public int RecId { get; set; }
        public int Tb1Key { get; set; }
    }

    class Processor
    {
        Task iterator;
        CancellationTokenSource cts;
        IEnumerable<ITableQueue> tableQ;
        int maxConcurrency = 0;
        TimeSpan retryLoop;
        Action<Exception> ex;

        public void Init(IEnumerable<ITableQueue> tableQ, int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
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
                        if (x.IsFaulted)
                            ex(x.Exception);
                    }).GetAwaiter().GetResult());

                await Task.Delay(retryLoop, cts.Token);
            }
        }
    }

    class TableQueue<T> : ITableQueue where T : class, new()
    {
        private string tableName;
        private SqlConnectionFactory connectionFactory;        
       
        public TableQueue(string connection, string tableName)
        {
            this.tableName = tableName;
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

        async Task<bool> TryProcess(dynamic message)
        {
            XmlReader xmlReader = null;

            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
            {
                var command = new SqlCommand(message.ReplyToAdress, connection, transaction);
                command.CommandType = CommandType.StoredProcedure;

                ((ICollection<KeyValuePair<string, object>>)message)
                    .Where(w => w.Key != "EndPoint" && w.Key != "ReplyToAdress")
                    .ToList()
                    .ForEach(f => command.Parameters.AddWithValue('@' + f.Key, f.Value));

                xmlReader = await command.ExecuteXmlReaderAsync().ConfigureAwait(false);
                transaction.Commit();
            }

            return await EndPointProcess(new Uri(message.EndPoint), xmlReader).ConfigureAwait(false);
        }
        
        async Task<bool> EndPointProcess(Uri endPoint, XmlReader xmlReader)
        {
            var cnnString = System.Configuration.ConfigurationManager.ConnectionStrings[endPoint.Host].ConnectionString;
            using (var epConnection = await SqlConnectionFactory.Default(cnnString).OpenNewConnection().ConfigureAwait(false))
            using (var epTransaction = epConnection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
            {
                var epCommand = new SqlCommand(endPoint.Segments[1], epConnection, epTransaction);
                epCommand.CommandType = CommandType.StoredProcedure;
                epCommand.Parameters.AddWithValue("@EndPoint", endPoint.ToString());
                epCommand.Parameters.AddWithValue("@Xml", new SqlXml(xmlReader));
                await epCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                epTransaction.Commit();
            }
            return true;
        }

        async Task<dynamic> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
        {
            string receiveText = $@"
            DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
            IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
            SET NOCOUNT ON;

            --OUTPUT deleted.Id, deleted.CorrelationId, deleted.ReplyToAddress, deleted.Recoverable, deleted.Headers, deleted.Body;

            WITH message AS (SELECT TOP(1) * FROM dbo.{tableName} WITH (UPDLOCK, READPAST, ROWLOCK)) -- WHERE [Expires] IS NULL OR [Expires] > GETUTCDATE() ORDER BY [RowVersion]
            DELETE FROM message
            OUTPUT deleted.*;
            IF(@NOCOUNT = 'ON') SET NOCOUNT ON;
            IF(@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

            //var commandText = $"select top 1 * from dbo."; //Format(Sql.ReceiveText, schemaName, tableName);

            using (var command = new SqlCommand(receiveText, connection, transaction))
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

                return await Read(dataReader).ConfigureAwait(false);
            }
        }

        static async Task<dynamic> Read(SqlDataReader dr) 
        {            
            var eo = new ExpandoObject();
            var members = (ICollection<KeyValuePair<string, object>>) eo;

            for (int i = 0; i < dr.FieldCount; i++)
            {
                string name = dr.GetName(i);
                object value = await GetNullableAsync<object>(dr, i);
                members.Add(new KeyValuePair<string, object>(name, value));
            }

            return eo;
        }

        static async Task<T> GetNullableAsync<T>(SqlDataReader dataReader, int index) where T : class
        {
            if (await dataReader.IsDBNullAsync(index).ConfigureAwait(false))
            {
                return default(T);
            }

            return await dataReader.GetFieldValueAsync<T>(index).ConfigureAwait(false);
        }
    }

    internal interface ITableQueue
    {
        Task Receive(CancellationToken ct);
    }
}