using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Dynamic;
using System.Linq;
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
                 new TableQueue(connection,"Test"),
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

    public abstract class EndPoint
    {
        public abstract Task<bool> Send(string from, XmlReader reader);
        protected abstract void Init(Uri adress);
        public static Task<EndPoint> Factory(Envelope envelope)
        {
            EndPoint result;

            switch (envelope.EndPoint.Scheme)
            {
                case "mssql":
                    result = new SqlEndPoint();
                    break;
                default:
                    throw new Exception($"{envelope.EndPoint.Scheme} desteklenmityor !");
            }

            result.Init(envelope.EndPoint);
            return Task.FromResult(result);
        }
    }

    public class SqlEndPoint : EndPoint
    {
        private SqlConnectionFactory sqlConnectionFactory;
        private string procedureName;
        private string endPoint;

        protected override void Init(Uri adress)
        {
            endPoint = adress.ToString();
            var connection = System.Configuration.ConfigurationManager.ConnectionStrings[adress.Host].ConnectionString;
            sqlConnectionFactory = SqlConnectionFactory.Default(connection);
            procedureName = adress.Segments[1];
        }

        public override async Task<bool> Send(string from, XmlReader reader)
        {
            using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var commmand = new SqlCommand(procedureName, connection, transaction);
                commmand.CommandType = CommandType.StoredProcedure;
                commmand.Parameters.AddWithValue("@From", from);
                commmand.Parameters.AddWithValue("@Xml", new SqlXml(reader));
                await commmand.ExecuteNonQueryAsync();
                transaction.Commit();
                return true;
            }
        }
    }

    public class Envelope
    {
        public Uri EndPoint { get; set; }

        public string ReplyTo { get; set; }

        public dynamic Headers { get; set; }


        public void PrepareExportCommand(SqlCommand command)
        {
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.AddWithValue("@To", EndPoint.ToString());

            var headers = (ICollection<KeyValuePair<string, object>>) Headers;
            foreach (var prm in headers)
            {
                command.Parameters.AddWithValue('@'+prm.Key, prm.Value ?? DBNull.Value);
            }
        }

        static T TryGetHeaderValue<T>(Dictionary<string, string> headers, string name, Func<string, T> conversion)
        {
            string text;
            if (!headers.TryGetValue(name, out text))
            {
                return default(T);
            }
            var value = conversion(text);
            return value;
        }


        public static async Task<Envelope> Read(SqlDataReader dataReader)
        {
            var result = await ReadRow(dataReader);
            return result.TryParse();
        }

        Envelope TryParse()
        {
            try
            {
                if (EndPoint.Scheme != "mssql")
                    throw new Exception("Desteklenmeyen endpoint !");
                return this;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        static async Task<Envelope> ReadRow(SqlDataReader dataReader)
        {
            var envelope = new Envelope();
            envelope.Headers = new ExpandoObject();

            var headers = (ICollection<KeyValuePair<string, object>>)envelope.Headers;
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                string name = dataReader.GetName(i);

                if (name == "EndPoint")
                    envelope.EndPoint = new Uri(await dataReader.GetFieldValueAsync<string>(i).ConfigureAwait(false));
                else if (name == "ReplyTo")
                    envelope.ReplyTo = await dataReader.GetFieldValueAsync<string>(i).ConfigureAwait(false);
                else
                    headers.Add(new KeyValuePair<string, object>(name, await GetNullableAsync<object>(dataReader, i)));
            }

            envelope.Headers = headers;
            return envelope;
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

    class TableQueue : ITableQueue 
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
                Envelope envelope = null;

                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    envelope = await TryReceive(connection, transaction, ct).ConfigureAwait(false);

                    if (envelope == null)
                    {
                        transaction.Commit();
                        return;
                    }

                    if (await TryProcess(envelope).ConfigureAwait(false))
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

        async Task<bool> TryProcess(Envelope envelope)
        {
            bool result = false;

            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var command = new SqlCommand(envelope.ReplyTo, connection, transaction);
                envelope.PrepareExportCommand(command);                                    
                var xmlReader = await command.ExecuteXmlReaderAsync().ConfigureAwait(false);

                var endPoint = await EndPoint.Factory(envelope);
                result = await endPoint.Send(tableName, xmlReader);

                transaction.Commit();
            }

            return result;
        }
        
        async Task<Envelope> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
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

            using (var command = new SqlCommand(receiveText, connection, transaction))
            {
                return await ReadMessage(command).ConfigureAwait(false);
            }
        }

        async Task<Envelope> ReadMessage(SqlCommand command)
        {
            using (var dataReader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    return null;
                }

                return await Envelope.Read(dataReader).ConfigureAwait(false);
            }
        }
    }

    internal interface ITableQueue
    {
        Task Receive(CancellationToken ct);
    }
}