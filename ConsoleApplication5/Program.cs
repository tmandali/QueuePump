using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
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

            List<ITableQueue> tableQ = new List<ITableQueue>() {
                 new TableQueue<Tb1>(connection,"Tb1"),
                 new TableQueue<Tb1>(connection,"Tb1"),
                 new TableQueue<Tb1>(connection,"Tb1"),
                 new TableQueue<Tb1>(connection,"Tb1"),
                 new TableQueue<Tb1>(connection,"Tb1"),
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
                        if (x.IsFaulted) ex(x.Exception);
                    }).GetAwaiter().GetResult());

                await Task.Delay(retryLoop, cts.Token);
            }
        }
    }

    class TableQueue<T> : ITableQueue where T : class, new()
    {
        private string tableName;
        private SqlConnectionFactory connectionFactory;
        private Dictionary<string, string> endPointList = new Dictionary<string, string>();

       
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
            var row = new
            {
                EndPoint = new UriBuilder("mssql", "localhost.sqlexpress", 1433, "testdb.dbo.sp_ImportXML").Uri,
                ReplyToAdress = "dbo.sp_ExportXML",
                Body = "<prms><prm1>1<prm1><prms>",
            };

            object xml = DBNull.Value;

            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                var command = new SqlCommand(row.ReplyToAdress, connection);
                command.CommandType = CommandType.Text;

                ((ICollection<KeyValuePair<string, object>>)message)
                    .Where(w => w.Key != "EndPoint" && w.Key != "ReplyToAdress" && w.Key != "Body")
                    .ToList()
                    .ForEach(f => command.Parameters.AddWithValue('@' + f.Key, f.Value));

                //xml = await command.ExecuteScalarAsync().ConfigureAwait(false);
            }

            var cnnString = GetEndPointConnection(row.EndPoint);

            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted))
            {
                var command = new SqlCommand(row.EndPoint.Segments[1], connection, transaction);
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@Xml", xml);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                transaction.Commit();
            }

            return true;
        }
        
        string GetEndPointConnection(Uri endpoint)
        {
            string result;

            if (endPointList.TryGetValue(endpoint.ToString(), out result))
                return result;


            var cnnStr = new SqlConnectionStringBuilder();
            cnnStr.DataSource = endpoint.Authority.Replace('.','\\');
            cnnStr.IntegratedSecurity = true;
            endPointList.Add(endpoint.ToString(), cnnStr.ConnectionString);

            return cnnStr.ConnectionString;
        }

        async Task<dynamic> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
        {            
            var commandText = $"select top 1 * from dbo.{tableName}"; //Format(Sql.ReceiveText, schemaName, tableName);

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

                return Read(dataReader);
            }
        }

        static dynamic Read(SqlDataReader dr) 
        {            
            var eo = new ExpandoObject();
            var members = (ICollection<KeyValuePair<string, object>>) eo;

            for (int i = 0; i < dr.FieldCount; i++)
            {
                var member = new KeyValuePair<string, object>(dr.GetName(i), dr.GetValue(i));
                members.Add(member);
            }

            return eo;
        }
    }

    internal interface ITableQueue
    {
        Task Receive(CancellationToken ct);
    }



}