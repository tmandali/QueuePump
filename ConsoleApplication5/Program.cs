using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using Dapper;

namespace QueueProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            var prg = new Program();
            AsyncMain(prg).GetAwaiter().GetResult();
            Console.ReadLine();
        }

        async static Task AsyncMain(Program prg)
        {
            var prs = new Processor();
            prs.Init(prg.GetQueueList, 3, 
                TimeSpan.FromSeconds(5), 
                ex => System.Diagnostics.Trace.TraceError(ex.InnerException.Message));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }

        public IEnumerable<IQueue> GetQueueList()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["localhost"].ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                return connection.Query("select QueueName, ConnectionString from QueuePump")
                    .Select(s => new TableQueue(s.ConnectionString, s.QueueName));                
            }
        }
    }    
}