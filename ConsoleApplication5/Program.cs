using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using System.Xml;
using System.Xml.Schema;
using System.Diagnostics;
using System.IO;

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
                ex => Trace.TraceError(ex.InnerException.Message));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }

        public IEnumerable<IQueue> GetQueueList()
        {
            var local = ConfigurationManager.ConnectionStrings["localhost"].ConnectionString;
            
            using (var connection = new SqlConnection(local))
            {
                connection.Open();
                var command = new SqlCommand("SELECT [Table], [ConnectionString] FROM [Queue]", connection);
                var dataReader = command.ExecuteReader();
                while (dataReader.Read())
                {
                    var tableName = dataReader.GetFieldValue<string>(0);
                    var connectionString = dataReader.GetFieldValue<string>(1);
                    yield return new TableQueue(tableName, connectionString);
                }
            }
        }
    }    
}