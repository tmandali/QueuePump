using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using Dapper;
using System.Xml;

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
                connection.Open();
                var command = new SqlCommand("SELECT [Table], [ConnectionString], [Schema] FROM [Queue]", connection);
                var dataReader = command.ExecuteReader();
                while (dataReader.Read())
                {
                    XmlReader schema = null;
                    if (!dataReader.GetSqlXml(2).IsNull)
                        schema = dataReader.GetSqlXml(2).CreateReader();

                    yield return new TableQueue(dataReader.GetFieldValue<string>(0), dataReader.GetFieldValue<string>(1), schema);
                }
            }
        }        
    }    
}