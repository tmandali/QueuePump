using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;

namespace QueueProcessor
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
            var connection = ConfigurationManager.ConnectionStrings["localhost"].ConnectionString; 

            List<IQueue> queueList = new List<IQueue>() {
                 new TableQueue(connection,"Test"),
            };

            var prs = new Processor();
            prs.Init(queueList, 2, TimeSpan.FromSeconds(5), ex => Console.WriteLine(ex));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }
    }    
}