using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace QueueProcessor
{
    class Program
    {       
        static void Main(string[] args)
        {
            var svc = new Service();
            AsyncMain(svc).GetAwaiter().GetResult();
        }

        async static Task AsyncMain(Service svc)
        {
            var mp = new MessagePump();

            await mp.Init(
                context => 
                {
                    //var propertyBag = (ICollection<KeyValuePair<string, object>>) context.Body;
                    //dynamic obj = context.Message;

                    //if (obj.RowVersion == 9178)
                    //    throw new Exception($"Hata : {obj.RowVersion}");                    

                    return Task.FromResult(0);
                },
                error =>
                {
                    return Task.FromResult(ErrorHandleResult.RetryRequired);
                },
                new TableBaseQueue("dbo","Test", new[] {"RowVersion"}), 
                "Data Source=.;Initial Catalog=nservicebus;Integrated Security=True;Connection Timeout=10;");

            mp.Start();
            Console.ReadLine();
            await mp.Stop();

            //svc.Init().Start(Environment.ProcessorCount, TimeSpan.FromSeconds(10));            
            //Console.ReadLine();            
            //await svc.Stop();
        }
    }
}