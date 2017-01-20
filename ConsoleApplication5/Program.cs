using System;
using System.Threading.Tasks;

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
                context => { return Task.FromResult(0); },
                "dbo.Test", 
                "Data Source=.;Initial Catalog=nservicebus;Integrated Security=True;Connection Timeout=10; Pooling = true; Min Pool Size = 20; Max Pool Size = 200;");

            mp.Start();
            Console.ReadLine();
            await mp.Stop();

            //svc.Init().Start(Environment.ProcessorCount, TimeSpan.FromSeconds(10));            
            //Console.ReadLine();            
            //await svc.Stop();
        }
    }
}