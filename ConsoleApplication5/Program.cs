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
            svc.Init().Start(Environment.ProcessorCount, TimeSpan.FromSeconds(10));            
            Console.ReadLine();            
            await svc.Stop();
        }
    }
}