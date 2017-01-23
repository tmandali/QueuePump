namespace QueueProcessor
{
    using System;
    using System.Threading.Tasks;

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
                    var rowVersion = context.Message.Body.RowVersion;

                    if (rowVersion == 13170)
                        throw new Exception($"Hata : {rowVersion}");

                    return Task.FromResult(0);
                },
                error =>
                {
                    return Task.FromResult(ErrorHandleResult.RetryRequired);
                },
                () => 
                {
                    return Task.FromResult(true);
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