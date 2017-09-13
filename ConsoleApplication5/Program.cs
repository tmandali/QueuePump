namespace QueueProcessor
{
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    class Program
    {
        public static IConfigurationRoot Configuration { get; set; }

        static void Main(string[] args)
        {
            //var dict = new Dictionary<string, string>
            //{
            //    {"Profile:MachineName", "Rick"},
            //    {"App:MainWindow:Height", "11"},
            //    {"App:MainWindow:Width", "11"},
            //    {"App:MainWindow:Top", "11"},
            //    {"App:MainWindow:Left", "11"}
            //};

            //var builder = new ConfigurationBuilder();
            //builder.AddInMemoryCollection(dict);
            //Configuration = builder.Build();
                        
            IServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            AsyncMain(serviceCollection.BuildServiceProvider()).GetAwaiter().GetResult();
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.Configure<MessagePump>(opt => {
             
            });
            services.AddSingleton<MessagePump>();
        }

        async static Task AsyncMain(IServiceProvider serviceProvider)
        {            
            var mp =  serviceProvider.GetService<MessagePump>();

            await mp.Init(
                SqlConnectionFactory.Default("Data Source=localhost,1401;Initial Catalog=TestDB;User Id=sa;Password=tbmX1821;Connection Timeout=10"),
                new TableBasedQueue("dbo", "Test", new[] { "RowVersion" }),
                context => 
                {
                    var rowVersion = context.Message.Body.RowVersion;

                    if (rowVersion == 13170)
                        throw new Exception($"Hata : {rowVersion}");

                    return Task.FromResult(0);
                },
                error => Task.FromResult(ErrorHandleResult.RetryRequired)
                );

            mp.Start();
            Console.ReadLine();
            await mp.Stop();

            //svc.Init().Start(Environment.ProcessorCount, TimeSpan.FromSeconds(10));            
            //Console.ReadLine();            
            //await svc.Stop();
        }
    }
}