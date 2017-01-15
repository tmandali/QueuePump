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
using System.Threading;
using System.Collections.Concurrent;

namespace QueueProcessor
{
    class Program
    {
        IList<ConnectionStringSettings> hostlist;
        CancellationTokenSource cancellationTokenSource;
        Task[] tasks;

        static void Main(string[] args)
        {
            var prg = new Program();
            AsyncMain(prg).GetAwaiter().GetResult();
        }

        async static Task AsyncMain(Program prg)
        {
            prg.Init().Start(4, TimeSpan.FromSeconds(10));            
            Console.ReadLine();            
            await prg.Stop();
        }

        Program Init()
        {
            cancellationTokenSource = new CancellationTokenSource();
            hostlist = ReadHost(new List<ConnectionStringSettings>());
            return this;
        }
   
        void Start(int maxConcurrency, TimeSpan retry)
        {
            tasks = (
                from host in hostlist
                select new HostConnect(host).Starter(maxConcurrency, retry, cancellationTokenSource.Token)
            ).ToArray();
                        
            Trace.TraceInformation("Service started");
        }

        async Task Stop()
        {
            cancellationTokenSource.Cancel();
            var timeout = Task.Delay(TimeSpan.FromSeconds(30));
            var finaly = await Task.WhenAny(timeout, Task.WhenAll(tasks)).ConfigureAwait(false);

            if (finaly == timeout)
                Trace.TraceWarning("Service process timeout");

            cancellationTokenSource.Dispose();
        }

        IList<ConnectionStringSettings> ReadHost(IList<ConnectionStringSettings> list)
        {
            foreach (ConnectionStringSettings connectionSetting in ConfigurationManager.ConnectionStrings)
            {
                if (connectionSetting.Name.Split('.')[0] != "QueueHost")
                    continue;
                list.Add(connectionSetting);
            }
            return list;
        }
    }
}