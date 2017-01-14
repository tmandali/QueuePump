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
        Task startTask;

        static void Main(string[] args)
        {
            var prg = new Program();
            AsyncMain(prg).GetAwaiter().GetResult();
        }

        async static Task AsyncMain(Program prg)
        {
            prg.Init().Start();            
            Console.ReadLine();            
            await prg.Stop();

            //var prs = new Processor();
            //prs.Init(3,
            //    TimeSpan.FromSeconds(10),
            //    ex => Trace.TraceError(ex.InnerException.Message));
            //prs.Start();
            //Console.ReadLine();            
        }

        Program Init()
        {
            cancellationTokenSource = new CancellationTokenSource();
            hostlist = ReadHost(new List<ConnectionStringSettings>());
            return this;
        }
   
        void Start()
        {
            tasks = (
                from host in hostlist
                select new HostConnect(host).Starter(TimeSpan.FromSeconds(10), cancellationTokenSource.Token)
            ).ToArray();
                        
            Trace.TraceInformation("Service started");
        }

        async Task Stop()
        {
            cancellationTokenSource.Cancel();
            await Task.WhenAll(tasks);
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