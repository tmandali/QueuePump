using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    internal class Service
    {
        IList<ConnectionStringSettings> hostlist;
        CancellationTokenSource cancellationTokenSource;
        Task[] hosts;

        public Service Init()
        {
            cancellationTokenSource = new CancellationTokenSource();
            hostlist = ReadHost(new List<ConnectionStringSettings>());
            return this;
        }

        public void Start(int maxConcurrencyPerHost, TimeSpan retry)
        {
            hosts = hostlist.Select(h => new HostConnect(h).Starter(maxConcurrencyPerHost, retry, cancellationTokenSource.Token)).ToArray();
            Trace.TraceInformation("Service started");
        }

        public async Task Stop()
        {
            cancellationTokenSource.Cancel();
            var timeout = Task.Delay(TimeSpan.FromSeconds(30));
            var finaly = await Task.WhenAny(timeout, Task.WhenAll(hosts)).ConfigureAwait(false);

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
