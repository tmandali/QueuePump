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
            prs.Init(3, 
                TimeSpan.FromSeconds(10), 
                ex => Trace.TraceError(ex.InnerException.Message));
            prs.Start();
            Console.ReadLine();
            await prs.Stop();
        }        
    }    
}