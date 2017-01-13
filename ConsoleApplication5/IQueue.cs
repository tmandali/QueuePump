﻿using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    internal interface IQueue
    {
        string Name { get; }

        Task Receive(int i, CancellationToken ct);
    }
}
