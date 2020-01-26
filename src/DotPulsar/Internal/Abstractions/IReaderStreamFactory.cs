using System;
using System.Collections.Generic;
using System.Threading;

namespace DotPulsar.Internal.Abstractions
{
    public interface IReaderStreamFactory : IAsyncDisposable
    {
        IAsyncEnumerable<IReaderStream> Streams(CancellationToken cancellationToken = default);
    }
}
