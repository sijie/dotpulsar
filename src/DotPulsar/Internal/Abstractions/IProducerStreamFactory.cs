using System.Collections.Generic;
using System.Threading;

namespace DotPulsar.Internal.Abstractions
{
    public interface IProducerStreamFactory
    {
        IAsyncEnumerable<IProducerStream> Streams(CancellationToken cancellationToken = default);
    }
}
