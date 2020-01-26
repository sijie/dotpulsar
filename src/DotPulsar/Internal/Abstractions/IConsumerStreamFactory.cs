using System.Collections.Generic;
using System.Threading;

namespace DotPulsar.Internal.Abstractions
{
    public interface IConsumerStreamFactory
    {
        IAsyncEnumerable<IConsumerStream> Streams(CancellationToken cancellationToken = default);
    }
}
