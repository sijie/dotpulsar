using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal.Abstractions
{
    public interface IConnectionPool : IAsyncDisposable
    {
        ValueTask<IConnection> FindConnectionForTopic(string topic, CancellationToken cancellationToken);
    }
}
