using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal.Abstractions
{
    public interface IReaderStream
    {
        ValueTask<Message> Receive(CancellationToken cancellationToken = default);
    }
}
