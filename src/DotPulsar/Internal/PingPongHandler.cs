using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;

namespace DotPulsar.Internal
{
    public sealed class PingPongHandler
    {
        private readonly IConnection _connection;
        private readonly CommandPong _pong;

        public PingPongHandler(IConnection connection)
        {
            _connection = connection;
            _pong = new CommandPong();
        }

        public void Incoming(CommandPing ping)
        {
            _ = _connection.Send(_pong);
        }
    }
}
