using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ProducerStreamFactory : IProducerStreamFactory
    {
        private readonly IConnectionPool _connectionPool;
        private readonly IStateManager<ProducerState> _stateManager;
        private readonly IExecute _executor;
        private readonly SequenceId _sequenceId;
        private readonly CommandProducer _commandProducer;

        public ProducerStreamFactory(IConnectionPool connectionPool, IStateManager<ProducerState> stateManager, IExecute executor, ProducerOptions options)
        {
            _connectionPool = connectionPool;
            _stateManager = stateManager;
            _executor = executor;
            _sequenceId = new SequenceId(options.InitialSequenceId);

            _commandProducer = new CommandProducer
            {
                ProducerName = options.ProducerName,
                Topic = options.Topic
            };
        }

        public async IAsyncEnumerable<IProducerStream> Streams([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var proxy = new ProducerProxy(_stateManager);
                var stream = await _executor.Execute(() => GetStream(proxy, cancellationToken), cancellationToken);
                yield return stream;
                proxy.Connected();
                await _stateManager.StateChangedTo(ProducerState.Disconnected, cancellationToken);
            }
        }

        private async ValueTask<IProducerStream> GetStream(ProducerProxy proxy, CancellationToken cancellationToken)
        {
            var connection = await _connectionPool.FindConnectionForTopic(_commandProducer.Topic, cancellationToken);
            var response = await connection.Send(_commandProducer, proxy);
            return new ProducerStream(response.ProducerId, response.ProducerName, _sequenceId, connection);
        }
    }
}
