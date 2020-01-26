using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ReaderStreamFactory : IReaderStreamFactory
    {
        private readonly IConnectionPool _connectionPool;
        private readonly IStateManager<ReaderState> _stateManager;
        private readonly IExecute _executor;
        private readonly CommandSubscribe _subscribe;
        private readonly uint _messagePrefetchCount;
        private readonly BatchHandler _batchHandler;

        public ReaderStreamFactory(IConnectionPool connectionPool, IStateManager<ReaderState> stateManager, IExecute executor, ReaderOptions options)
        {
            _connectionPool = connectionPool;
            _stateManager = stateManager;
            _executor = executor;
            _messagePrefetchCount = options.MessagePrefetchCount;

            _subscribe = new CommandSubscribe
            {
                ConsumerName = options.ReaderName,
                Durable = false,
                ReadCompacted = options.ReadCompacted,
                StartMessageId = options.StartMessageId.Data,
                Subscription = "Reader-" + Guid.NewGuid().ToString("N"),
                Topic = options.Topic
            };

            _batchHandler = new BatchHandler(false);
        }

        public ValueTask DisposeAsync()
        {
            _stateManager.SetState(ReaderState.Closed);
            return new ValueTask();
        }

        public async IAsyncEnumerable<IReaderStream> Streams([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using var proxy = new ReaderProxy(_stateManager, new AsyncQueue<MessagePackage>());
                var stream = await _executor.Execute(() => GetStream(proxy, cancellationToken), cancellationToken);
                yield return stream;
                proxy.Active();
                await _stateManager.StateChangedTo(ReaderState.Disconnected, cancellationToken);
            }
        }

        private async ValueTask<IReaderStream> GetStream(ReaderProxy proxy, CancellationToken cancellationToken)
        {
            var connection = await _connectionPool.FindConnectionForTopic(_subscribe.Topic, cancellationToken);
            var response = await connection.Send(_subscribe, proxy);
            return new ConsumerStream(response.ConsumerId, _messagePrefetchCount, proxy, connection, proxy, _batchHandler);
        }
    }
}
