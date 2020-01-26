﻿using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ConsumerStreamFactory : IConsumerStreamFactory
    {
        private readonly IConnectionPool _connectionPool;
        private readonly IStateManager<ConsumerState> _stateManager;
        private readonly IExecute _executor;
        private readonly CommandSubscribe _subscribe;
        private readonly uint _messagePrefetchCount;
        private readonly BatchHandler _batchHandler;

        public ConsumerStreamFactory(IConnectionPool connectionPool, IStateManager<ConsumerState> stateManager, IExecute executor, ConsumerOptions options)
        {
            _connectionPool = connectionPool;
            _stateManager = stateManager;
            _executor = executor;
            _messagePrefetchCount = options.MessagePrefetchCount;

            _subscribe = new CommandSubscribe
            {
                ConsumerName = options.ConsumerName,
                initialPosition = (CommandSubscribe.InitialPosition)options.InitialPosition,
                PriorityLevel = options.PriorityLevel,
                ReadCompacted = options.ReadCompacted,
                Subscription = options.SubscriptionName,
                Topic = options.Topic,
                Type = (CommandSubscribe.SubType)options.SubscriptionType
            };

            _batchHandler = new BatchHandler(true);
        }

        public async IAsyncEnumerable<IConsumerStream> Streams([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using var proxy = new ConsumerProxy(_stateManager, new AsyncQueue<MessagePackage>());
                var stream = await _executor.Execute(() => GetStream(proxy, cancellationToken), cancellationToken);
                yield return stream;

                if (_subscribe.Type == CommandSubscribe.SubType.Failover)
                    await _stateManager.StateChangedFrom(ConsumerState.Disconnected, cancellationToken);
                else
                    proxy.Active();

                await _stateManager.StateChangedTo(ConsumerState.Disconnected, cancellationToken);
            }
        }

        private async ValueTask<IConsumerStream> GetStream(ConsumerProxy proxy, CancellationToken cancellationToken)
        {
            var connection = await _connectionPool.FindConnectionForTopic(_subscribe.Topic, cancellationToken);
            var response = await connection.Send(_subscribe, proxy);
            return new ConsumerStream(response.ConsumerId, _messagePrefetchCount, proxy, connection, proxy, _batchHandler);
        }
    }
}
