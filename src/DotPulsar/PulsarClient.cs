using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar
{
    public sealed class PulsarClient : IPulsarClient
    {
        private readonly object _lock;
        private readonly IExecute _executor;
        private readonly LinkedList<IAsyncDisposable> _disposabels;
        private readonly IConnectionPool _connectionPool;
        private int _isDisposed;

        internal PulsarClient(IConnectionPool connectionPool, IExecute executor)
        {
            _lock = new object();
            _disposabels = new LinkedList<IAsyncDisposable>();
            _connectionPool = connectionPool;
            _executor = executor;
            _isDisposed = 0;
        }

        public static IPulsarClientBuilder Builder() => new PulsarClientBuilder();

        public IProducer CreateProducer(ProducerOptions options)
        {
            ThrowIfDisposed();
            var stateManager = new StateManager<ProducerState>(ProducerState.Disconnected, ProducerState.Closed, ProducerState.Faulted);
            var producerStreamFactory = new ProducerStreamFactory(_connectionPool, stateManager, _executor, options);
            var producer = new Producer(producerStreamFactory, new NotReadyStream(), _executor, stateManager);
            Add(producer);
            producer.StateChangedTo(ProducerState.Closed, default).AsTask().ContinueWith(t => Remove(producer));
            return producer;
        }

        public IConsumer CreateConsumer(ConsumerOptions options)
        {
            ThrowIfDisposed();
            var stateManager = new StateManager<ConsumerState>(ConsumerState.Disconnected, ConsumerState.Closed, ConsumerState.ReachedEndOfTopic, ConsumerState.Faulted);
            var consumerStreamFactory = new ConsumerStreamFactory(_connectionPool, stateManager, _executor, options);
            var consumer = new Consumer(consumerStreamFactory, _executor, new NotReadyStream(), stateManager);
            Add(consumer);
            consumer.StateChangedTo(ConsumerState.Closed, default).AsTask().ContinueWith(t => Remove(consumer));
            return consumer;
        }

        public IReader CreateReader(ReaderOptions options)
        {
            ThrowIfDisposed();
            var stateManager = new StateManager<ReaderState>(ReaderState.Disconnected, ReaderState.Closed, ReaderState.ReachedEndOfTopic, ReaderState.Faulted);
            var streamFactory = new ReaderStreamFactory(_connectionPool, stateManager, _executor, options);
            var reader = new Reader(streamFactory, new NotReadyStream(), _executor, stateManager);
            Add(reader);
            reader.StateChangedTo(ReaderState.Closed, default).AsTask().ContinueWith(t => Remove(reader));
            return reader;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            IAsyncDisposable[] disposables;
            lock (_lock)
            {
                disposables = _disposabels.ToArray();
                _disposabels.Clear();
            }

            foreach (var disposable in disposables)
            {
                await disposable.DisposeAsync();
            }

            await _connectionPool.DisposeAsync();
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(PulsarClient));
        }

        private void Add(IAsyncDisposable disposable)
        {
            lock (_lock)
            {
                _disposabels.AddFirst(disposable);
            }
        }

        private void Remove(IAsyncDisposable disposable)
        {
            lock (_lock)
            {
                _disposabels.Remove(disposable);
            }
        }
    }
}
