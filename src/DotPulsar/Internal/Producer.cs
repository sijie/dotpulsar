using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Producer : IProducer
    {
        private readonly IExecute _executor;
        private readonly IProducerStreamFactory _streamFactory;
        private readonly IStateChanged<ProducerState> _state;
        private readonly CancellationTokenSource _connectTokenSource;
        private readonly Task _connectTask;
        private int _isDisposed;

        private IProducerStream Stream { get; set; }

        public Producer(IProducerStreamFactory streamFactory, IProducerStream initialStream, IExecute executor, IStateManager<ProducerState> state)
        {
            _executor = executor;
            _streamFactory = streamFactory;
            _state = state;
            _connectTokenSource = new CancellationTokenSource();
            Stream = initialStream;
            _isDisposed = 0;
            _connectTask = Connect(_connectTokenSource.Token);
        }

        public async ValueTask<ProducerState> StateChangedTo(ProducerState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedTo(state, cancellationToken);
        }

        public async ValueTask<ProducerState> StateChangedFrom(ProducerState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedFrom(state, cancellationToken);
        }

        public bool IsFinalState()
        {
            ThrowIfDisposed();
            return _state.IsFinalState();
        }

        public bool IsFinalState(ProducerState state)
        {
            ThrowIfDisposed();
            return _state.IsFinalState(state);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _connectTokenSource.Cancel();
            await _connectTask;
        }

        public async ValueTask<MessageId> Send(byte[] data, CancellationToken cancellationToken) => await Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => await Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(ReadOnlySequence<byte> data, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => Stream.Send(data), cancellationToken);
            return new MessageId(response.MessageId);
        }

        public async ValueTask<MessageId> Send(MessageMetadata metadata, byte[] data, CancellationToken cancellationToken)
            => await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
            => await Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public async ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlySequence<byte> data, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => Stream.Send(metadata.Metadata, data), cancellationToken);
            return new MessageId(response.MessageId);
        }

        private async Task Connect(CancellationToken cancellationToken)
        {
            await Task.Yield();

            await foreach (var stream in _streamFactory.Streams(cancellationToken))
            {
                Stream = stream;
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(Producer));
        }
    }
}
