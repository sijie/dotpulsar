using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Consumer : IConsumer
    {
        private readonly CommandAck _cachedCommandAck;
        private readonly IConsumerStreamFactory _streamFactory;
        private readonly IExecute _executor;
        private readonly IStateChanged<ConsumerState> _state;
        private readonly CancellationTokenSource _connectTokenSource;
        private readonly Task _connectTask;
        private int _isDisposed;

        private IConsumerStream Stream { get; set; }

        public Consumer(IConsumerStreamFactory streamFactory, IExecute executor, IConsumerStream initialStream, IStateChanged<ConsumerState> state)
        {
            _cachedCommandAck = new CommandAck();
            _state = state;
            _streamFactory = streamFactory;
            _executor = executor;
            _connectTokenSource = new CancellationTokenSource();
            _isDisposed = 0;
            Stream = initialStream;
            _connectTask = Connect(_connectTokenSource.Token);
        }

        public async ValueTask<ConsumerState> StateChangedTo(ConsumerState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedTo(state, cancellationToken);
        }

        public async ValueTask<ConsumerState> StateChangedFrom(ConsumerState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedFrom(state, cancellationToken);
        }

        public bool IsFinalState()
        {
            ThrowIfDisposed();
            return _state.IsFinalState();
        }

        public bool IsFinalState(ConsumerState state)
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

        public async IAsyncEnumerable<Message> Messages([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            while (!cancellationToken.IsCancellationRequested)
            {
                yield return await _executor.Execute(() => Stream.Receive(cancellationToken), cancellationToken);
            }
        }

        public async ValueTask Acknowledge(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask AcknowledgeCumulative(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask Unsubscribe(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            _ = await _executor.Execute(() => Stream.Send(new CommandUnsubscribe()), cancellationToken);
        }

        public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var seek = new CommandSeek { MessageId = messageId.Data };
            _ = await _executor.Execute(() => Stream.Send(seek), cancellationToken);
            return;
        }

        public async ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => Stream.Send(new CommandGetLastMessageId()), cancellationToken);
            return new MessageId(response.LastMessageId);
        }

        private async ValueTask Acknowledge(MessageIdData messageIdData, CommandAck.AckType ackType, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            await _executor.Execute(() =>
            {
                _cachedCommandAck.Type = ackType;
                _cachedCommandAck.MessageIds.Clear();
                _cachedCommandAck.MessageIds.Add(messageIdData);
                return Stream.Send(_cachedCommandAck);
            }, cancellationToken);
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
                throw new ObjectDisposedException(nameof(Consumer));
        }
    }
}
