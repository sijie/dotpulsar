using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Reader : IReader
    {
        private readonly IReaderStreamFactory _streamFactory;
        private readonly IExecute _executor;
        private readonly IStateChanged<ReaderState> _state;
        private int _isDisposed;

        private IReaderStream Stream { get; set; }

        public Reader(IReaderStreamFactory streamFactory, IReaderStream initialStream, IExecute executor, IStateChanged<ReaderState> state)
        {
            _streamFactory = streamFactory;
            _executor = executor;
            _state = state;
            Stream = initialStream;
            _isDisposed = 0;
            _ = Task.Run(Connect);
        }

        public async ValueTask<ReaderState> StateChangedTo(ReaderState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedTo(state, cancellationToken);
        }

        public async ValueTask<ReaderState> StateChangedFrom(ReaderState state, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            return await _state.StateChangedFrom(state, cancellationToken);
        }

        public bool IsFinalState()
        {
            ThrowIfDisposed();
            return _state.IsFinalState();
        }

        public bool IsFinalState(ReaderState state)
        {
            ThrowIfDisposed();
            return _state.IsFinalState(state);
        }

        public async IAsyncEnumerable<Message> Messages([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            while (!cancellationToken.IsCancellationRequested)
            {
                yield return await _executor.Execute(() => Stream.Receive(cancellationToken), cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            if (_executor is IAsyncDisposable disposable)
                await disposable.DisposeAsync();

            await _streamFactory.DisposeAsync();
        }

        private async Task Connect()
        {
            try
            {
                await foreach (var stream in _streamFactory.Streams())
                {
                    Stream = stream;
                }
            }
            catch
            {
                // Ignore
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(Reader));
        }
    }
}
