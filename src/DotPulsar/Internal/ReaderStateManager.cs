using DotPulsar.Internal.Abstractions;

namespace DotPulsar.Internal
{
    public sealed class ReaderStateManager
    {
        private readonly object _lock;
        private readonly IStateManager<ReaderState> _stateManager;
        private OperatorState _operatorState;
        private StreamState _streamState;

        public ReaderStateManager(IStateManager<ReaderState> stateManager)
        {
            _lock = new object();
            _stateManager = stateManager;
            _operatorState = OperatorState.NotReady;
            _streamState = StreamState.Disconnected;
        }

        public void StreamConnected()
        {
            lock (_lock)
            {
                _streamState = StreamState.Connected;
                SetReaderState();
            }
        }

        public void StreamDisconnected()
        {
            lock (_lock)
            {
                _streamState = StreamState.Disconnected;
                SetReaderState();
            }
        }

        public void OperatorDisposed()
        {
            lock (_lock)
            {
                _operatorState = OperatorState.Disposed;
                SetReaderState();
            }
        }

        public void OperatorReady()
        {
            lock (_lock)
            {
                _operatorState = OperatorState.Ready;
                SetReaderState();
            }
        }

        public void OperatorNotReady()
        {
            lock (_lock)
            {
                _operatorState = OperatorState.NotReady;
                SetReaderState();
            }
        }

        public void OperatorFaulted()
        {
            lock (_lock)
            {
                _operatorState = OperatorState.Faulted;
                SetReaderState();
            }
        }

        private void SetReaderState()
        {
            if (_streamState == StreamState.Disconnected)
                _stateManager.SetState(ReaderState.Disconnected);
        }

        private enum OperatorState : byte
        {
            Disposed,
            NotReady,
            Ready,
            Faulted
        }

        private enum StreamState : byte
        {
            Disconnected,
            Connected,
            ReachedEndOfTopic
        }
    }
}
