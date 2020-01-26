using DotPulsar.Internal.Abstractions;

namespace DotPulsar.Internal
{
    public sealed class ProducerProxy : IProducerProxy
    {
        private readonly object _lock;
        private readonly IStateManager<ProducerState> _stateManager;
        private bool _hasDisconnected;

        public ProducerProxy(IStateManager<ProducerState> stateManager)
        {
            _lock = new object();
            _stateManager = stateManager;
            _hasDisconnected = false;
        }

        public void Connected() => SetState(ProducerState.Connected);

        public void Disconnected() => SetState(ProducerState.Disconnected);

        private void SetState(ProducerState state)
        {
            lock (_lock)
            {
                if (_hasDisconnected)
                    return;

                _stateManager.SetState(state);
                _hasDisconnected = state == ProducerState.Disconnected;
            }
        }
    }
}
