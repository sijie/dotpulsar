using DotPulsar.Abstractions;

namespace DotPulsar.Internal.Abstractions
{
    public interface IStateManager<TState> : IStateChanged<TState> where TState : notnull
    {
        TState CurrentState { get; }
        TState SetState(TState state);
    }
}
