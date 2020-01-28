namespace DotPulsar.Internal.Abstractions
{
    public interface IRegisterEvent
    {
        void Register(IEvent @event);
    }
}
