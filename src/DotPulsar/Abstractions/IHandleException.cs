using System.Threading.Tasks;

namespace DotPulsar.Abstractions
{
    /// <summary>
    /// An exception handling abstraction.
    /// </summary>
    public interface IHandleException
    {
        /// <summary>
        /// Called after an action has thrown an Exception.
        /// </summary>
        ValueTask OnException(ExceptionContext exceptionContext);
    }
}
