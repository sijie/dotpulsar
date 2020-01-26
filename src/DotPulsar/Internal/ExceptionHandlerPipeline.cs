using DotPulsar.Abstractions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class ExceptionHandlerPipeline : IHandleException
    {
        private readonly IHandleException[] _handlers;

        public ExceptionHandlerPipeline(IEnumerable<IHandleException> handlers) => _handlers = handlers.ToArray();

        public async ValueTask OnException(ExceptionContext exceptionContext)
        {
            foreach (var handler in _handlers)
            {
                await handler.OnException(exceptionContext);
                if (exceptionContext.ExceptionHandled)
                    break;
            }
        }
    }
}
