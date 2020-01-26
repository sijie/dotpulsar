using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal.Exceptions;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class DefaultExceptionHandler : IHandleException
    {
        private readonly TimeSpan _retryInterval;

        public DefaultExceptionHandler(TimeSpan retryInterval) => _retryInterval = retryInterval;

        public async ValueTask OnException(ExceptionContext exceptionContext)
        {
            exceptionContext.Result = DetermineFaultAction(exceptionContext.Exception);
            if (exceptionContext.Result == FaultAction.Retry)
                await Task.Delay(_retryInterval, exceptionContext.CancellationToken);
            exceptionContext.ExceptionHandled = true;
        }

        private FaultAction DetermineFaultAction(Exception exception)
        {
            switch (exception)
            {
                case TooManyRequestsException _: return FaultAction.Retry;
                case StreamNotReadyException _: return FaultAction.Retry;
                case ServiceNotReadyException _: return FaultAction.Retry;
                case OperationCanceledException _: return FaultAction.Rethrow;
                case DotPulsarException _: return FaultAction.Rethrow;
                case SocketException socketException:
                    switch (socketException.SocketErrorCode)
                    {
                        case SocketError.HostNotFound:
                        case SocketError.HostUnreachable:
                        case SocketError.NetworkUnreachable:
                            return FaultAction.Rethrow;
                    }
                    break;
            }

            return FaultAction.Retry;
        }
    }
}
