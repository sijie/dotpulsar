using System;
using System.Threading;

namespace DotPulsar
{
    public sealed class ExceptionContext
    {
        internal ExceptionContext(Exception exception, CancellationToken cancellationToken)
        {
            Exception = exception;
            CancellationToken = cancellationToken;
            ExceptionHandled = false;
            Result = FaultAction.Rethrow;
        }

        public Exception Exception { set; get; }
        public CancellationToken CancellationToken { get; }
        public bool ExceptionHandled { get; set; }
        public FaultAction Result { get; set; }
    }
}
