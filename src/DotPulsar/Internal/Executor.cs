using DotPulsar.Abstractions;
using DotPulsar.Internal.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Executor : IExecute
    {
        private readonly IHandleException _exceptionHandler;

        public Executor(IHandleException exceptionHandler) => _exceptionHandler = exceptionHandler;

        public async ValueTask Execute(Action action, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    action();
                    return;
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        public async ValueTask Execute(Func<Task> func, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await func();
                    return;
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        public async ValueTask Execute(Func<ValueTask> func, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await func();
                    return;
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<TResult> func, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    return func();
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    return await func();
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    return await func();
                }
                catch (Exception exception)
                {
                    if (await Handle(exception, cancellationToken))
                        throw;
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private async ValueTask<bool> Handle(Exception exception, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return true;

            var context = new ExceptionContext(exception, cancellationToken);
            await _exceptionHandler.OnException(context);
            if (context.Result == FaultAction.ThrowException)
                throw context.Exception;
            return context.Result == FaultAction.Rethrow;
        }
    }
}
