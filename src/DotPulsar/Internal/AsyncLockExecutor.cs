using DotPulsar.Internal.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class AsyncLockExecutor : IExecute, IAsyncDisposable
    {
        private readonly AsyncLock _lock;
        private readonly IExecute _executor;

        public AsyncLockExecutor(IExecute executor)
        {
            _lock = new AsyncLock();
            _executor = executor;
        }

        public async ValueTask DisposeAsync() => await _lock.DisposeAsync();

        public async ValueTask Execute(Action action, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                await _executor.Execute(action, cancellationToken);
            }
        }

        public async ValueTask Execute(Func<Task> func, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                await _executor.Execute(func, cancellationToken);
            }
        }

        public async ValueTask Execute(Func<ValueTask> func, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                await _executor.Execute(func, cancellationToken);
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<TResult> func, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                return await _executor.Execute(func, cancellationToken);
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                return await _executor.Execute(func, cancellationToken);
            }
        }

        public async ValueTask<TResult> Execute<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                return await _executor.Execute(func, cancellationToken);
            }
        }
    }
}
