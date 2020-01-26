using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotPulsar.Internal.Abstractions
{
    public interface IExecute
    {
        ValueTask Execute(Action action, CancellationToken cancellationToken);

        ValueTask Execute(Func<Task> func, CancellationToken cancellationToken);

        ValueTask Execute(Func<ValueTask> func, CancellationToken cancellationToken);

        ValueTask<TResult> Execute<TResult>(Func<TResult> func, CancellationToken cancellationToken);

        ValueTask<TResult> Execute<TResult>(Func<Task<TResult>> func, CancellationToken cancellationToken);

        ValueTask<TResult> Execute<TResult>(Func<ValueTask<TResult>> func, CancellationToken cancellationToken);
    }
}
