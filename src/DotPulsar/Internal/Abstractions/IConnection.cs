using DotPulsar.Internal.PulsarApi;
using System;
using System.Threading.Tasks;

namespace DotPulsar.Internal.Abstractions
{
    public interface IConnection : IAsyncDisposable
    {
        public Task IsClosed { get; }

        ValueTask<bool> IsActive();

        Task<ProducerResponse> Send(CommandProducer command, IProducerProxy proxy);
        Task<SubscribeResponse> Send(CommandSubscribe command, IConsumerProxy proxy);

        Task Send(CommandPing command);
        Task Send(CommandPong command);
        Task Send(CommandAck command);
        Task Send(CommandFlow command);

        Task<BaseCommand> Send(CommandUnsubscribe command);
        Task<BaseCommand> Send(CommandConnect command);
        Task<BaseCommand> Send(CommandLookupTopic command);
        Task<BaseCommand> Send(CommandSeek command);
        Task<BaseCommand> Send(CommandGetLastMessageId command);
        Task<BaseCommand> Send(CommandCloseProducer command);
        Task<BaseCommand> Send(CommandCloseConsumer command);
        Task<BaseCommand> Send(SendPackage command);
    }
}
