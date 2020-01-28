﻿using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;
using System.Threading.Tasks;

namespace DotPulsar.Internal
{
    public sealed class Connection : IConnection
    {
        private readonly AsyncLock _lock;
        private readonly ProducerManager _producerManager;
        private readonly ConsumerManager _consumerManager;
        private readonly RequestResponseHandler _requestResponseHandler;
        private readonly PingPongHandler _pingPongHandler;
        private readonly PulsarStream _stream;

        public Connection(PulsarStream stream)
        {
            _lock = new AsyncLock();
            _producerManager = new ProducerManager();
            _consumerManager = new ConsumerManager();
            _requestResponseHandler = new RequestResponseHandler();
            _pingPongHandler = new PingPongHandler(this);
            _stream = stream;
            _ = ProcessStream();
        }

        public async ValueTask<bool> IsActive()
        {
            using (await _lock.Lock())
            {
                return _producerManager.HasProducers || _consumerManager.HasConsumers;
            }
        }

        public async Task<ProducerResponse> Send(CommandProducer command, IProducerProxy proxy)
        {
            Task<BaseCommand>? responseTask = null;
            using (await _lock.Lock())
            {
                _producerManager.Outgoing(command, proxy);
                var baseCommand = command.AsBaseCommand();
                responseTask = _requestResponseHandler.Outgoing(baseCommand);
                var sequence = Serializer.Serialize(baseCommand);
                await _stream.Send(sequence);
            }

            var response = await responseTask;
            if (response.CommandType == BaseCommand.Type.Error)
            {
                _producerManager.Remove(command.ProducerId);
                response.Error.Throw();
            }

            return new ProducerResponse(command.ProducerId, response.ProducerSuccess.ProducerName);
        }

        public async Task<SubscribeResponse> Send(CommandSubscribe command, IConsumerProxy proxy)
        {
            Task<BaseCommand>? responseTask = null;
            using (await _lock.Lock())
            {
                _consumerManager.Outgoing(command, proxy);
                var baseCommand = command.AsBaseCommand();
                responseTask = _requestResponseHandler.Outgoing(baseCommand);
                var sequence = Serializer.Serialize(baseCommand);
                await _stream.Send(sequence);
            }

            var response = await responseTask;
            if (response.CommandType == BaseCommand.Type.Error)
            {
                _consumerManager.Remove(command.ConsumerId);
                response.Error.Throw();
            }

            return new SubscribeResponse(command.ConsumerId);
        }

        public async Task Send(CommandPing command) => await Send(command.AsBaseCommand());
        public async Task Send(CommandPong command) => await Send(command.AsBaseCommand());
        public async Task Send(CommandAck command) => await Send(command.AsBaseCommand());
        public async Task Send(CommandFlow command) => await Send(command.AsBaseCommand());

        public async Task<BaseCommand> Send(CommandUnsubscribe command)
        {
            var response = await SendRequestResponse(command.AsBaseCommand());
            if (response.CommandType == BaseCommand.Type.Success)
                _consumerManager.Remove(command.ConsumerId);
            return response;
        }

        public async Task<BaseCommand> Send(CommandConnect command) => await SendRequestResponse(command.AsBaseCommand());
        public async Task<BaseCommand> Send(CommandLookupTopic command) => await SendRequestResponse(command.AsBaseCommand());
        public async Task<BaseCommand> Send(CommandSeek command) => await SendRequestResponse(command.AsBaseCommand());
        public async Task<BaseCommand> Send(CommandGetLastMessageId command) => await SendRequestResponse(command.AsBaseCommand());

        public async Task<BaseCommand> Send(CommandCloseProducer command)
        {
            var response = await SendRequestResponse(command.AsBaseCommand());
            if (response.CommandType == BaseCommand.Type.Success)
                _producerManager.Remove(command.ProducerId);
            return response;
        }

        public async Task<BaseCommand> Send(CommandCloseConsumer command)
        {
            var response = await SendRequestResponse(command.AsBaseCommand());
            if (response.CommandType == BaseCommand.Type.Success)
                _consumerManager.Remove(command.ConsumerId);
            return response;
        }

        public async Task<BaseCommand> Send(SendPackage command)
        {
            Task<BaseCommand>? response = null;
            using (await _lock.Lock())
            {
                var baseCommand = command.Command.AsBaseCommand();
                response = _requestResponseHandler.Outgoing(baseCommand);
                var sequence = Serializer.Serialize(baseCommand, command.Metadata, command.Payload);
                await _stream.Send(sequence);
            }
            return await response;
        }

        private async Task<BaseCommand> SendRequestResponse(BaseCommand command)
        {
            Task<BaseCommand>? response = null;
            using (await _lock.Lock())
            {
                response = _requestResponseHandler.Outgoing(command);
                var sequence = Serializer.Serialize(command);
                await _stream.Send(sequence);
            }
            return await response;
        }

        private async Task Send(BaseCommand command)
        {
            using (await _lock.Lock())
            {
                var sequence = Serializer.Serialize(command);
                await _stream.Send(sequence);
            }
        }

        private async Task ProcessStream()
        {
            await Task.Yield();

            await foreach (var frame in _stream.Frames(default))
            {
                var commandSize = frame.ReadUInt32(0, true);
                var command = Serializer.Deserialize<BaseCommand>(frame.Slice(4, commandSize));

                switch (command.CommandType)
                {
                    case BaseCommand.Type.Message:
                        _consumerManager.Incoming(command.Message, frame.Slice(commandSize + 4));
                        break;
                    case BaseCommand.Type.CloseConsumer:
                        _consumerManager.Incoming(command.CloseConsumer);
                        break;
                    case BaseCommand.Type.ActiveConsumerChange:
                        _consumerManager.Incoming(command.ActiveConsumerChange);
                        break;
                    case BaseCommand.Type.ReachedEndOfTopic:
                        _consumerManager.Incoming(command.ReachedEndOfTopic);
                        break;
                    case BaseCommand.Type.CloseProducer:
                        _producerManager.Incoming(command.CloseProducer);
                        break;
                    case BaseCommand.Type.Ping:
                        _pingPongHandler.Incoming(command.Ping);
                        break;
                    default:
                        _requestResponseHandler.Incoming(command);
                        break;
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _lock.DisposeAsync();
            _consumerManager.Dispose();
            _producerManager.Dispose();
            _requestResponseHandler.Dispose();
            await _stream.DisposeAsync();
        }
    }
}
