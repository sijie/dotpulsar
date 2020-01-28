#if NETSTANDARD2_1

using System.Diagnostics.Tracing;
using System.Threading;

namespace DotPulsar.Internal
{
    public sealed class DotPulsarEventSource : EventSource
    {
        private readonly PollingCounter _totalClientsCounter;
        private long _totalClients;

        private readonly PollingCounter _currentClientsCounter;
        private long _currentClients;

        private readonly PollingCounter _totalConnectionsCounter;
        private long _totalConnections;

        private readonly PollingCounter _currentConnectionsCounter;
        private long _currentConnections;

        private readonly PollingCounter _totalConsumersCounter;
        private long _totalConsumers;

        private readonly PollingCounter _currentConsumersCounter;
        private long _currentConsumers;

        private readonly PollingCounter _totalProducersCounter;
        private long _totalProducers;

        private readonly PollingCounter _currentProducersCounter;
        private long _currentProducers;

        private readonly PollingCounter _totalReadersCounter;
        private long _totalReaders;

        private readonly PollingCounter _currentReadersCounter;
        private long _currentReaders;

        public static readonly DotPulsarEventSource Log = new DotPulsarEventSource();

        public DotPulsarEventSource() : base("DotPulsar")
        {
            _totalClientsCounter = new PollingCounter("total-clients", this, () => _totalClients)
            {
                DisplayName = "Total number of clients"
            };

            _currentClientsCounter = new PollingCounter("current-clients", this, () => _currentClients)
            {
                DisplayName = "Current number of clients"
            };

            _totalConnectionsCounter = new PollingCounter("total-connections", this, () => _totalClients)
            {
                DisplayName = "Total number of connections"
            };

            _currentConnectionsCounter = new PollingCounter("current-connections", this, () => _currentClients)
            {
                DisplayName = "Current number of connections"
            };

            _totalConsumersCounter = new PollingCounter("total-consumers", this, () => _totalClients)
            {
                DisplayName = "Total number of consumers"
            };

            _currentConsumersCounter = new PollingCounter("current-consumers", this, () => _currentClients)
            {
                DisplayName = "Current number of consumers"
            };

            _totalProducersCounter = new PollingCounter("total-producers", this, () => _totalClients)
            {
                DisplayName = "Total number of producers"
            };

            _currentProducersCounter = new PollingCounter("current-producers", this, () => _currentClients)
            {
                DisplayName = "Current number of producers"
            };

            _totalReadersCounter = new PollingCounter("total-readers", this, () => _totalClients)
            {
                DisplayName = "Total number of readers"
            };

            _currentReadersCounter = new PollingCounter("current-readers", this, () => _currentClients)
            {
                DisplayName = "Current number of readers"
            };
        }

        public void ClientCreated() 
        {
            Interlocked.Increment(ref _totalClients);
            Interlocked.Increment(ref _currentClients);
        }

        public void ClientDisposed()
        {
            Interlocked.Decrement(ref _currentClients);
        }

        public void ConnectionCreated()
        {
            Interlocked.Increment(ref _totalConnections);
            Interlocked.Increment(ref _currentConnections);
        }

        public void ConnectionDisposed()
        {
            Interlocked.Decrement(ref _currentConnections);
        }

        public void ConsumerCreated()
        {
            Interlocked.Increment(ref _totalConsumers);
            Interlocked.Increment(ref _currentConsumers);
        }

        public void ConsumerDisposed()
        {
            Interlocked.Decrement(ref _currentConsumers);
        }

        public void ProducerCreated()
        {
            Interlocked.Increment(ref _totalProducers);
            Interlocked.Increment(ref _currentProducers);
        }

        public void ProducerDisposed()
        {
            Interlocked.Decrement(ref _currentProducers);
        }

        public void ReaderCreated()
        {
            Interlocked.Increment(ref _totalReaders);
            Interlocked.Increment(ref _currentReaders);
        }

        public void ReaderDisposed()
        {
            Interlocked.Decrement(ref _currentReaders);
        }
    }

}

#endif
