

namespace GraphView.Transaction
{
    using System.Threading;

    /// <summary>
    /// An interface for generating strictly-incremented sequence numbers for transactions
    /// </summary>
    internal interface ITxSequenceGenerator
    {
        long NextSequenceNumber();
    }

    /// <summary>
    /// A singleton sequence number generator. The generated numbers are only unique in one machine.
    /// For concurrent transactions on separate machines, either all transactions acquire sequence numbers 
    /// from the same machine, or generated sequence numbers are sync'ed across machines through a centralized source, 
    /// e.g., the underlying logging store.
    /// </summary>
    internal class SingletonTxSequenceGenerator : ITxSequenceGenerator
    {
        private static volatile SingletonTxSequenceGenerator instance;
        private static object initiLock = new object();
        private long sequenceNumber;
        private object sequenceLock;

        private SingletonTxSequenceGenerator()
        {
            this.sequenceNumber = -1;
            this.sequenceLock = new object();
        }

        internal static SingletonTxSequenceGenerator Instance
        {
            get
            {
                if (SingletonTxSequenceGenerator.instance == null)
                {
                    lock (SingletonTxSequenceGenerator.initiLock)
                    {
                        if (SingletonTxSequenceGenerator.instance == null)
                        {
                            SingletonTxSequenceGenerator.instance = new SingletonTxSequenceGenerator();
                        }
                    }
                }

                return SingletonTxSequenceGenerator.instance;
            }
        }

        internal void Initialize(LogStore logstore = null)
        {
            lock(this.sequenceLock)
            {
                if (this.sequenceNumber < 0)
                {
                    this.sequenceNumber = logstore == null ? 0 : logstore.GetMaxTxSequenceNumber() + 1;
                }

                Monitor.Pulse(this.sequenceLock);
            }
        }

        public long NextSequenceNumber()
        {
            long txNumber;
            lock (this.sequenceLock)
            {
                txNumber = this.sequenceNumber;
                this.sequenceNumber++;
                Monitor.Pulse(this.sequenceLock);
            }

            return txNumber;
        }
    }
}
