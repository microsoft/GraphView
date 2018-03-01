namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A redis sequence number generator for redis database index
    /// Redis supports unlimited number of databases, the unique number ensures that 
    ///     no repeated database are used
    /// </summary>
    internal class RedisDbIndexSequenceGenerator
    {
        private static volatile RedisDbIndexSequenceGenerator instance;
        private static readonly object initLock = new object();
        private long sequenceNumber;
        private readonly object sequenceLock;

        private RedisDbIndexSequenceGenerator()
        {
            this.sequenceNumber = 0;
            this.sequenceLock = new object();
        }

        internal static RedisDbIndexSequenceGenerator Instance
        {
            get
            {
                if (RedisDbIndexSequenceGenerator.instance == null)
                {
                    lock (RedisDbIndexSequenceGenerator.initLock)
                    {
                        if (RedisDbIndexSequenceGenerator.instance == null)
                        {
                            RedisDbIndexSequenceGenerator.instance = new RedisDbIndexSequenceGenerator();
                        }
                    }
                }
                return RedisDbIndexSequenceGenerator.instance;
            }
        }

        internal long NextSequenceNumber()
        {
            long dbIndexNumber;
            lock (this.sequenceLock)
            {
                dbIndexNumber = this.sequenceNumber;
                this.sequenceNumber++;
                Monitor.Pulse(this.sequenceLock);
            }
            return dbIndexNumber;
        }
    }
}
