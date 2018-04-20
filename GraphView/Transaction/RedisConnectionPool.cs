namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    internal class RedisConnectionKey
    {
        internal int RedisInstanceIndex { get; private set; }

        internal long RedisDbIndex { get; private set; }

        public RedisConnectionKey(int redisInstanceIndex, long redisDbIndex)
        {
            this.RedisDbIndex = redisDbIndex;
            this.RedisInstanceIndex = redisInstanceIndex;
        }

        public override bool Equals(object obj)
        {
            RedisConnectionKey other = obj as RedisConnectionKey;
            if (obj == null)
            {
                return false;
            }

            return this.RedisDbIndex == other.RedisDbIndex &&
                this.RedisInstanceIndex == other.RedisInstanceIndex;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.RedisDbIndex.GetHashCode();
            hash = hash * 23 + this.RedisInstanceIndex.GetHashCode();
            return hash;
        }
    }

    /// <summary>
    /// A redis client pool to provide clients service for the specify redisDbIndex
    /// RedisClientPool will have a daemon thread to finish those two tasks:
    /// 1. Send requests. Once the queue size reaches the requestBatchSize or wait time
    ///                   reach the windowMicroSec, the daemon thread will send all pooling
    ///                   requests to the redis
    /// 2. Collect results. 
    /// redis client pool provide a interface to get the redis client
    /// </summary>
    internal class RedisConnectionPool
    {
        public static readonly int DEFAULT_BATCH_SIZE = 10000;

        public static readonly long DEFAULT_WINDOW_MICRO_SEC = 100L;

        public static readonly int DEFAULT_MAX_READ_POOL_SIZE = 10;

        public static readonly int DEFAULT_MAX_WRITE_POOL_SIZE = 10;
        /// <summary>
        /// The real pool manager maintaining clients pool
        /// Which is from ServiceStack.Redis
        /// </summary>
        private IRedisClientsManager redisManagerPool;

        /// <summary>
        /// Request queue holding the pending requests
        /// </summary>
        internal RedisRequest[] requestQueue = null;

        /// <summary>
        /// The current request index
        /// </summary>
        internal int currReqId;

        /// <summary>
        /// Request Window Size, client manager can reset it by the propery
        /// </summary>
        internal int RequestBatchSize { get; set; }

        /// <summary>
        /// The request pending time threshold, client manager can set it by the propery
        /// 100 micro sec = 0.1 milli sec
        /// </summary>
        internal long WindowMicroSec { get; set; }

        /// <summary>
        /// The status of current redis client pool, client manager can set it by the propery
        /// </summary>
        internal bool Active { get; set; }

        /// <summary>
        /// The spin lock to ensure that enqueue and flush are exclusive
        /// </summary>
        private SpinLock spinLock;

        public RedisConnectionPool(string redisConnectionString, long database)
        {
            // Init the pooledRedisClient Manager
            RedisClientManagerConfig config = new RedisClientManagerConfig();
            config.DefaultDb = database;
            config.MaxReadPoolSize = RedisConnectionPool.DEFAULT_MAX_READ_POOL_SIZE;
            config.MaxWritePoolSize = RedisConnectionPool.DEFAULT_MAX_WRITE_POOL_SIZE;

            this.redisManagerPool =
                new PooledRedisClientManager(
                    new string[] { redisConnectionString },
                    new string[] { redisConnectionString },
                    config);

            this.RequestBatchSize = RedisConnectionPool.DEFAULT_BATCH_SIZE;
            this.WindowMicroSec = RedisConnectionPool.DEFAULT_WINDOW_MICRO_SEC;
            this.Active = true;

            this.requestQueue = new RedisRequest[this.RequestBatchSize];
            this.currReqId = -1;

            this.spinLock = new SpinLock();
        }

        /// <summary>
        /// Get the client for the specify redis database, the redisDbIndex has been
        /// initilizated in the constructor
        /// </summary>
        /// <returns></returns>
        internal RedisClient GetRedisClient()
        {
            IRedisClient redisClient = this.redisManagerPool.GetClient();
            return (RedisClient) redisClient;
        }

        /// <summary>
        /// Enqueues an incoming Redis request to a queue. Queued requests are periodically sent to Redis.
        /// </summary>
        /// <param name="request">The incoming request</param>
        /// <returns>The index of the spot the request takes</returns>
        private void EnqueueRequest(RedisRequest request)
        {
            int reqId = -1;
            // Spinlock until an empty spot is available in the queue
            while (reqId < 0 || reqId >= this.RequestBatchSize)
            {
				reqId = this.currReqId + 1;
                if (reqId >= this.RequestBatchSize)
                {
                    continue;
                }
                else
                {
                    bool lockTaken = false;
                    try
                    {
                        this.spinLock.Enter(ref lockTaken);
						// No need to take interlocked since it already in the lock
						// reqId = Interlocked.Increment(ref this.currReqId);
						reqId = this.currReqId + 1;
						if (reqId < this.RequestBatchSize)
						{
							this.currReqId++;
							this.requestQueue[reqId] = request;
						}
                    }
                    finally
                    {
                        if (lockTaken)
                        {
                            this.spinLock.Exit();
                        }
                    }
                }
            }
        }

        private void Flush()
        {
            // Send queued requests to Redis, collect results and store each of them in the corresonding request
            using (RedisClient redisClient = this.GetRedisClient())
            {
                using (IRedisPipeline pipe = redisClient.CreatePipeline())
                {
                    for (int reqId = 0; reqId <= this.currReqId; reqId++)
                    {
                        RedisRequest req = this.requestQueue[reqId];
                        if (req == null)
                        {
                            continue;
                        }

                        switch(req.Type)
                        {
                            case RedisRequestType.HGet:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HGet(req.HashId, req.Key),
                                    req.SetValue, req.SetError);
                                break;
                            case RedisRequestType.HMGet:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HMGet(req.HashId, req.Keys),
                                    req.SetValues, req.SetError);
                                break;
                            case RedisRequestType.HGetAll:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HGetAll(req.HashId),
                                    req.SetValues, req.SetError);
                                break;
                            case RedisRequestType.HSetNX:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HSetNX(req.HashId, req.Key, req.Value),
                                    req.SetLong, req.SetError);
                                break;
                            case RedisRequestType.HSet:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HSet(req.HashId, req.Key, req.Value),
                                    req.SetLong, req.SetError);
                                break;
                            case RedisRequestType.HMSet:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HMSet(req.HashId, req.Keys, req.Values),
                                    req.SetVoid, req.SetError);
                                break;
                            case RedisRequestType.HDel:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HDel(req.HashId, req.Key), 
                                        req.SetLong, req.SetError);
                                break;
                            case RedisRequestType.EvalSha:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).EvalSha(req.Sha1, req.NumberKeysInArgs, req.Keys),
                                        req.SetValues, req.SetError);
                                break;
                            default:
                                break;
                        }
                    }

                    pipe.Flush();
                }

                // Release the request lock to make sure processRequest can keep going
                for (int reqId = 0; reqId <= this.currReqId; reqId++)
                {
                    // Monitor.Wait must be called in sync block, here we should lock the 
                    // request and release the it on time
                    lock (this.requestQueue[reqId])
                    {
                        System.Threading.Monitor.PulseAll(this.requestQueue[reqId]);
                    }
                }
            }

            this.currReqId = -1;
        }

        /// <summary>
        /// A daemon thread invokes the Monitor() method to monitor the request queue,  
        /// periodically flushes queued request to Redis, and get back results for each request.
        /// </summary>
        internal void Monitor()
        {
            long lastFlushTime = DateTime.Now.Ticks / 10;
            while (this.Active)
            {
                long now = DateTime.Now.Ticks / 10;
                if (now - lastFlushTime >= this.WindowMicroSec || 
                    this.currReqId + 1 >= this.RequestBatchSize)
                {
                    if (this.currReqId >= 0)
                    {
                        bool lockTaken = false;
                        try
                        {
                            this.spinLock.Enter(ref lockTaken);
                            this.Flush();
                        }
                        finally
                        {
                            if (lockTaken)
                            {
                                this.spinLock.Exit();
                            }
                        }
                    }
                    lastFlushTime = DateTime.Now.Ticks / 10;
                }
            }

            if (this.currReqId >= 0)
            {
                bool lockTaken = false;
                try
                {
                    this.spinLock.Enter(ref lockTaken);
                    this.Flush();
                }
                finally
                {
                    if (lockTaken)
                    {
                        this.spinLock.Exit();
                    } 
                }
            }
        }

        public void Dispose()
        {
            this.Active = false;
        }

        internal long ProcessLongRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            lock (redisRequest)
            {
                while (!redisRequest.Finished)
                {
                    System.Threading.Monitor.Wait(redisRequest);
                }
            }

            return (long)redisRequest.Result;
        }

        internal byte[][] ProcessValuesRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            lock (redisRequest)
            {
                while (!redisRequest.Finished)
                {
                    System.Threading.Monitor.Wait(redisRequest);
                }
            }

            return (byte[][])redisRequest.Result;
        }

        internal byte[] ProcessValueRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            lock (redisRequest)
            {
                while (!redisRequest.Finished)
                {
                    System.Threading.Monitor.Wait(redisRequest);
                }
            }
            return (byte[])redisRequest.Result;
        }

        internal void ProcessVoidRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            lock (redisRequest)
            {
                while (!redisRequest.Finished)
                {
                    System.Threading.Monitor.Wait(redisRequest);
                }
            }
        }

        internal byte[][] ProcessValueRequestInBatch(IEnumerable<RedisRequest> reqBatch)
        {
            RedisRequest lastRequest = null;
            int count = 0;
            foreach (RedisRequest req in reqBatch)
            {
                count++;
                lastRequest = req;
                this.EnqueueRequest(req);
            }

            // Since requests may be executed in different batch, we must enusre all
            // requests are finished by checking whether the last request is finished
            lock (lastRequest)
            {
                while (!lastRequest.Finished)
                {
                    System.Threading.Monitor.Wait(lastRequest);
                }
            }

            byte[][] values = new byte[count][];
            count = 0;
            foreach (RedisRequest req in reqBatch)
            {
                values[count++] = (byte[])req.Result;
            }
            return values;
        }
    }
}

