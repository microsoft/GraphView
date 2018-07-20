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
    public class RedisConnectionPool
    {
        // THOSE TWO VARIABLES ARE ONLY FOR BENCHMARK TEST
        /// <summary>
        /// The number of times it has flushed
        /// </summary>
        internal static int FLUSH_TIMES = 0;

        /// <summary>
        /// the times flushed since the number of reqeusts in queue is
        /// up to the batch size
        /// </summary>
        internal static int FLUSH_TIMES_UPTO_BATCH = 0;

        public static readonly int DEFAULT_BATCH_SIZE = 100;

        /// <summary>
        /// The timeout to flush is 1ms (10000Ticks)
        /// </summary>
        public static readonly long DEFAULT_WINDOW_MICRO_SEC = 10000L;

        public static readonly int DEFAULT_MAX_READ_POOL_SIZE = 64;

        public static readonly int DEFAULT_MAX_WRITE_POOL_SIZE = 64;
        /// <summary>
        /// The real pool manager maintaining clients pool
        /// Which is from ServiceStack.Redis
        /// </summary>
        private IRedisClientsManager redisManagerPool;

        /// <summary>
        /// Request queue holding the pending Redis requests
        /// </summary>
        private Queue<RedisRequest> redisRequestQueue;

        private readonly RedisTxEntryRequestVisitor txEntryVisitor;
        private readonly RedisVersionEntryRequestVisitor versionEntryVisitor;
        private readonly RedisResponseVisitor redisResponseVisitor;

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

        private long lastFlushTime;

        public RedisConnectionPool(string redisConnectionString, long database, RedisLuaScriptManager luaScriptManager)
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

            this.redisRequestQueue = new Queue<RedisRequest>(this.RequestBatchSize);

            this.redisResponseVisitor = new RedisResponseVisitor();
            this.txEntryVisitor = new RedisTxEntryRequestVisitor(luaScriptManager);
            this.versionEntryVisitor = new RedisVersionEntryRequestVisitor(luaScriptManager);

            this.spinLock = new SpinLock();

            lastFlushTime = DateTime.Now.Ticks / 10;
        }

        public RedisConnectionPool(string redisConnectionString, long database) :
            this(redisConnectionString, database, null)
        {
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
        private void EnqueueRedisRequest(RedisRequest request)
        {
            bool lockTaken = false;
            try
            {
                this.spinLock.Enter(ref lockTaken);
                this.redisRequestQueue.Enqueue(request);
            }
            finally
            {
                if (lockTaken)
                {
                    this.spinLock.Exit();
                }
            }
        }

        internal void Flush(IEnumerable<RedisRequest> requests, RedisClient redisClient = null, int maxRequests = -1)
        {
            int reqCount = 0;
            using (IRedisPipeline pipe = redisClient.CreatePipeline())
            {
                foreach (RedisRequest req in requests)
                {
                    reqCount++;
                    if (req == null)
                    {
                        continue;
                    }

                    switch (req.Type)
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
                            // delete a single field
                            if (req.Key != null)
                            {
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HDel(req.HashId, req.Key),
                                        req.SetLong, req.SetError);
                            }
                            // delete multiple fields
                            else
                            {
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HDel(req.HashId, req.Keys),
                                        req.SetLong, req.SetError);
                            }
                            break;
                        case RedisRequestType.EvalSha:
                            pipe.QueueCommand(
                                r => ((RedisNativeClient)r).EvalSha(req.Sha1, req.NumberKeysInArgs, req.Keys),
                                    req.SetValues, req.SetError);
                            break;
                        default:
                            break;
                    }

                    if (maxRequests != -1 && reqCount >= maxRequests)
                    {
                        break;
                    }
                }
                pipe.Flush();
            }
        }

        internal void EnqueueTxEntryRequest(TxEntryRequest req)
        {
            this.txEntryVisitor.Invoke(req);
            RedisRequest redisReq = this.txEntryVisitor.RedisReq;
            redisReq.ResponseVisitor = this.redisResponseVisitor;

            this.redisRequestQueue.Enqueue(redisReq);
        }

        internal void EnqueueVersionEntryRequest(VersionEntryRequest req)
        {
            this.versionEntryVisitor.Invoke(req);
            RedisRequest redisReq = this.versionEntryVisitor.RedisReq;
            redisReq.ResponseVisitor = this.redisResponseVisitor;

            this.redisRequestQueue.Enqueue(redisReq);
        }

        internal void Visit()
        {
            this.Flush(this.redisRequestQueue);
            this.redisRequestQueue.Clear();
        }

        internal void VisitRedisRequestQueue()
        {
            long now = DateTime.Now.Ticks / 10;
            if (now - lastFlushTime >= this.WindowMicroSec ||
                this.redisRequestQueue.Count >= this.RequestBatchSize)
            {
                if (this.redisRequestQueue.Count > 0)
                {
                    // ONLY FOR BENCHMARK TEST
                    Interlocked.Increment(ref RedisConnectionPool.FLUSH_TIMES);
                    if (this.redisRequestQueue.Count >= this.RequestBatchSize)
                    {
                        Interlocked.Increment(ref RedisConnectionPool.FLUSH_TIMES_UPTO_BATCH);
                    }

                    bool lockTaken = false;
                    RedisRequest[] flushReqs = null;
                    try
                    {
                        this.spinLock.Enter(ref lockTaken);
                        // Copy a batch of elements to an array and clear the request queue, then release the lock.
                        // It reduces the time holding the lock and let requests enqueue timely
                        int flushCount = Math.Min(this.RequestBatchSize, this.redisRequestQueue.Count);
                        if (flushCount > 0)
                        {
                            flushReqs = new RedisRequest[flushCount];
                            for (int i = 0; i < flushCount; i++)
                            {
                                flushReqs[i] = this.redisRequestQueue.Dequeue();
                            }
                        }
                    }
                    finally
                    {
                        if (lockTaken)
                        {
                            this.spinLock.Exit();
                        }
                        if (flushReqs != null)
                        {
                            this.Flush(flushReqs);
                        }
                    }
                    
                }
                lastFlushTime = DateTime.Now.Ticks / 10;
            }
        }

        public void Dispose()
        {
            this.Active = false;
        }

        internal long ProcessLongRequest(RedisRequest redisRequest)
        {
            this.EnqueueRedisRequest(redisRequest);
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
            this.EnqueueRedisRequest(redisRequest);
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
            this.EnqueueRedisRequest(redisRequest);
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
            this.EnqueueRedisRequest(redisRequest);
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
                this.EnqueueRedisRequest(req);
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

