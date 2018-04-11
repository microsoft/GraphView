namespace GraphView.Transaction
{
    using System;
    using System.Threading;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    /// <summary>
    /// A singleton redis client manager to get redis client based on RedisManagerPool
    /// Can extract IRedisClient and IRedisNativeClient from this redis client manager 
    /// </summary>
    internal class RedisClientManager : IDisposable
    { 
        private static readonly object initLock = new object();
        private static IRedisClientsManager redisManagerPool;

        private static readonly int requestBatchSize = 10000;
        private static readonly long windowMicroSec = 100;      // 100 micro sec = 0.1 milli sec
        internal static RedisRequest[] requestQueue = null;
        internal static int currReqId;
        private static bool active;

        internal static IRedisClientsManager Instance
        {
            get
            {
                if (RedisClientManager.redisManagerPool == null)
                {
                    lock (RedisClientManager.initLock)
                    {
                        if (RedisClientManager.redisManagerPool == null)
                        {
                            RedisClientManagerConfig config = new RedisClientManagerConfig();
                            config.MaxReadPoolSize = 1000;
                            config.MaxWritePoolSize = 1000;
                            // TODO: read redis config from config files
                            string redisConnectionString = "127.0.0.1:6379";
                            RedisClientManager.redisManagerPool = 
                                new PooledRedisClientManager(
                                    new string[] { redisConnectionString },
                                    new string[] { redisConnectionString},
                                    config);

                            RedisClientManager.requestQueue = new RedisRequest[RedisClientManager.requestBatchSize];
                            RedisClientManager.currReqId = -1;
                            RedisClientManager.active = true;
                        }
                    }
                }

                return RedisClientManager.redisManagerPool;
            }
        }

        internal RedisClient GetRedisClient()
        {
            IRedisClient redisClient = RedisClientManager.redisManagerPool.GetClient();
            return (RedisClient)redisClient;
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
            while (reqId < 0 || reqId >= RedisClientManager.requestBatchSize)
            {
                if (reqId >= RedisClientManager.requestBatchSize)
                {
                    continue;
                }
                else
                {
                    reqId = Interlocked.Increment(ref RedisClientManager.currReqId);
                }
            }

            RedisClientManager.requestQueue[reqId] = request;
        }

        private void Flush()
        {
            // Send queued requests to Redis, collect results and store each of them in the corresonding request
            using (RedisClient redisClient = this.GetRedisClient())
            {
                using (IRedisPipeline pipe = redisClient.CreatePipeline())
                {
                    for (int reqId = 0; reqId <= RedisClientManager.currReqId; reqId++)
                    {
                        RedisRequest req = RedisClientManager.requestQueue[reqId];
                        if (req == null)
                        {
                            continue;
                        }

                        switch(req.Type)
                        {
                            case RedisRequestType.NewTx1:
                                pipe.QueueCommand(                              
                                    r => ((RedisNativeClient)r).HSetNX(req.HashId, new byte[0], new byte[0]), 
                                    req.SetLong);
                                break;
                            case RedisRequestType.GetTxEntry:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HMGet(req.HashId, req.Keys), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.UpdateTxStatus:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HSet(req.HashId, req.Key, req.Value), 
                                    req.SetLong);
                                break;
                            case RedisRequestType.SetCommitTs:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).EvalSha(req.Sha, 1, req.Keys), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.UpdateCommitLowerBound:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).EvalSha(req.Sha, 1, req.Keys), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.GetVersionList:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HGetAll(req.HashId), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.InitiGetVersionList:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HSetNX(req.HashId, req.Key, req.Value), 
                                    req.SetLong);
                                break;
                            case RedisRequestType.ReplaceVersion:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).EvalSha(req.Sha, 1, req.Keys), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.UploadVersion:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HSetNX(req.HashId, req.Key, req.Value), 
                                    req.SetLong);
                                break;
                            case RedisRequestType.UpdateVersionMaxTs:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).EvalSha(req.Sha, 1, req.Keys), 
                                    req.SetValues);
                                break;
                            case RedisRequestType.DeleteVersion:
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).HDel(req.HashId, req.Key), 
                                    req.SetLong);
                                break;
                            default:
                                break;
                        }
                    }

                    pipe.Flush();
                }
            }

            RedisClientManager.currReqId = -1;
        }

        /// <summary>
        /// A daemon thread invokes the Monitor() method to monitor the request queue,  
        /// periodically flushes queued request to Redis, and get back results for each request.
        /// </summary>
        internal void Monitor()
        {
            long lastFlushTime = DateTime.Now.Ticks / 10;
            while (RedisClientManager.active)
            {
                long now = DateTime.Now.Ticks / 10;
                if (now - lastFlushTime >= RedisClientManager.windowMicroSec || 
                    RedisClientManager.currReqId >= RedisClientManager.requestBatchSize)
                {
                    this.Flush();
                    lastFlushTime = DateTime.Now.Ticks / 10;
                }
            }

            if (RedisClientManager.currReqId >= 0)
            {
                this.Flush();
            }
        }

        public void Dispose()
        {
            RedisClientManager.active = false;
        }

        internal long ProcessLongRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            while (redisRequest.Result == null) { }

            return (long)redisRequest.Result;
        }

        internal byte[][] ProcessValuesRequest(RedisRequest redisRequest)
        {
            this.EnqueueRequest(redisRequest);
            while (redisRequest.Result == null) { }

            return (byte[][])redisRequest.Result;
        }
    }
}
