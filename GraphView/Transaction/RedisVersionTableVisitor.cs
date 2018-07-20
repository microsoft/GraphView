namespace GraphView.Transaction
{
    using System;
    using System.Text;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    internal class RedisVersionTableVisitor : VersionTableVisitor
    {
        internal static int LargestReqIndex = 0;

        /// <summary>
        /// The client pool to flush requests
        /// </summary>
        private readonly RedisConnectionPool clientPool;

        /// <summary>
        /// The lua script manager 
        /// </summary>
        private RedisLuaScriptManager redisLuaManager;

        /// <summary>
        /// The response visitor to handle returned redis requests
        /// </summary>
        private RedisResponseVisitor responseVisitor;

        /// <summary>
        /// The redis requests object to send to the redis server
        /// </summary>
        private readonly List<RedisRequest> redisRequests;

        /// <summary>
        /// The index of redis requests
        /// </summary>
        private int reqIndex;

        private RedisClient redisClient;

        public RedisVersionTableVisitor(
            RedisConnectionPool clientPool,
            RedisLuaScriptManager redisLuaManager,
            RedisResponseVisitor responseVisitor)
        {
            this.clientPool = clientPool;
            this.redisClient = clientPool.GetRedisClient();
            this.redisLuaManager = redisLuaManager;

            this.responseVisitor = responseVisitor;
            this.redisRequests = new List<RedisRequest>();
            this.reqIndex = 0;
        }

        public override void Invoke(Queue<VersionEntryRequest> reqQueue)
        {
            int reqCount = 0;
            while (reqQueue.Count > 0)
            {
                VersionEntryRequest req = reqQueue.Dequeue();
                req.Accept(this);
                reqCount++;
            }

            if (reqCount > 0)
            {
                this.clientPool.Flush(this.redisRequests, redisClient, reqCount);
            }
            LargestReqIndex = Math.Max(LargestReqIndex, this.reqIndex);
            this.reqIndex = 0;
        }

        private RedisRequest NextRedisRequest()
        {
            RedisRequest nextReq = null;

            while (reqIndex >= this.redisRequests.Count)
            {
                nextReq = new RedisRequest(this.responseVisitor);
                this.redisRequests.Add(nextReq);
            }

            nextReq = this.redisRequests[reqIndex];
            this.reqIndex++;

            return nextReq;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            string hashId = req.RecordKey.ToString();
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, keyBytes, RedisRequestType.HDel);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            string hashId = req.RecordKey.ToString();

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, RedisRequestType.HGetAll);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            string hashId = req.RecordKey.ToString();

            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry(req.RecordKey);
            byte[] keyBytes = BitConverter.GetBytes(emptyEntry.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(emptyEntry);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(ReadVersionRequest req)
        {
            string hashId = req.RecordKey.ToString();
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, keyBytes, RedisRequestType.HGet);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1("REPLACE_VERSION_ENTRY");
            string hashId = req.RecordKey.ToString();

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.BeginTs),
                BitConverter.GetBytes(req.EndTs),
                BitConverter.GetBytes(req.TxId),
                BitConverter.GetBytes(req.ReadTxId),
                BitConverter.GetBytes(req.ExpectedEndTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            string hashId = req.RecordKey.ToString();
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(req.VersionEntry);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, keyBytes, valueBytes, RedisRequestType.HSetNX);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1("UPDATE_VERSION_MAX_COMMIT_TS");
            string hashId = req.RecordKey.ToString();

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.MaxCommitTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
            redisReq.ParentRequest = req;
        }
    }
}
