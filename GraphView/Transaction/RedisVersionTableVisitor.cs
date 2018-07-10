namespace GraphView.Transaction
{
    using System;
    using System.Text;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    public class RedisVersionTableVisitor : VersionTableVisitor
    {
        /// <summary>
        /// The client pool to flush requests
        /// </summary>
        private readonly RedisConnectionPool clientPool;

        /// <summary>
        /// 
        /// </summary>
        private RedisLuaScriptManager redisLuaManager;

        private readonly List<RedisRequest> redisRequests;
        private readonly IRedisPipeline pipe;
        private int reqIndex;

        public RedisVersionTableVisitor(
            RedisConnectionPool clientPool, RedisLuaScriptManager redisLuaManager)
        {
            this.clientPool = clientPool;
            this.redisLuaManager = redisLuaManager;

            this.redisRequests = new List<RedisRequest>();
            this.reqIndex = 0;
        }

        public override void Invoke(IEnumerable<VersionEntryRequest> reqs)
        {
            this.reqIndex = 0;

            foreach (VersionEntryRequest req in reqs)
            {
                clientPool.EnqueueVersionEntryRequest(req);
            }
            clientPool.Visit();
        }

        private RedisRequest NextRedisRequest()
        {
            RedisRequest nextReq = null;

            while (reqIndex >= this.redisRequests.Count)
            {
                nextReq = new RedisRequest();
                this.redisRequests.Add(nextReq);
            }

            nextReq = this.redisRequests[reqIndex];
            this.reqIndex++;

            return nextReq;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            string hashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HDel(hashId, keyBytes), 
                redisReq.SetLong, redisReq.SetError);
        }

        internal override void Visit(GetVersionListRequest req)
        {
            string hashId = req.RecordKey as string;

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HGetAll(hashId), 
                redisReq.SetValues, redisReq.SetError);
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            string hashId = req.RecordKey as string;

            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry(req.RecordKey);
            byte[] keyBytes = BitConverter.GetBytes(emptyEntry.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(emptyEntry);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HSetNX(hashId, keyBytes, valueBytes), 
                redisReq.SetLong, redisReq.SetError);
        }

        internal override void Visit(ReadVersionRequest req)
        {
            string hashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(hashId, keyBytes), 
                redisReq.SetValue, redisReq.SetError);
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1("REPLACE_VERSION_ENTRY");
            string hashId = req.RecordKey as string;

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
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).EvalSha(hashId, 1, keysAndArgs),
                redisReq.SetValues, redisReq.SetError);
        }

        internal override void Visit(UploadVersionRequest req)
        {
            string hashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(req.VersionEntry);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HSetNX(hashId, keyBytes, valueBytes),
                redisReq.SetLong, redisReq.SetError);
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1("UPDATE_VERSION_MAX_COMMIT_TS");
            string hashId = req.RecordKey as string;

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.MaxCommitTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).EvalSha(hashId, 1, keysAndArgs),
                redisReq.SetValues, redisReq.SetError);
        }
    }
}
