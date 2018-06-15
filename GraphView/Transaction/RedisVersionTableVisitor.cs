namespace GraphView.Transaction
{
    using System;
    using System.Text;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    public class RedisVersionTableVisitor : VersionTableVisitor
    {
        private readonly RedisConnectionPool clientPool;
        private readonly List<RedisRequest> redisRequests;
        private readonly IRedisPipeline pipe;
        private int reqIndex;

        public RedisVersionTableVisitor(RedisConnectionPool clientPool)
        {
            this.clientPool = clientPool;
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
            long versionKey = VersionEntry.VERSION_KEY_STRAT_INDEX;

            VersionEntry emptyEntry = new VersionEntry(
                req.RecordKey,
                versionKey,
                VersionEntry.EMPTY_RECORD,
                VersionEntry.EMPTY_TXID);
            byte[] keyBytes = BitConverter.GetBytes(versionKey);
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
            string sha1 = null; // this.clientPool.redisLuaScriptManager.GetLuaScriptSha1("REPLACE_VERSION_ENTRY");
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

            redisReq = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha)
            {
                ParentRequest = req
            };
        }
    }
}
