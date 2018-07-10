namespace GraphView.Transaction
{
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Text;

    public class RedisVersionDbVisitor : VersionDbVisitor
    {
        /// <summary>
        /// The client pool to flush requests
        /// </summary>
        private readonly RedisConnectionPool clientPool;

        /// <summary>
        /// The redis lua script manager to get sha1 of CAS
        /// </summary>
        private readonly RedisLuaScriptManager redisLuaScriptManager;

        private readonly IRedisPipeline pipe;

        private int reqIndex;

        private List<RedisRequest> redisRequests;

        public RedisVersionDbVisitor(
            RedisConnectionPool clientPool, 
            RedisLuaScriptManager redisLuaScriptManager)
        {
            this.clientPool = clientPool;
            this.redisLuaScriptManager = redisLuaScriptManager;

            this.redisRequests = new List<RedisRequest>();
            this.reqIndex = 0;
        }

        public override void Invoke(IEnumerable<TxEntryRequest> reqs)
        {
            foreach (TxEntryRequest req in reqs)
            {
                clientPool.EnqueueTxEntryRequest(req);
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

        internal override void Visit(GetTxEntryRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[][] keyBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HMGet(hashId, keyBytes),
                redisReq.SetValues, redisReq.SetError);
        }

        internal override void Visit(RecycleTxRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[][] keysBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.TXID_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };
            byte[][] valuesBytes =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes((int) TxStatus.Ongoing),
                BitConverter.GetBytes(TxTableEntry.DEFAULT_COMMIT_TIME),
                BitConverter.GetBytes(TxTableEntry.DEFAULT_LOWER_BOUND)
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HMSet(hashId, keysBytes, valuesBytes),
                redisReq.SetVoid, redisReq.SetError);
        }

        internal override void Visit(NewTxIdRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[] keyBytes = Encoding.ASCII.GetBytes(TxTableEntry.TXID_STRING);
            byte[] valueBytes = BitConverter.GetBytes(req.TxId);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HSetNX(hashId, keyBytes, valueBytes),
                redisReq.SetLong, redisReq.SetError);
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[][] keysBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };
            byte[][] valuesBytes =
            {
                BitConverter.GetBytes((int) TxStatus.Ongoing),
                BitConverter.GetBytes(TxTableEntry.DEFAULT_COMMIT_TIME),
                BitConverter.GetBytes(TxTableEntry.DEFAULT_LOWER_BOUND)
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HMSet(hashId, keysBytes, valuesBytes),
                redisReq.SetVoid, redisReq.SetError);
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            string hashId = req.TxId.ToString();
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1("SET_AND_GET_COMMIT_TIME");
            byte[][] keys =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.ProposedCommitTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).EvalSha(hashId, 1, keys),
               redisReq.SetValues, redisReq.SetError);
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[] keyBytes = Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING);
            byte[] valueBytes = BitConverter.GetBytes((int)req.TxStatus);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(hashId, keyBytes, valueBytes),
                redisReq.SetLong, redisReq.SetError);
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            string hashId = req.TxId.ToString();
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1("UPDATE_COMMIT_LOWER_BOUND");
            byte[][] keys =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.CommitTsLowerBound),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
                RedisVersionDb.NEGATIVE_TWO_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).EvalSha(sha1, 1, keys),
                redisReq.SetValues, redisReq.SetError);
        }

        internal override void Visit(RemoveTxRequest req)
        {
            string hashId = req.TxId.ToString();
            byte[][] keysBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.TXID_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.ParentRequest = req;

            pipe.QueueCommand(r => ((RedisNativeClient)r).HDel(hashId, keysBytes),
                redisReq.SetLong, redisReq.SetError);
        }
    }
}
