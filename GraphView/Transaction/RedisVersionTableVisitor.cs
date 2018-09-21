namespace GraphView.Transaction
{
    using System;
    using System.Text;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;

    internal class RedisVersionTableVisitor : VersionTableVisitor
    {
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

        private RedisVersionDbMode redisVersionDbMode;

        public RedisVersionTableVisitor(
            RedisConnectionPool clientPool,
            RedisLuaScriptManager redisLuaManager,
            RedisResponseVisitor responseVisitor,
            RedisVersionDbMode mode)
        {
            this.clientPool = clientPool;
            this.redisClient = clientPool.GetRedisClient();
            this.redisLuaManager = redisLuaManager;

            this.redisVersionDbMode = mode;
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

        private RedisRequest CreateLuaRequest(
            LuaScriptName script, byte[][] args)
        {
            string sha1 = redisLuaManager.GetLuaScriptSha1(script);
            RedisRequest result = NextRedisRequest();
            result.Set(args, sha1, 1, RedisRequestType.EvalSha);
            return result;
        }

        private string GetHashKey(VersionEntryRequest request)
        {
            string hashId = request.RecordKey.ToString();
            return redisVersionDbMode == RedisVersionDbMode.Cluster
                ? RedisVersionDb.PACK_KEY(RedisVersionDb.VER_KEY_PREFIX, hashId)
                : hashId;
        }


        internal override void Visit(DeleteVersionRequest request)
        {
            string hashId = GetHashKey(request);
            byte[][] args = {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(request.VersionKey)
            };
            var redisRequest = CreateLuaRequest(
                LuaScriptName.DELETE_DIRTY_VERSION, args);
            redisRequest.ParentRequest = request;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            string hashKey = GetHashKey(req);
            byte[][] args = { Encoding.ASCII.GetBytes(hashKey) };
            var redisRequest = CreateLuaRequest(
                LuaScriptName.GET_VERSION_LIST, args);
            redisRequest.ParentRequest = req;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            string hashKey = GetHashKey(req);

            var redisRequest = this.NextRedisRequest();
            redisRequest.Set(
                hashKey,
                RedisVersionDb.LATEST_VERSION_PTR_FIELD,
                RedisVersionDb.EMPTY_BYTES,
                RedisRequestType.HSetNX);

            redisRequest.ParentRequest = req;
        }

        internal override void Visit(ReadVersionRequest req)
        {
            string hashId = req.RecordKey.ToString();
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                hashId = RedisVersionDb.PACK_KEY(RedisVersionDb.VER_KEY_PREFIX, hashId);
            }

            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(hashId, keyBytes, RedisRequestType.HGet);
            redisReq.ParentRequest = req;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1(LuaScriptName.REPLACE_VERSION_ENTRY);
            string hashId = req.RecordKey.ToString();
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                hashId = RedisVersionDb.PACK_KEY(RedisVersionDb.VER_KEY_PREFIX, hashId);
            }

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(hashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.BeginTs),
                BitConverter.GetBytes(req.EndTs),
                BitConverter.GetBytes(req.TxId),
                BitConverter.GetBytes(req.SenderId),
                BitConverter.GetBytes(req.ExpectedEndTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            RedisRequest redisReq = this.NextRedisRequest();
            redisReq.Set(keysAndArgs, sha1, 1, RedisRequestType.EvalSha);
            redisReq.ParentRequest = req;
        }


        internal override void Visit(UploadVersionRequest req)
        {
            string hashKey = GetHashKey(req);

            byte[][] args = {
                Encoding.ASCII.GetBytes(hashKey),
                BitConverter.GetBytes(req.VersionKey),
                VersionEntry.Serialize(req.VersionEntry)
            };

            var redisRequest = CreateLuaRequest(
                LuaScriptName.UPLOAD_NEW_VERSION, args);
            redisRequest.ParentRequest = req;
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            string sha1 = this.redisLuaManager.GetLuaScriptSha1(LuaScriptName.UPDATE_VERSION_MAX_COMMIT_TS);
            string hashId = req.RecordKey.ToString();
            if (this.redisVersionDbMode == RedisVersionDbMode.Cluster)
            {
                hashId = RedisVersionDb.PACK_KEY(RedisVersionDb.VER_KEY_PREFIX, hashId);
            }

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
