
namespace GraphView.Transaction
{
    using System;
    using System.Text;

    internal class RedisTxEntryRequestVisitor : TxEntryVisitor
    {
        internal RedisRequest RedisReq { get; private set; }
        internal string HashId { get; private set; }
        private RedisLuaScriptManager redisLuaScriptManager;

        public RedisTxEntryRequestVisitor(RedisLuaScriptManager redisLuaScriptManager)
        {
            this.redisLuaScriptManager = redisLuaScriptManager;
        }

        internal RedisRequest Invoke(TxEntryRequest txReq)
        {
            txReq.Accept(this);
            return RedisReq;
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            this.HashId = req.TxId.ToString();
            byte[][] keyBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };
            this.RedisReq = new RedisRequest(this.HashId, keyBytes, RedisRequestType.HMGet)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            TxTableEntry txTableEntry = new TxTableEntry(req.TxId);
            this.HashId = req.TxId.ToString();
            byte[][] keysBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };
            byte[][] valuesBytes =
            {
                BitConverter.GetBytes((int) txTableEntry.Status),
                BitConverter.GetBytes(txTableEntry.CommitTime),
                BitConverter.GetBytes(txTableEntry.CommitLowerBound)
            };

            this.RedisReq = new RedisRequest(this.HashId, keysBytes, valuesBytes, RedisRequestType.HMSet)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(NewTxIdRequest req)
        {
            this.HashId = req.TxId.ToString();
            byte[] keyBytes = Encoding.ASCII.GetBytes(TxTableEntry.TXID_STRING);
            byte[] valueBytes = BitConverter.GetBytes(req.TxId);

            this.RedisReq = new RedisRequest(this.HashId, keyBytes, valueBytes, RedisRequestType.HSetNX)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(RecycleTxRequest req)
        {
            this.HashId = req.TxId.ToString();
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

            this.RedisReq = new RedisRequest(this.HashId, keysBytes, valuesBytes, RedisRequestType.HMSet)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(RemoveTxRequest req)
        {
            this.HashId = req.TxId.ToString();
            byte[][] keysBytes =
            {
                Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_TIME_STRING),
                Encoding.ASCII.GetBytes(TxTableEntry.COMMIT_LOWER_BOUND_STRING)
            };

            this.RedisReq = new RedisRequest(this.HashId, keysBytes, RedisRequestType.HDel)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            this.HashId = req.TxId.ToString();
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1(LuaScriptName.SET_AND_GET_COMMIT_TIME);
            byte[][] keys =
            {
                Encoding.ASCII.GetBytes(this.HashId),
                BitConverter.GetBytes(req.ProposedCommitTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            this.RedisReq = new RedisRequest(keys, sha1, 1, RedisRequestType.EvalSha)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(TxEntryRequest req)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            this.HashId = req.TxId.ToString();
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1(LuaScriptName.UPDATE_COMMIT_LOWER_BOUND);
            byte[][] keys =
            {
                Encoding.ASCII.GetBytes(this.HashId),
                BitConverter.GetBytes(req.CommitTsLowerBound),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
                RedisVersionDb.NEGATIVE_TWO_BYTES,
            };

            this.RedisReq = new RedisRequest(keys, sha1, 1, RedisRequestType.EvalSha)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            this.HashId = req.TxId.ToString();
            byte[] keyBytes = Encoding.ASCII.GetBytes(TxTableEntry.STATUS_STRING);
            byte[] valueBytes = BitConverter.GetBytes((int)req.TxStatus);

            this.RedisReq = new RedisRequest(this.HashId, keyBytes, valueBytes, RedisRequestType.HSet)
            {
                ParentRequest = req
            };
        }
    }

    /// <summary>
    /// The visitor that translates a tx request to the corresonding Redis request.
    /// </summary>
    internal class RedisVersionEntryRequestVisitor : VersionEntryVisitor
    {
        internal RedisRequest RedisReq { get; private set; }
        internal string HashId { get; private set; }
        private RedisLuaScriptManager redisLuaScriptManager;

        internal RedisRequest Invoke(VersionEntryRequest txReq)
        {
            txReq.Accept(this);
            return RedisReq;
        }

        internal RedisVersionEntryRequestVisitor(RedisLuaScriptManager luaScriptManager)
        {
            this.redisLuaScriptManager = luaScriptManager;
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            this.HashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            this.RedisReq = new RedisRequest(this.HashId, keyBytes, RedisRequestType.HDel)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(GetVersionListRequest req)
        {
            this.HashId = req.RecordKey as string;
            this.RedisReq = new RedisRequest(this.HashId, RedisRequestType.HGetAll)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            this.HashId = req.RecordKey as string;
            long versionKey = VersionEntry.VERSION_KEY_START_INDEX;
            VersionEntry emptyEntry = new VersionEntry(
                versionKey,
                VersionEntry.EMPTY_RECORD,
                VersionEntry.EMPTY_TXID);

            byte[] keyBytes = BitConverter.GetBytes(versionKey);
            byte[] valueBytes = VersionEntry.Serialize(emptyEntry);

            this.RedisReq = new RedisRequest(this.HashId, keyBytes, valueBytes, RedisRequestType.HSetNX)
            {
                ParentRequest = req
            };
        }
        
        internal override void Visit(ReadVersionRequest req)
        {
            this.HashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            this.RedisReq = new RedisRequest(this.HashId, keyBytes, RedisRequestType.HGet)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1(LuaScriptName.REPLACE_VERSION_ENTRY);
            this.HashId = req.RecordKey as string;

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(this.HashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.BeginTs),
                BitConverter.GetBytes(req.EndTs),
                BitConverter.GetBytes(req.TxId),
                BitConverter.GetBytes(req.SenderId),
                BitConverter.GetBytes(req.ExpectedEndTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            this.RedisReq = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            this.HashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(req.VersionEntry);

            this.RedisReq = new RedisRequest(this.HashId, keyBytes, valueBytes, RedisRequestType.HSet)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            string sha1 = this.redisLuaScriptManager.GetLuaScriptSha1(LuaScriptName.UPDATE_VERSION_MAX_COMMIT_TS);
            this.HashId = req.RecordKey as string;

            byte[][] keysAndArgs =
            {
                Encoding.ASCII.GetBytes(this.HashId),
                BitConverter.GetBytes(req.VersionKey),
                BitConverter.GetBytes(req.MaxCommitTs),
                RedisVersionDb.NEGATIVE_ONE_BYTES,
            };

            this.RedisReq = new RedisRequest(keysAndArgs, sha1, 1, RedisRequestType.EvalSha)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(UploadVersionRequest req)
        {
            this.HashId = req.RecordKey as string;
            byte[] keyBytes = BitConverter.GetBytes(req.VersionKey);
            byte[] valueBytes = VersionEntry.Serialize(req.VersionEntry);

            this.RedisReq = new RedisRequest(this.HashId, keyBytes, valueBytes, RedisRequestType.HSetNX)
            {
                ParentRequest = req
            };
        }

        internal override void Visit(VersionEntryRequest req)
        {
            throw new NotImplementedException();
        }
    }
}
