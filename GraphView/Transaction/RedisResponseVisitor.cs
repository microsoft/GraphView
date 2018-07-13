
namespace GraphView.Transaction
{
    using System;

    /// <summary>
    /// The visitor that interprets the data returned by Redis 
    /// to the expected type of each tx request.
    /// </summary>
    internal class RedisResponseVisitor : TxRequestVisitor
    {
        internal void Invoke(TxRequest req, object result)
        {
            req.Result = result;
            req.Accept(this);
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 1L;
            }
            catch (Exception)
            {
                req.Result = false;
            } 
        }

        internal override void Visit(GetVersionListRequest req)
        {
            TxList<VersionEntry> versionList = req.LocalContainer;

            byte[][] returnBytes = req.Result as byte[][];
            if (returnBytes == null || returnBytes.Length == 0)
            {
                req.Result = 0;
                return;
            }

            // First scan to find the largest version key
            long largestVersionKey = -1L;
            for (int i = 0; i < returnBytes.Length; i += 2)
            {
                long versionKey = BitConverter.ToInt64(returnBytes[i], 0);
                largestVersionKey = Math.Max(largestVersionKey, versionKey);
            }

            // second scan to find those two version entries
            int entryCount = 0;
            for (int i = 0; i < returnBytes.Length; i += 2)
            {
                long versionKey = BitConverter.ToInt64(returnBytes[i], 0);
                if (versionKey >= 0L && (versionKey == largestVersionKey || versionKey == largestVersionKey - 1))
                {
                    VersionEntry entry = versionList[entryCount];
                    VersionEntry.Deserialize(req.RecordKey, versionKey, returnBytes[i + 1], entry);
                    entryCount++;
                }
            }
      
            req.Result = entryCount;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 1L;
            }
            catch (Exception)
            {
                req.Result = false;
            }
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length == 0 ?
                null :
                VersionEntry.Deserialize(req.RecordKey, req.VersionKey, returnBytes[1], req.LocalVerEntry);
        }

        internal override void Visit(ReadVersionRequest req)
        {
            byte[] valueBytes = req.Result as byte[];
            req.Result = valueBytes == null || valueBytes.Length == 0 ?
                null :
                VersionEntry.Deserialize(req.RecordKey, req.VersionKey, valueBytes, req.LocalVerEntry);
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length < 2 ?
                null :
                VersionEntry.Deserialize(req.RecordKey, req.VersionKey, returnBytes[1], req.LocalVerEntry);
        }

        internal override void Visit(UploadVersionRequest req)
        {
            req.RemoteVerEntry = req.VersionEntry;
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 1L;
            }
            catch (Exception)
            {
                req.Result = false;
            }
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 1L;
            }
            catch (Exception)
            {
                req.Result = false;
            }
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            byte[][] valueBytes = req.Result as byte[][];

            if (valueBytes == null || valueBytes.Length == 0)
            {
                req.Result = null;
            }
            else
            {
                TxTableEntry txEntry = req.LocalTxEntry;
                txEntry.Set(
                    req.TxId,
                    (TxStatus)BitConverter.ToInt32(valueBytes[0], 0),
                    BitConverter.ToInt64(valueBytes[1], 0),
                    BitConverter.ToInt64(valueBytes[2], 0));
                req.Result = txEntry;
            }
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            // There is no response for HMSET command
            // Nothing to do here
        }

        internal override void Visit(NewTxIdRequest req)
        {
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 1L;
            }
            catch (Exception)
            {
                req.Result = false;
            }
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];

            req.Result = returnBytes == null || returnBytes.Length == 0 ?
                -1L:
                BitConverter.ToInt64(returnBytes[1], 0);
        }

        internal override void Visit(RecycleTxRequest req)
        {
            // There is no response from HMSet, always set the result as successful
            req.Result = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            byte[] returnBytes = req.Result as byte[];
            req.Result = returnBytes == null ? false : BitConverter.ToInt64(returnBytes, 0) == 1L;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length == 0 ?
                RedisVersionDb.REDIS_CALL_ERROR_CODE:
                BitConverter.ToInt64(returnBytes[1], 0);
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            try
            {
                long ret = (long)req.Result;
                req.Result = ret == 0L;
            }
            catch (Exception)
            {
                req.Result = false;
            }
        }
    }
}
