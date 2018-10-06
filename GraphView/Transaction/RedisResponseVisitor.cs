using System.Diagnostics;

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
            byte[][] returnBytes = req.Result as byte[][];
            Debug.Assert(returnBytes.IsSuccess());
            req.Result = returnBytes.IsSuccess();
        }

        private void PrintVersionKeys(long[] keys)
        {
            Console.WriteLine($"Read {keys.Length} key(s)");
            foreach (var key in keys)
            {
                Console.Write($"{key} ");
            }
            Console.WriteLine();
        }

        private int ExtractVersionEntry(
            byte[][] response, TxList<VersionEntry> dest)
        {
            Debug.Assert(response.Length <= 4 || response.Length % 2 == 0);
            int entryCount = response.Length / 2;
            long[] debugKeys = new long[entryCount];
            for (int i = 0; i < entryCount; ++i)
            {
                int versionKeyIndex = i * 2;
                long versionKey = BitConverter.ToInt64(
                    response[versionKeyIndex], 0);
                byte[] entryBytes = response[versionKeyIndex + 1];
                VersionEntry.Deserialize(
                    versionKey, entryBytes, dest[i]);
                debugKeys[i] = versionKey;
            }
            if (debugKeys.Length == 2)
                Debug.Assert(debugKeys[0] + 1 == debugKeys[1]);
            return entryCount;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            // if ( returnBytes == null || returnBytes.Length == 0)
            // {
            //     req.Result = 0;
            //     return;
            // }
            int entryCount = ExtractVersionEntry(
                returnBytes, req.LocalContainer);
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
                VersionEntry.Deserialize(
                    req.VersionKey,
                    returnBytes.ValueBytes(), req.LocalVerEntry);
        }

        internal override void Visit(ReadVersionRequest req)
        {
            byte[] valueBytes = req.Result as byte[];
            req.Result = valueBytes == null || valueBytes.Length == 0 ?
                null :
                VersionEntry.Deserialize(req.VersionKey, valueBytes, req.LocalVerEntry);
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length < 2 ?
                null :
                VersionEntry.Deserialize(
                    req.VersionKey,
                    returnBytes.ValueBytes(), req.LocalVerEntry);
        }

        internal override void Visit(UploadVersionRequest req)
        {
            // what is this line doing ???
            req.RemoteVerEntry = req.VersionEntry;

            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes.IsSuccess();
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
                -1L :
                BitConverter.ToInt64(returnBytes.ValueBytes(), 0);
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
                RedisVersionDb.REDIS_CALL_ERROR_CODE :
                BitConverter.ToInt64(returnBytes.ValueBytes(), 0);
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
