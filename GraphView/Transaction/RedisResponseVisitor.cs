
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

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
                req.Result = (long)req.Result;
            }
            catch (Exception)
            {
                req.Result = (long)0;
            } 
        }

        internal override void Visit(GetVersionListRequest req)
        {
            List<VersionEntry> versionList = new List<VersionEntry>();

            byte[][] returnBytes = req.Result as byte[][];
            if (returnBytes != null)
            {
                for (int i = 0; i < returnBytes.Length; i += 2)
                {
                    long versionKey = BitConverter.ToInt64(returnBytes[i], 0);
                    VersionEntry entry = VersionEntry.Deserialize(req.RecordKey, versionKey, returnBytes[i + 1]);
                    versionList.Add(entry);
                }
            }

            req.Result = versionList;
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
                req.Result = new TxTableEntry(
                    req.TxId,
                    (TxStatus)BitConverter.ToInt32(valueBytes[0], 0),
                    BitConverter.ToInt64(valueBytes[1], 0),
                    BitConverter.ToInt64(valueBytes[2], 0));
            }
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            try
            {
                req.Result = (long)req.Result;
            }
            catch (Exception)
            {
                req.Result = (long)0;
            }
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            try
            {
                if ((long)req.Result > 0)
                {
                    req.Result = req.TxId;
                }
                else
                {
                    req.Result = (long)-1;
                }
            }
            catch (Exception)
            {
                req.Result = -1L;
            }
        }

        internal override void Visit(NewTxIdRequest req)
        {
            try
            {
                req.Result = (long)req.Result;
            }
            catch (Exception)
            {
                req.Result = 0L;
            } 
        }

        internal override void Visit(ReadVersionRequest req)
        {
            byte[] valueBytes = req.Result as byte[];
            req.Result = valueBytes == null || valueBytes.Length == 0 ?
                    null :
                    VersionEntry.Deserialize(req.RecordKey, req.VersionKey, valueBytes);
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length == 0 || returnBytes[1] == null ?
                null:
                VersionEntry.Deserialize(req.RecordKey, req.VersionKey, returnBytes[1]);
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length == 0 ?
                -1L:
                BitConverter.ToInt64(returnBytes[1], 0);
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
                req.Result = (long)req.Result;
            }
            catch (Exception)
            {
                req.Result = 0L;
            }
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            byte[][] returnBytes = req.Result as byte[][];
            req.Result = returnBytes == null || returnBytes.Length < 2 ? 
                null :
                VersionEntry.Deserialize(req.RecordKey, req.VersionKey, returnBytes[1]);
        }

        internal override void Visit(UploadVersionRequest req)
        {
            try
            {
                req.Result = (long)req.Result;
            }
            catch (Exception)
            {
                req.Result = 0L;
            }
        }
    }
}
