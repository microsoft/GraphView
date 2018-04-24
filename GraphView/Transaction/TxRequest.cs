
namespace GraphView.Transaction
{
    internal abstract class TxRequest
    {
        internal bool Finished { get; set; } = false;
        internal object Result { get; set; }

        internal virtual void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    //internal class BulkReadVersionsRequest : TxRequest
    //{
    //    internal string TableId { get; }
    //    internal Dictionary<VersionPrimaryKey, VersionEntry> BulkRecords { get; }

    //    public BulkReadVersionsRequest(string tableId, IEnumerable<VersionPrimaryKey> recordVersionKeys)
    //    {
    //        this.BulkRecords = new Dictionary<VersionPrimaryKey, VersionEntry>();
    //        foreach (VersionPrimaryKey vpk in recordVersionKeys)
    //        {
    //            this.BulkRecords.Add(vpk, null);
    //        }
    //    }
    //}

    internal class DeleteVersionRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }

        public DeleteVersionRequest(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class GetVersionListRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }

        public GetVersionListRequest(string tableId, object recordKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class GetTxEntryRequest : TxRequest
    {
        internal long TxId { get; }

        public GetTxEntryRequest(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class InitiGetVersionListRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }

        public InitiGetVersionListRequest(string tableId, object recordKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class InsertTxIdRequest : TxRequest
    {
        internal long TxId { get; }

        public InsertTxIdRequest(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class NewTxIdRequest : TxRequest
    {
        internal long TxId { get; }

        public NewTxIdRequest(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class ReadVersionRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }

        public ReadVersionRequest(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class ReplaceVersionRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }
        internal long BeginTs { get; }
        internal long EndTs { get; }
        internal long TxId { get; }
        internal long ReadTxId { get; }
        internal long ExpectedEndTs { get; }

        public ReplaceVersionRequest(
            string tableId, 
            object recordKey, 
            long versionKey,
            long beginTimestamp, 
            long endTimestamp, 
            long txId, 
            long readTxId, 
            long expectedEndTimestamp)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTs = beginTimestamp;
            this.EndTs = endTimestamp;
            this.TxId = txId;
            this.ReadTxId = readTxId;
            this.ExpectedEndTs = expectedEndTimestamp;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class ReplaceWholeVersionRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }
        internal VersionEntry VersionEntry { get; }

        public ReplaceWholeVersionRequest(string tableId,
            object recordKey,
            long versionKey,
            VersionEntry versionEntry)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
        }
    }

    internal class SetCommitTsRequest : TxRequest
    {
        internal long ProposedCommitTs { get; }
        internal long TxId { get; }

        public SetCommitTsRequest(long txId, long proposedCommitTs)
        {
            this.TxId = txId;
            this.ProposedCommitTs = proposedCommitTs;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class UpdateCommitLowerBoundRequest : TxRequest
    {
        internal long TxId { get; }
        internal long CommitTsLowerBound { get; }

        public UpdateCommitLowerBoundRequest(long txId, long commitTsLowerBound)
        {
            this.TxId = txId;
            this.CommitTsLowerBound = commitTsLowerBound;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class UpdateTxStatusRequest : TxRequest
    {
        internal long TxId { get; }
        internal TxStatus TxStatus { get; }

        public UpdateTxStatusRequest(long txId, TxStatus status)
        {
            this.TxId = txId;
            this.TxStatus = status;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class UpdateVersionMaxCommitTsRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }
        internal long MaxCommitTs { get; }

        public UpdateVersionMaxCommitTsRequest(string tableId, object recordKey, long versionKey, long commitTime)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.MaxCommitTs = commitTime;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class UploadVersionRequest : TxRequest
    {
        internal string TableId { get; }
        internal object RecordKey { get; }
        internal long VersionKey { get; }
        internal VersionEntry VersionEntry { get; }

        public UploadVersionRequest(string tableId, object recordKey, long versionKey, VersionEntry versionEntry)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }
 }
