
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

    /// <summary>
    /// Tx requests towards tx entries
    /// </summary>
    internal abstract class TxEntryRequest : TxRequest
    {
        internal long TxId { get; private set; }

        public TxEntryRequest(long txId)
        {
            this.TxId = txId;
        }
    }

    /// <summary>
    /// Tx requests toward version entries
    /// </summary>
    internal abstract class VersionEntryRequest : TxRequest
    {
        internal string TableId { get; private set; }
        internal object RecordKey { get; private set; }
        internal long VersionKey { get; private set; }

        public VersionEntryRequest(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
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

    internal class DeleteVersionRequest : VersionEntryRequest
    {
        public DeleteVersionRequest(string tableId, object recordKey, long versionKey)
            : base(tableId, recordKey, versionKey) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class GetVersionListRequest : VersionEntryRequest
    {
        public GetVersionListRequest(string tableId, object recordKey)
            : base(tableId, recordKey, -1) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class GetTxEntryRequest : TxEntryRequest
    {
        public GetTxEntryRequest(long txId)
            : base(txId) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class InitiGetVersionListRequest : VersionEntryRequest
    {
        public InitiGetVersionListRequest(string tableId, object recordKey)
            : base(tableId, recordKey, -1) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class InsertTxIdRequest : TxEntryRequest
    {
        public InsertTxIdRequest(long txId)
            : base(txId) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class NewTxIdRequest : TxEntryRequest
    {
        public NewTxIdRequest(long txId)
            : base(txId) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class RecycleTxRequest : TxEntryRequest
    {
        public RecycleTxRequest(long txId)
            : base(txId) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class ReadVersionRequest : VersionEntryRequest
    {
        public ReadVersionRequest(string tableId, object recordKey, long versionKey)
            : base(tableId, recordKey, versionKey) { }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    internal class ReplaceVersionRequest : VersionEntryRequest
    {
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
            : base(tableId, recordKey, versionKey)
        {
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

    internal class ReplaceWholeVersionRequest : VersionEntryRequest
    {
        internal VersionEntry VersionEntry { get; }

        public ReplaceWholeVersionRequest(
            string tableId,
            object recordKey,
            long versionKey,
            VersionEntry versionEntry)
            : base(tableId, recordKey, versionKey)
        {
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

    internal class SetCommitTsRequest : TxEntryRequest
    {
        internal long ProposedCommitTs { get; }

        public SetCommitTsRequest(long txId, long proposedCommitTs)
            : base(txId)
        {
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

    internal class UpdateCommitLowerBoundRequest : TxEntryRequest
    {
        internal long CommitTsLowerBound { get; }

        public UpdateCommitLowerBoundRequest(long txId, long commitTsLowerBound)
            : base(txId)
        {
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

    internal class UpdateTxStatusRequest : TxEntryRequest
    {
        internal TxStatus TxStatus { get; }

        public UpdateTxStatusRequest(long txId, TxStatus status)
            : base(txId)
        {
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

    internal class UpdateVersionMaxCommitTsRequest : VersionEntryRequest
    {
        internal long MaxCommitTs { get; }

        public UpdateVersionMaxCommitTsRequest(
            string tableId, object recordKey, long versionKey, long commitTime)
            : base(tableId, recordKey, versionKey)
        {
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

    internal class UploadVersionRequest : VersionEntryRequest
    {
        internal VersionEntry VersionEntry { get; }

        public UploadVersionRequest(
            string tableId, object recordKey, long versionKey, VersionEntry versionEntry)
            : base(tableId, recordKey, versionKey)
        {
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
