
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal abstract class TxRequest : IResource
    {
        internal bool Finished { get; set; } = false;
        internal object Result { get; set; }
        internal bool InUse { get; set; }

        internal virtual void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        public void Use()
        {
            this.InUse = true;
        }

        public bool IsActive()
        {
            return InUse;
        }

        public void Free()
        {
            this.InUse = false;
            this.Finished = false;
        }
    }

    /// <summary>
    /// Tx requests towards tx entries
    /// </summary>
    internal abstract class TxEntryRequest : TxRequest
    {
        internal long TxId { get; set; }

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
        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal long VersionKey { get; set; }

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
        internal List<VersionEntry> Container { get; set; }

        public GetVersionListRequest(string tableId, object recordKey, List<VersionEntry> container)
            : base(tableId, recordKey, -1)
        {
            this.Container = container;
        }

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
        internal List<VersionEntry> Container { get; set; }

        public InitiGetVersionListRequest(string tableId, object recordKey, List<VersionEntry> container = null)
            : base(tableId, recordKey, -1)
        {
            this.Container = container;
        }

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
        internal long BeginTs { get; set; }
        internal long EndTs { get; set; }
        internal long TxId { get; set; }
        internal long ReadTxId { get; set; }
        internal long ExpectedEndTs { get; set; }

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
        internal VersionEntry VersionEntry { get; set; }

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
        internal long ProposedCommitTs { get; set; }

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
        internal long CommitTsLowerBound { get; set; }

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
        internal TxStatus TxStatus { get; set; }

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

    internal class RemoveTxRequest : TxEntryRequest
    {
        public RemoveTxRequest(long txId) : base(txId)
        {

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
        internal long MaxCommitTs { get; set; }

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
        internal VersionEntry VersionEntry { get; set; }

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
