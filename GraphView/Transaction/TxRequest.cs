
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    public abstract class TxRequest : IResource
    {
        internal bool Finished { get; set; } = false;
        internal object Result { get; set; }
        internal bool InUse { get; set; }
        
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
            this.Result = null;
        }

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
    public abstract class TxEntryRequest : TxRequest
    {
        public long TxId { get; internal set; }

        public TxEntryRequest(long txId)
        {
            this.TxId = txId;
        }

        internal virtual void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    /// <summary>
    /// Tx requests toward version entries
    /// </summary>
    public abstract class VersionEntryRequest : TxRequest
    {
        public string TableId { get; internal set; }
        public object RecordKey { get; internal set; }
        public long VersionKey { get; internal set; }

        public VersionEntryRequest(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }

        internal virtual void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class DeleteVersionRequest : VersionEntryRequest
    {
        public DeleteVersionRequest(string tableId, object recordKey, long versionKey)
            : base(tableId, recordKey, versionKey) { }

        public void Set(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class GetVersionListRequest : VersionEntryRequest
    {
        public TxList<VersionEntry> Container { get; internal set; }

        public void Set(string tableId, object recordKey, TxList<VersionEntry> container)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = -1L;
            this.Container = container;
        }

        public GetVersionListRequest(string tableId, object recordKey, TxList<VersionEntry> container)
            : base(tableId, recordKey, -1)
        {
            this.Container = container;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class GetTxEntryRequest : TxEntryRequest
    {
        public TxTableEntry TxEntry { get; set; }

        public GetTxEntryRequest(long txId)
            : base(txId) { }

        public void Set(long txId, TxTableEntry txEntry = null)
        {
            this.TxId = txId;
            this.TxEntry = txEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class InitiGetVersionListRequest : VersionEntryRequest
    {
        public TxList<VersionEntry> Container { get; internal set; }

        public InitiGetVersionListRequest(string tableId, object recordKey, TxList<VersionEntry> container = null)
            : base(tableId, recordKey, -1)
        {
            this.Container = container;
        }

        public void Set(string tableId, object recordKey, TxList<VersionEntry> container = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = -1L;
            this.Container = container;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class InsertTxIdRequest : TxEntryRequest
    {
        public InsertTxIdRequest(long txId)
            : base(txId) { }

        public void Set(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class NewTxIdRequest : TxEntryRequest
    {
        public NewTxIdRequest(long txId)
            : base(txId) { }

        public void Set(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class RecycleTxRequest : TxEntryRequest
    {
        public RecycleTxRequest(long txId)
            : base(txId) { }

        public void Set(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class ReadVersionRequest : VersionEntryRequest
    {
        public VersionEntry VersionEntry { get; set; }
        public ReadVersionRequest(string tableId, object recordKey, long versionKey)
            : base(tableId, recordKey, versionKey) { }

        public void Set(
            string tableId, 
            object recordKey, 
            long versionKey, 
            VersionEntry versionEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class ReplaceVersionRequest : VersionEntryRequest
    {
        public long BeginTs { get; internal set; }
        public long EndTs { get; internal set; }
        public long TxId { get; internal set; }
        public long ReadTxId { get; internal set; }
        public long ExpectedEndTs { get; internal set; }
        public VersionEntry VersionEntry { get; set; }

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

        public void Set(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            long readTxId,
            long expectedEndTimestamp,
            VersionEntry versionEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTs = beginTimestamp;
            this.EndTs = endTimestamp;
            this.TxId = txId;
            this.ReadTxId = readTxId;
            this.ExpectedEndTs = expectedEndTimestamp;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class ReplaceWholeVersionRequest : VersionEntryRequest
    {
        public VersionEntry VersionEntry { get; internal set; }

        public ReplaceWholeVersionRequest(
            string tableId,
            object recordKey,
            long versionKey,
            VersionEntry versionEntry)
            : base(tableId, recordKey, versionKey)
        {
            this.VersionEntry = versionEntry;
        }

        public void Set(
            string tableId,
            object recordKey,
            long versionKey,
            VersionEntry versionEntry)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
		{
			if (visitor != null)
			{
				visitor.Visit(this);
			}
		}
	}

    public class SetCommitTsRequest : TxEntryRequest
    {
        public long ProposedCommitTs { get; internal set; }

        public SetCommitTsRequest(long txId, long proposedCommitTs)
            : base(txId)
        {
            this.ProposedCommitTs = proposedCommitTs;
        }

        public void Set(long txId, long proposedCommitTs)
        {
            this.TxId = txId;
            this.ProposedCommitTs = proposedCommitTs;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class UpdateCommitLowerBoundRequest : TxEntryRequest
    {
        public long CommitTsLowerBound { get; internal set; }

        public UpdateCommitLowerBoundRequest(long txId, long commitTsLowerBound)
            : base(txId)
        {
            this.CommitTsLowerBound = commitTsLowerBound;
        }

        public void Set(long txId, long lowerBound)
        {
            this.TxId = txId;
            this.CommitTsLowerBound = lowerBound;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class UpdateTxStatusRequest : TxEntryRequest
    {
        public TxStatus TxStatus { get; internal set; }

        public UpdateTxStatusRequest(long txId, TxStatus status)
            : base(txId)
        {
            this.TxStatus = status;
        }

        public void Set(long txId, TxStatus status)
        {
            this.TxId = txId;
            this.TxStatus = status;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class RemoveTxRequest : TxEntryRequest
    {
        public RemoveTxRequest(long txId) : base(txId)
        {

        }

        public void Set(long txId)
        {
            this.TxId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class UpdateVersionMaxCommitTsRequest : VersionEntryRequest
    {
        public long MaxCommitTs { get; internal set; }
        public VersionEntry VersionEntry { get; set; }

        public UpdateVersionMaxCommitTsRequest(
            string tableId, object recordKey, long versionKey, long commitTime)
            : base(tableId, recordKey, versionKey)
        {
            this.MaxCommitTs = commitTime;
        }

        public void Set(
            string tableId, 
            object recordKey, 
            long versionKey, 
            long commitTime, 
            VersionEntry versionEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.MaxCommitTs = commitTime;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class UploadVersionRequest : VersionEntryRequest
    {
        public VersionEntry VersionEntry { get; internal set; }

        public UploadVersionRequest(
            string tableId, object recordKey, long versionKey, VersionEntry versionEntry)
            : base(tableId, recordKey, versionKey)
        {
            this.VersionEntry = versionEntry;
        }

        public void Set(
            string tableId,
            object recordKey, 
            long versionKey, 
            VersionEntry versionEntry)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }
 }
