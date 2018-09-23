
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    public abstract class TxRequest : IResource
    {
        internal volatile bool Finished = false;
        internal object Result { get; set; }
        internal bool InUse { get; set; }

        // The Id of the transaction initiating this request
        internal long SenderId { get; set; }
        
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
            this.Finished = false;
            this.InUse = false;
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

        /// <summary>
        /// The reference to local entry
        /// </summary>
        public TxTableEntry LocalTxEntry { get; set; }

        /// <summary>
        /// The reference to remote entry, it's only for remote version entry
        /// </summary>
        public TxTableEntry RemoteTxEntry { get; set; }

        public TxEntryRequest(long txId)
        {
            this.TxId = txId;
            this.LocalTxEntry = null;
            this.RemoteTxEntry = null;
        }

        public TxEntryRequest(long txId, TxTableEntry localTxEntry, TxTableEntry remoteTxEntry)
        {
            this.TxId = txId;
            this.LocalTxEntry = localTxEntry;
            this.RemoteTxEntry = remoteTxEntry;
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

        /// <summary>
        /// The reference to local version entry
        /// </summary>
        public VersionEntry LocalVerEntry { get; set; }

        /// <summary>
        /// The reference to remote version entry, it's only for in-memory version
        /// </summary>
        public VersionEntry RemoteVerEntry { get; set; }

        /// <summary>
        /// The reference to the remote version list address
        /// </summary>
        public IDictionary<long, VersionEntry> RemoteVerList { get; set; }

        public VersionEntryRequest(string tableId, object recordKey, long versionKey)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.LocalVerEntry = null;
            this.RemoteVerEntry = null;
            this.RemoteVerList = null;
        }

        public VersionEntryRequest(
            string tableId,
            object recordKey,
            long versionKey,
            VersionEntry localVerEntry,
            VersionEntry remoteVerEntry,
            IDictionary<long, VersionEntry> remoteVerList)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.LocalVerEntry = localVerEntry;
            this.RemoteVerEntry = remoteVerEntry;
            this.RemoteVerList = remoteVerList;
        }

        internal virtual void Accept(VersionEntryVisitor visitor)
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
            this.SenderId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        public void Set(long txId, TxTableEntry remoteEntry = null)
        {
            this.TxId = txId;
            this.SenderId = txId;
            this.RemoteTxEntry = remoteEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        // If the remote tx entry has been known, it would optimize the process
        public void Set(long txId, TxTableEntry remoteTxEntry = null)
        {
            this.TxId = txId;
            this.SenderId = txId;
            this.RemoteTxEntry = remoteTxEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class GetTxEntryRequest : TxEntryRequest
    {
        public GetTxEntryRequest(long txId)
            : base(txId) { }

        public void Set(long txId, long senderId, TxTableEntry localEntry = null, TxTableEntry remoteEntry = null)
        {
            this.TxId = txId;
            this.SenderId = senderId;
            this.LocalTxEntry = localEntry;
            this.RemoteTxEntry = remoteEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        public void Set(long txId, long proposedCommitTs, TxTableEntry remoteEntry = null)
        {
            this.TxId = txId;
            this.SenderId = txId;
            this.ProposedCommitTs = proposedCommitTs;
            this.RemoteTxEntry = remoteEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        public void Set(long txId, long senderId, long lowerBound, TxTableEntry remoteEntry = null)
        {
            this.TxId = txId;
            this.SenderId = senderId;
            this.CommitTsLowerBound = lowerBound;
            this.RemoteTxEntry = remoteEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        public void Set(long txId, TxStatus status, TxTableEntry remoteEntry = null)
        {
            this.TxId = txId;
            this.SenderId = txId;
            this.TxStatus = status;
            this.RemoteTxEntry = remoteEntry;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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
            this.SenderId = txId;
        }

        internal override void Accept(TxEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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

        public void Set(
            string tableId,
            object recordKey,
            long versionKey,
            long senderId,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.RemoteVerList = remoteVerList;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class GetVersionListRequest : VersionEntryRequest
    {
        public TxList<VersionEntry> LocalContainer { get; internal set; }

        /// <summary>
        /// A list of reference of remote version entries, which would be used
        /// to find the romote visiable version entry
        /// </summary>
        public TxList<VersionEntry> RemoteContainer { get; internal set; }

        public void Set(
            string tableId, 
            object recordKey, 
            long senderId,
            TxList<VersionEntry> localContainer,
            TxList<VersionEntry> remoteContainer = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = -1L;
            this.LocalContainer = localContainer;
            this.RemoteContainer = remoteContainer;
            this.SenderId = senderId;
        }

        public GetVersionListRequest(
            string tableId, 
            object recordKey, 
            TxList<VersionEntry> localContainer,
            TxList<VersionEntry> remoteContainer = null)
            : base(tableId, recordKey, -1)
        {
            this.LocalContainer = localContainer;
            this.RemoteContainer = remoteContainer;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class InitiGetVersionListRequest : VersionEntryRequest
    {
        public InitiGetVersionListRequest(string tableId, object recordKey)
            : base(tableId, recordKey, -1)
        {

        }

        public void Set(
            string tableId, 
            object recordKey,
            long senderId)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = -1L;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class ReadVersionRequest : VersionEntryRequest
    {
        public ReadVersionRequest(string tableId, object recordKey, long versionKey)
            : base(tableId, recordKey, versionKey) { }

        public void Set(
            string tableId,
            object recordKey,
            long versionKey,
            long senderId,
            VersionEntry localEntry = null,
            VersionEntry remoteEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.LocalVerEntry = localEntry;
            this.RemoteVerEntry = remoteEntry;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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
        public long ExpectedEndTs { get; internal set; }

        public ReplaceVersionRequest(
            string tableId, 
            object recordKey, 
            long versionKey,
            long beginTimestamp, 
            long endTimestamp, 
            long txId, 
            long senderId, 
            long expectedEndTimestamp)
            : base(tableId, recordKey, versionKey)
        {
            this.BeginTs = beginTimestamp;
            this.EndTs = endTimestamp;
            this.TxId = txId;
            this.SenderId = senderId;
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
            VersionEntry localVersionEntry = null,
            VersionEntry remoteVersionEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTs = beginTimestamp;
            this.EndTs = endTimestamp;
            this.TxId = txId;
            this.SenderId = readTxId;
            this.ExpectedEndTs = expectedEndTimestamp;
            this.LocalVerEntry = localVersionEntry;
            this.RemoteVerEntry = remoteVersionEntry;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }
    }

    public class ReplaceWholeVersionRequest : VersionEntryRequest
    {
        public VersionEntry VersionEntry { get; set; }

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
            long senderId,
            VersionEntry localVerEntry = null,
            VersionEntry remoteVerEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.LocalVerEntry = localVerEntry;
            this.RemoteVerEntry = remoteVerEntry;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
		{
			if (visitor != null)
			{
				visitor.Visit(this);
			}
		}

        internal override void Accept(TxRequestVisitor visitor)
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
            long senderId,
            VersionEntry localVerEntry = null,
            VersionEntry remoteVerEntry = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.MaxCommitTs = commitTime;
            this.LocalVerEntry = localVerEntry;
            this.RemoteVerEntry = remoteVerEntry;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
        }

        internal override void Accept(TxRequestVisitor visitor)
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
            VersionEntry versionEntry,
            long senderId,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.VersionEntry = versionEntry;
            this.RemoteVerList = remoteVerList;
            this.SenderId = senderId;
        }

        internal override void Accept(VersionEntryVisitor visitor)
        {
            if (visitor != null)
            {
                visitor.Visit(this);
            }
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
