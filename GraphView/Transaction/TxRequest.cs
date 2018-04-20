
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    internal abstract class TxRequestVisitor
    {
        internal virtual void Visit(TxRequest req) { }
        internal virtual void Visit(BulkReadVersionsRequest req) { }
        internal virtual void Visit(DeleteVersionRequest req) { }
        internal virtual void Visit(GetVersionListRequest req) { }
        internal virtual void Visit(GetTxEntryRequest req) { }
        internal virtual void Visit(InitiGetVersionListRequest req) { }
        internal virtual void Visit(InsertTxIdRequest req) { }
        internal virtual void Visit(NewTxIdRequest req) { }
        internal virtual void Visit(ReadVersionRequest req) { }
        internal virtual void Visit(ReplaceVersionRequest req) { }
        internal virtual void Visit(SetCommitTsRequest req) { }
        internal virtual void Visit(UpdateCommitLowerBoundRequest req) { }
        internal virtual void Visit(UpdateTxStatusRequest req) { }
        internal virtual void Visit(UpdateVersionMaxCommitTsRequest req) { }
        internal virtual void Visit(UploadVersionRequest req) { }
    }

    internal abstract class TxRequest
    {
        internal bool Finished { get; private set; } = false;
        internal object Result { get; set; }
    }

    internal class BulkReadVersionsRequest : TxRequest
    {
        internal string TableId { get; }
        internal Dictionary<VersionPrimaryKey, VersionEntry> BulkRecords { get; }

        public BulkReadVersionsRequest(string tableId, IEnumerable<VersionPrimaryKey> recordVersionKeys)
        {
            this.BulkRecords = new Dictionary<VersionPrimaryKey, VersionEntry>();
            foreach (VersionPrimaryKey vpk in recordVersionKeys)
            {
                this.BulkRecords.Add(vpk, null);
            }
        }
    }

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
    }

    internal class GetTxEntryRequest : TxRequest
    {
        internal long TxId { get; }

        public GetTxEntryRequest(long txId)
        {
            this.TxId = TxId;
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
    }

    internal class InsertTxIdRequest : TxRequest
    {
        internal long TxId { get; }

        public InsertTxIdRequest(long txId)
        {
            this.TxId = txId;
        }
    }

    internal class NewTxIdRequest : TxRequest
    {
        internal long TxId { get; }

        public NewTxIdRequest(long txId)
        {
            this.TxId = txId;
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
        internal long ExpectedEndTimestamp { get; }

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
            this.ExpectedEndTimestamp = expectedEndTimestamp;
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
    }
 }
