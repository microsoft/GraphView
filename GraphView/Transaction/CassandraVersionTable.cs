namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using Cassandra;

    /// TODO: questions need to be confirmed
    /// 1. how to handle cql query exceptions
    /// 2. when to release cluster resource
    /// 3. rowset doesn't return the result of INSERT, DELETE, UPDATE operation
    /// 
    /// <summary>
    /// A version table implementation in cassandra.
    /// The storage format of version table
    /// // blob: Arbitrary bytes (no validation), expressed as hexadecimal
    /// CREATE TABLE version_table (
    ///     record_key blob,
    ///     version_key bigint,
    ///     is_begin_tx_id boolean,
    ///     begin_timestamp bigint,
    ///     is_end_tx_id boolean,
    ///     end_timestamp bigint,
    ///     record blob,
    ///     PRIMARY KEY(record_key, version_key)
    /// ) WITH CLUSTERING ORDER BY(version_key DESC);
    /// </summary>
    internal partial class CassandraVersionTable : VersionTable
    {
        public CassandraVersionTable(VersionDb versionDb, string tableId, int partitionCount = 4)
            : base(versionDb, tableId)
        {
            this.PartitionCount = partitionCount;
           
            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                this.tableVisitors[pid] = new CassandraVersionTableVisitor();
            }
        }

        internal CassandraSessionManager SessionManager
        {
            get
            {
                return (this.VersionDb as CassandraVersionDb).SessionManager;
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            //int pk = this.VersionDb.PhysicalPartitionByKey(req.RecordKey);
            //this.tableVisitors[pk].Visit(req);
            this.tableVisitors[execPartition].Visit(req);
        }
    }

    /// <summary>
    /// CQL statements
    /// </summary>
    internal partial class CassandraVersionTable
    {
        public static readonly string CQL_GET_VERSION_TOP_2 =
            "SELECT * FROM {0} WHERE recordKey = '{1}' ORDER BY versionKey DESC LIMIT 2";

        public static readonly string CQL_REPLACE_VERSION =
            "UPDATE {0} SET beginTimestamp={1}, endTimestamp={2}, txId={3} " +
            "WHERE recordKey='{4}' AND versionKey={5} " +
            "IF txId={6} AND endTimestamp={7}";

        public static readonly string CQL_GET_VERSION_ENTRY =
            "SELECT * FROM {0} WHERE recordKey='{1}' AND versionKey={2}";

        public static readonly string CQL_REPLACE_WHOLE_VERSION =
            "UPDATE {0} SET beginTimestamp={1}, endTimestamp={2}, record={3}, txId={4}, maxCommitTs={5} " +
            "WHERE recordKey='{6}' AND versionKey={7} " +
            "IF EXISTS";

        public static readonly string CQL_UPLOAD_VERSION_ENTRY =
            "INSERT INTO {0} (recordKey, versionKey, beginTimestamp, endTimestamp, record, txId, maxCommitTs) " +
            "VALUES ('{1}', {2}, {3}, {4}, {5}, {6}, {7}) " +
            "IF NOT EXISTS";

        public static readonly string CQL_UPDATE_MAX_COMMIT_TIMESTAMP =
            "UPDATE {0} SET maxCommitTs = {1} " +
            "WHERE recordKey='{2}' AND versionKey={3} " +
            "IF maxCommitTs < {1}";

        public static readonly string CQL_DELETE_VERSION_ENTRY =
            "DELETE FROM {0} WHERE recordKey = '{1}' AND versionKey = {2} IF EXISTS";
          
    }

    /// <summary>
    /// Version Table operation
    /// </summary>
    internal partial class CassandraVersionTable
    {
        /// <summary>
        /// Execute the `cql` statement
        /// </summary>
        internal RowSet CQLExecute(string cql)
        {
            //CassandraSessionManager.CqlCnt += 1;
            //Console.WriteLine(cql);
            //return this.SessionManager.GetSession(((CassandraVersionDb)this.VersionDb).threadId, CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            return this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
        }

        /// <summary>
        /// Execute statements with Light Weight Transaction (IF),
        /// result RowSet just has one row, whose `[applied]` column indicates 
        /// the execution's state
        /// NOTE: `CREATE TABLE IF ...` can not be executed with this function, 
        /// catch `AlreadyExistsException` instead
        /// </summary>
        /// <param name="cql"></param>
        /// <returns>applied or not</returns>
        internal bool CQLExecuteWithIfApplied(string cql)
        {
            //CassandraSessionManager.CqlIfCnt += 1;
            //Console.WriteLine(cql);
            //var rs = this.SessionManager.GetSession(((CassandraVersionDb)this.VersionDb).threadId, CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rs = this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            return rse.Current.GetValue<bool>("[applied]");
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            List<VersionEntry> entries = new List<VersionEntry>();
            var rs = this.CQLExecute(string.Format(CassandraVersionTable.CQL_GET_VERSION_TOP_2,
                                                   this.tableId, recordKey.ToString()));
            foreach (var row in rs)
            {
                entries.Add(new VersionEntry(
                    row.GetValue<long>("versionkey"),
                    row.GetValue<long>("begintimestamp"),
                    row.GetValue<long>("endtimestamp"),
                    BytesSerializer.Deserialize(row.GetValue<byte[]>("record")),
                    row.GetValue<long>("txid"),
                    row.GetValue<long>("maxcommitts")
                ));
            }

            return entries;
        }

        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry();

            bool applied = this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_UPLOAD_VERSION_ENTRY,
                                                                this.tableId,
                                                                recordKey,
                                                                emptyEntry.VersionKey,
                                                                emptyEntry.BeginTimestamp,
                                                                emptyEntry.EndTimestamp,
                                                                BytesSerializer.ToHexString(BytesSerializer.Serialize(emptyEntry.Record)),
                                                                emptyEntry.TxId,
                                                                emptyEntry.MaxCommitTs));
            if (applied)
            {
                return this.GetVersionList(recordKey);
            } else
            {
                return null;
            }
        }

        internal override VersionEntry ReplaceVersionEntry(object recordKey, long versionKey, long beginTimestamp, long endTimestamp, long txId, long readTxId, long expectedEndTimestamp)
        {
            
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_REPLACE_VERSION,
                                                this.tableId, beginTimestamp, endTimestamp, txId,
                                                recordKey.ToString(), versionKey,
                                                readTxId, expectedEndTimestamp));

            return this.GetVersionEntryByKey(recordKey, versionKey);
        }

        internal override bool ReplaceWholeVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            return this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_REPLACE_WHOLE_VERSION,
                                                                    this.tableId, versionEntry.BeginTimestamp, versionEntry.EndTimestamp,
                                                                    BytesSerializer.ToHexString(BytesSerializer.Serialize(versionEntry.Record)),
                                                                    versionEntry.TxId, versionEntry.MaxCommitTs,
                                                                    recordKey.ToString(), versionEntry.VersionKey));
        }

        internal override bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            return this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_UPLOAD_VERSION_ENTRY,
                                                      this.tableId,
                                                      recordKey.ToString(),
                                                      versionEntry.VersionKey,
                                                      versionEntry.BeginTimestamp,
                                                      versionEntry.EndTimestamp,
                                                      BytesSerializer.ToHexString(BytesSerializer.Serialize(versionEntry.Record)),
                                                      versionEntry.TxId,
                                                      versionEntry.MaxCommitTs));
        }

        internal override VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTs)
        {           
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_UPDATE_MAX_COMMIT_TIMESTAMP,
                                            this.tableId, commitTs, recordKey.ToString(), versionKey));
            return this.GetVersionEntryByKey(recordKey, versionKey);
        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            return this.CQLExecuteWithIfApplied(string.Format(CassandraVersionTable.CQL_DELETE_VERSION_ENTRY,
                                                       this.tableId, recordKey.ToString(), versionKey));
        }

        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            var rs = this.CQLExecute(string.Format(CassandraVersionTable.CQL_GET_VERSION_ENTRY,
                                                    this.tableId, recordKey.ToString(), versionKey));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            if (row == null)
            {
                return null;
            }

            return new VersionEntry(
                versionKey, row.GetValue<long>("begintimestamp"),
                row.GetValue<long>("endtimestamp"),
                BytesSerializer.Deserialize(row.GetValue<byte[]>("record")),
                row.GetValue<long>("txid"),
                row.GetValue<long>("maxcommitts"));
        }

        internal override IDictionary<VersionPrimaryKey, VersionEntry> GetVersionEntryByKey(IEnumerable<VersionPrimaryKey> batch)
        {
            Dictionary<VersionPrimaryKey, VersionEntry> versionEntries = new Dictionary<VersionPrimaryKey, VersionEntry>();
            // sadly, there is no batch read method
            foreach (VersionPrimaryKey pk in batch)
            {
                versionEntries.Add(pk, this.GetVersionEntryByKey(pk.RecordKey, pk.VersionKey));
            }

            return versionEntries;
        }
    }
}
