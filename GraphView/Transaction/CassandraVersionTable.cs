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
        internal int PartitionCount { get; private set; }

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
    }

    /// <summary>
    /// CQL statements
    /// </summary>
    internal partial class CassandraVersionTable
    {
        public static readonly string CQL_GET_VERSION_TOP_2 =
            "SELECT * FROM {0} WHERE recordKey = {1} ORDER BY versionKey DESC LIMIT 2";

        public static readonly string CQL_INIT_VERSION =
            "INSERT INTO {0} (recordKey, versionKey, beginTimestamp, endTimestamp, record, txId, maxCommitTs) " +
            "VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}) IF NOT EXISTS";

        public static readonly string CQL_REPLACE_VERSION =
            "UPDATE {0} SET beginTimestamp={1}, endTimestamp={2}, txId={3} " +
            "WHERE recordKey={4} AND versionKey={5} " +
            "IF txId={6} AND endTimestamp={7}";

        public static readonly string CQL_GET_VERSION_ENTRY =
            "SELECT * FROM {0} WHERE recordKey={1} AND versionKey={2}";

        public static readonly string CQL_REPLACE_WHOLE_VERSION =
            "UPDATE {0} SET beginTimestamp={1}, endTimestamp={2}, record={3}, txId={4}, maxCommitTs={5} " +
            "WHERE recordKey={6} AND versionKey={7}";

        public static readonly string CQL_UPLOAD_VERSION_ENTRY =
            "INSERT INTO {0} (recordKey, versionKey, beginTimestamp, endTimestamp, record, txId, maxCommitTs) " +
            "VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7})";

        public static readonly string CQL_UPDATE_MAX_COMMIT_TIMESTAMP =
            "UPDATE {0} SET maxCommitTs = {1} " +
            "WHERE recordKey={2} AND versionKey={3} " +
            "IF maxCommitTs < {1}";

        public static readonly string CQL_DELETE_VERSION_ENTRY =
            "DELETE FROM {0} WHERE recordKey = {1} AND versionKey = {2}";
          
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
            return this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            List<VersionEntry> entries = new List<VersionEntry>();

            try
            {
                var rs = this.CQLExecute(string.Format(CassandraVersionTable.CQL_GET_VERSION_TOP_2,
                                                       this.tableId, recordKey as string));
                foreach (var row in rs)
                {

                    entries.Add(new VersionEntry(
                        row.GetValue<string>("recordKey"),
                        row.GetValue<long>("versionKey"),
                        row.GetValue<long>("beginTimestamp"),
                        row.GetValue<long>("endTimestamp"),
                        BytesSerializer.Deserialize(row.GetValue<byte[]>("record")),
                        row.GetValue<long>("txId"),
                        row.GetValue<long>("maxCommitTs")
                    ));
                }
            } catch (DriverException e)
            {
                //return null;
            }

            return entries;
        }

        internal override IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry(recordKey);

            try
            {

                this.CQLExecute(string.Format(CassandraVersionTable.CQL_INIT_VERSION,
                                              this.tableId,
                                              emptyEntry.RecordKey,
                                              emptyEntry.VersionKey,
                                              emptyEntry.BeginTimestamp,
                                              emptyEntry.EndTimestamp,
                                              BytesSerializer.ToHexString(BytesSerializer.Serialize(emptyEntry.Record)),
                                              emptyEntry.TxId,
                                              emptyEntry.MaxCommitTs));
                return this.GetVersionList(recordKey);
            } catch (DriverException e)
            {
                return null;
            }
        }

        internal override VersionEntry ReplaceVersionEntry(object recordKey, long versionKey, long beginTimestamp, long endTimestamp, long txId, long readTxId, long expectedEndTimestamp)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionTable.CQL_REPLACE_VERSION,
                                              this.tableId, beginTimestamp, endTimestamp, txId,
                                              recordKey as string, versionKey,
                                              readTxId, expectedEndTimestamp));
                return this.GetVersionEntryByKey(recordKey, versionKey);
            }
            catch (DriverException e)
            {
                return null;
            }
        }

        internal override bool ReplaceWholeVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionTable.CQL_REPLACE_WHOLE_VERSION,
                                              this.tableId, versionEntry.BeginTimestamp, versionEntry.EndTimestamp,
                                              BytesSerializer.ToHexString(BytesSerializer.Serialize(versionEntry.Record)),
                                              versionEntry.TxId, versionEntry.MaxCommitTs, 
                                              versionEntry.RecordKey as string, versionEntry.VersionKey));
            } catch (DriverException e)
            {
                return false;
            }

            return true;
        }

        internal override bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionTable.CQL_UPLOAD_VERSION_ENTRY,
                                              this.tableId,
                                              versionEntry.RecordKey as string,
                                              versionEntry.VersionKey,
                                              versionEntry.BeginTimestamp,
                                              versionEntry.EndTimestamp,
                                              BytesSerializer.ToHexString(BytesSerializer.Serialize(versionEntry.Record)),
                                              versionEntry.TxId,
                                              versionEntry.MaxCommitTs));
            } catch (DriverException e)
            {

            }

            return true;
        }

        internal override VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTs)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionTable.CQL_UPDATE_MAX_COMMIT_TIMESTAMP,
                                              this.tableId, commitTs, recordKey as string, versionKey));
                return this.GetVersionEntryByKey(recordKey, versionKey);
            } catch (DriverException e)
            {
                return null;
            }

        }

        internal override bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            try
            {
                this.CQLExecute(string.Format(CassandraVersionTable.CQL_DELETE_VERSION_ENTRY,
                                              recordKey as string, versionKey));

            } catch (DriverException e)
            {
                
            }

            return true;
        }

        internal override VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            try
            {
                var rs = this.CQLExecute(string.Format(CassandraVersionTable.CQL_GET_VERSION_ENTRY,
                                                       this.tableId, recordKey as string, versionKey));
                if (rs == null)
                {
                    return null;
                }

                Row row = rs.GetEnumerator().Current;
                if (row == null)
                {
                    return null;
                }

                return new VersionEntry(
                    recordKey, versionKey, row.GetValue<long>("beginTimestamp"),
                    row.GetValue<long>("endTimestamp"),
                    BytesSerializer.Deserialize(row.GetValue<byte[]>("record")),
                    row.GetValue<long>("txId"),
                    row.GetValue<long>("maxCommitTs"));
            } catch (DriverException e)
            {
                return null;
            }
        }

        internal override IDictionary<VersionPrimaryKey, VersionEntry> GetVersionEntryByKey(IEnumerable<VersionPrimaryKey> batch)
        {
            Dictionary<VersionPrimaryKey, VersionEntry> versionEntries = new Dictionary<VersionPrimaryKey, VersionEntry>();
            foreach (VersionPrimaryKey pk in batch)
            {
                versionEntries.Add(pk, this.GetVersionEntryByKey(pk.RecordKey, pk.VersionKey));
            }

            return versionEntries;
        }        
    }
}
