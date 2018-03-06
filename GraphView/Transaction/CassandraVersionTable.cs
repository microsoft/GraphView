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
    ///     // blob: Arbitrary bytes (no validation), expressed as hexadecimal
    ///     CREATE TABLE versionList(
    ///         record_key blob,
    ///         version_key bigint,
    ///         is_begin_tx_id boolean,
    ///         begin_timestamp bigint,
    ///         id_end_tx_id boolean,
    ///         end_timestamp bigint,
    ///         record blob,
    ///         PRIMARY KEY (record_key, version_key)
    /// );
    /// </summary>
    internal partial class CassandraVersionTable : VersionTable
    {
        /// <summary>
        /// The cluster instance
        /// </summary>
        private Cluster CassandraCluster {
            get
            {
                return CassandraClusterManager.CassandraCluster;
            }
        }

        public CassandraVersionTable(string tableId)
            : base(tableId)
        { 
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            List<VersionEntry> versionList = new List<VersionEntry>();
            using (ISession session = this.CassandraCluster.Connect())
            {
                RowSet rowSet = session.Execute($"SELECT * FROM '{this.tableId}'");

                foreach (Row row in rowSet)
                {
                    VersionEntry versionEntry = this.GetVersionEntryFromRow(row);
                    versionList.Add(versionEntry);
                }
           
            }
            return versionList;
        }

        internal override IEnumerable<VersionEntry> GetVersionList(
            object recordKey,
            long timestamp)
        {
            return this.GetVersionList(recordKey);
        }

        internal override void InsertAndUploadVersion(
            object recordKey,
            VersionEntry version)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                PreparedStatement preStatement = session.Prepare($@"INSERT INTO '{this.tableId}' 
                    (record_key, version_key, is_begin_tx_id, begin_timestamp, 
                    is_end_tx_id, end_timestamp, record)
                    VALUES (?, ?, ?, ?, ?, ?, ?)");
                Statement statement = preStatement.Bind(
                    version.RecordKey, version.VersionKey, 
                    version.IsBeginTxId, version.BeginTimestamp, version.IsEndTxId,
                    version.EndTimestamp, version.Record);

                RowSet rowSet = session.Execute(statement);
            } 
        }

        internal override bool UpdateAndUploadVersion(
            object recordKey,
            long versionKey,
            VersionEntry oldVersion,
            VersionEntry newVersion)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                // CAS update with IF keyword
                // Replace all columns except record_key and version_key (immutable fields)
                // if all columns are equal to the given oldVersion then replace it
                PreparedStatement preStatement = session.Prepare($@"UPDATE '{this.tableId}' 
                    SET is_begin_tx_id = ?, begin_timestamp = ?, is_end_tx_id = ?,
                    end_timestamp = ?, record = ? 
                    WHERE record_key = ? and version_key = ? 
                    IF is_begin_tx_id = ? and begin_timestamp = ? 
                    and is_end_tx_id = ? and end_timestamp = ? and record = ?");
                
                Statement statement = preStatement.Bind (
                    newVersion.IsBeginTxId, newVersion.BeginTimestamp, 
                    newVersion.IsEndTxId, newVersion.EndTimestamp, newVersion.Record,
                    recordKey, versionKey,
                    oldVersion.IsBeginTxId, oldVersion.BeginTimestamp, 
                    oldVersion.IsEndTxId, oldVersion.EndTimestamp, oldVersion.Record);

                RowSet rowSet = session.Execute(statement);
                return true;
            }
        }

        internal override void DeleteVersionEntry(object recordKey, long versionKey)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                PreparedStatement preStatement = session.
                    Prepare($@"DELETE FROM '{this.tableId}' WHERE record_key = ? and version_key = ?");
                Statement statement = preStatement.Bind( recordKey, versionKey);
                RowSet rowSet = session.Execute(statement);
            }
        }

        /// <summary>
        /// Reconstruct the version entry instance from cassandra row
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private VersionEntry GetVersionEntryFromRow(Row row)
        {
            if (row == null)
            {
                return null;
            }

            return new VersionEntry(
                row.GetValue<bool>("is_begin_tx_id"),
                row.GetValue<long>("begin_timestamp"),
                row.GetValue<bool>("id_end_tx_id"),
                row.GetValue<long>("end_timestamp"),
                row.GetValue<object>("record_key"),
                row.GetValue<long>("version_key"),
                row.GetValue<object>("record")
            );
        }
    }

    internal partial class CassandraVersionTable
    {
        public override bool DeleteJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
