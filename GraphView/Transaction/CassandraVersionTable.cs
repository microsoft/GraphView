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
    }
}
