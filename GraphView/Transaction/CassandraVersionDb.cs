namespace GraphView.Transaction
{
    using Cassandra;
    using RecordRuntime;
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// In cassandraVersionDb, we assume a cassandra table associates with a GraphView
    /// table, which takes (recordKey, versionKey) as the primary keys. A cassandra 
    /// keyspace associcates with a cassandraVersionDb. Every table name is in
    /// the format of 'keyspace.tablename', such as 'UserDb.Profile'
    /// </summary>
    internal partial class CassandraVersionDb : VersionDb
    {
        private static volatile CassandraVersionDb instance;
        private static readonly object initlock = new object();

        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        private CassandraVersionDb(int partitionCount)
            : base(partitionCount)
        {
            for (int pid = 0; pid < partitionCount; pid++)
            {
                this.dbVisitors[pid] = new CassandraVersionDbVisitor();
            }
        }

        internal static CassandraVersionDb Instance(int partitionCount = 4)
        {
            if (CassandraVersionDb.instance == null)
            {
                lock(initlock)
                {
                    if (CassandraVersionDb.instance == null)
                    {
                        CassandraVersionDb.instance = new CassandraVersionDb(partitionCount);
                    }
                }
            }
            return CassandraVersionDb.instance;
        }

        internal override void Clear()
        {
            this.versionTables.Clear();

            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                this.txEntryRequestQueues[pid].Clear();
                this.flushQueues[pid].Clear();
            }
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (!this.versionTables.ContainsKey(tableId))
                    {
                        CassandraVersionTable vtable = new CassandraVersionTable(this, tableId, this.PartitionCount);
                        this.versionTables.Add(tableId, vtable);
                    }
                }
            }

            return this.versionTables[tableId];
        }

        internal override bool DeleteTable(string tableId)
        {
            if (this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (this.versionTables.ContainsKey(tableId))
                    {
                        this.versionTables.Remove(tableId);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }


    }
}
