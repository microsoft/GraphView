namespace GraphView.Transaction
{
    using Cassandra;
    using RecordRuntime;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// In cassandraVersionDb, we assume a cassandra table associates with a GraphView
    /// table, which takes (recordKey, versionKey) as the primary keys. A cassandra 
    /// keyspace associcates with a cassandraVersionDb. Every table name is in
    /// the format of 'keyspace.tablename', such as 'UserDb.Profile'
    /// </summary>
    internal partial class CassandraVersionDb : VersionDb
    {
        /// <summary>
        /// A map from version table name to version table instance
        /// </summary>
        private Dictionary<string, CassandraVersionTable> versionTableMap;

        /// <summary>
        /// The singleton instance of CassandraVersionDb
        /// </summary>
        private static volatile CassandraVersionDb instance;

        /// <summary>
        /// the lock to guarantee the safety of table's creation and delete
        /// </summary>
        private readonly object tableLock;

        /// <summary>
        /// the lock to init the singleton instance
        /// </summary>
        private static readonly object initLock = new object();

        private Cluster CassandraCluster
        {
            get
            {
                return CassandraClusterManager.CassandraCluster;
            }
        }

        private CassandraVersionDb()
        {
            this.tableLock = new object();
            this.versionTableMap = new Dictionary<string, CassandraVersionTable>();
        }

        internal static CassandraVersionDb Instance
        {
            get
            {
                if (CassandraVersionDb.instance == null)
                {
                    lock (CassandraVersionDb.initLock)
                    {
                        if (CassandraVersionDb.instance == null)
                        {
                            CassandraVersionDb.instance = new CassandraVersionDb();
                        }
                    }
                }
                return CassandraVersionDb.instance;
            }
        }
        
        /// <summary>
        /// Add a version table to cassandra
        /// </summary>
        /// <param name="tableId">The table id in format of "keyspace:tablename"</param>
        /// <returns>true or false</returns>
        public bool AddVersionTable(string tableId)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                // The idea without "IF EXISTS" check is that we could catch the
                // exception in the upper level if there is a duplicated table.
                // Otherwise, the table creatation sentence will not create a table with
                // the same name and return true sliently, which is what we expect.
                RowSet rowSet = session.Execute(
                    $@"CREATE TABLE '{tableId}' (
                        record_key blob,
                        version_key bigint,
                        is_begin_tx_id boolean,
                        begin_timestamp bigint,
                        id_end_tx_id boolean,
                        end_timestamp bigint,
                        record blob,
                        PRIMARY KEY(record_key, version_key));");
                // extract result from rowSet
                return true;
            }
        }

        /// <summary>
        /// Delete a version table by tableId
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns>true or false</returns>
        public bool DeleteVersionTable(string tableId)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                RowSet rowSet = session.Execute($@"DROP TABLE IF EXISTS '{tableId}'");
                
                if (this.versionTableMap.ContainsKey(tableId))
                {
                    lock (this.tableLock)
                    {
                        if (this.versionTableMap.ContainsKey(tableId))
                        {
                            this.versionTableMap.RemoveKey(tableId);
                        }
                    }
                }
                return true;
            }
        }

        /// <summary>
        /// Return all version tables inside the specified keyspace
        /// If the key space is null return all tables except system tables
        /// </summary>
        /// <param name="keySpace"></param>
        /// <returns>a list of table names</returns>
        protected IList<string> getAllVersionTables(string keySpace = null)
        {
            List<string> versionTables = new List<string>();
            using (ISession session = this.CassandraCluster.Connect())
            {
                Statement statement;
                if (keySpace != null)
                {
                    PreparedStatement preStatement = session.Prepare(
                        @"SELECT keyspace_name, table_name FROM system_schema.tables 
                        WHERE keyspace_name = ?;");
                    statement = preStatement.Bind(keySpace);
                }
                else
                {
                    PreparedStatement preStatement = session.Prepare(
                        @"SELECT keyspace_name, table_name FROM system_schema.tables 
                        WHERE keyspace_name != ? and keyspace_name != ? and
                        keyspace_name != ? and keyspace_name != ? and keyspace_name != ?");

                    statement = preStatement.Bind("system", "system_auth",
                        "system_schema", "system_distributed", "system_traces");
                }

                RowSet rowSet = session.Execute(statement);
                foreach (Row row in rowSet)
                {
                    string rowKeySpace = row.GetValue<string>("keyspace_name");
                    string rowTableName = row.GetValue<string>("table_name");
                    versionTables.Add($"'{rowKeySpace}'.'{rowTableName}'");
                }
            }
            return versionTables;  
        }

        /// <summary>
        /// Get the version table instance by tableId
        /// </summary>
        /// <param name="tableId">In the format of "keyspace.tablename"</param>
        /// <returns>version table instance if the table exists, otherwise return null</returns>
        protected CassandraVersionTable GetCassandraVersionTable(string tableId)
        {
            if (!this.versionTableMap.ContainsKey(tableId))
            {
                // split tableId to keyspace and tablename
                string[] items = tableId.Split('.');
                using (ISession session = this.CassandraCluster.Connect())
                {
                    PreparedStatement preStatement = session.Prepare(@"SELECT table_name FROM 
                    system_schema.tables WHERE keyspace_name = ? and table_name = ?");

                    Statement statement = preStatement.Bind(items[0], items[1]);
                    RowSet rowSet = session.Execute(statement);

                    bool tableExist = false;
                    if (rowSet != null)
                    {
                        foreach (Row row in rowSet)
                        {
                            // If the row set is not null and has at least an entity
                            tableExist = true;
                            break;
                        }
                    }

                    if (!tableExist)
                    {
                        return null;
                    }

                    // The table exists and is loaded in the versionTableMap
                    if (!this.versionTableMap.ContainsKey(tableId))
                    {
                        lock (this.tableLock)
                        {
                            if (!this.versionTableMap.ContainsKey(tableId))
                            {
                                this.versionTableMap[tableId] = new CassandraVersionTable(tableId);
                            }
                        }
                    }
                }
            }
            return this.versionTableMap[tableId];
        }
    }

    /// <summary>
    /// 
    /// </summary>
    internal partial class CassandraVersionDb
    {
        internal override VersionTable GetVersionTable(string tableId)
        {
            return this.GetCassandraVersionTable(tableId);
        }

        internal override bool InsertVersion(string tableId, object recordKey,
            object record, long txId, long readTimestamp)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return false;
            }
            return versionTable.InsertVersion(recordKey, record, txId, readTimestamp);
        }
    }

    internal partial class CassandraVersionDb : IDataStore
    {
        public IList<Tuple<string, IndexSpecification>> GetIndexTables(string tableId)
        {
            throw new NotImplementedException();
        }

        public IList<string> GetTables()
        {
            throw new NotImplementedException();
        }
    }
}
