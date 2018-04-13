using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;

namespace GraphView.Transaction
{
    public class CassandraLogStore : ILogStore
    {
        /// <summary>
        /// The lock to init the singleton instance
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// The singleton instance of CassandraLogStore
        /// </summary>
        private static volatile CassandraLogStore instance;

        /// <summary>
        /// A hashset to store the tableId which has already been created.
        /// </summary>
        private readonly HashSet<string> tableSet;

        /// <summary>
        /// the lock to guarantee the safety of table's creation and delete
        /// </summary>
        private readonly object tableLock = new object();

        private readonly string TRANSACTION_TABLE_NAME = "transaction";

        private readonly string KEYSPACE = "log";


        private Cluster CassandraCluster
        {
            get
            {
                return CassandraClusterManager.CassandraCluster;
            }
        }

        private CassandraLogStore()
        {
            this.tableSet = new HashSet<string>();

            //create a transaction log table
            using (ISession session = this.CassandraCluster.Connect())
            {
                session.Execute($@"CREATE KEYSPACE IF NOT EXISTS {this.KEYSPACE} WITH replication = "+
                                "{'class':'SimpleStrategy', 'replication_factor':'3'};");

                session.Execute($@"
                        CREATE TABLE IF NOT EXISTS {this.KEYSPACE+"."+this.TRANSACTION_TABLE_NAME} (
                            txId bigint,
                            PRIMARY KEY (txId)
                        );");
            }
        }

        public static CassandraLogStore Instance
        {
            get
            {
                if (CassandraLogStore.instance == null)
                {
                    lock (CassandraLogStore.initLock)
                    {
                        if (CassandraLogStore.instance == null)
                        {
                            CassandraLogStore.instance = new CassandraLogStore();
                        }
                    }
                }

                return CassandraLogStore.instance;
            }
        }

        public bool WriteCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                if (!this.tableSet.Contains(tableId))
                {
                    lock (this.tableLock)
                    {
                        if (!this.tableSet.Contains(tableId))
                        {
                            try
                            {
                                session.Execute($@"
                                    CREATE TABLE {this.KEYSPACE + "." + tableId} (
                                    recordKey blob,
                                    beginTs bigint,
                                    txId bigint,
                                    payload blob,
                                    PRIMARY KEY (recordKey, beginTs)
                                );");
                            }
                            catch (DriverException e)
                            {
                                return false;
                            }

                            this.tableSet.Add(tableId);
                        }
                    }
                }

                try
                {
                    PreparedStatement ps = session.Prepare($@"
                        INSERT INTO {this.KEYSPACE + "." + tableId} (recordKey, beginTs, txId, payload) VALUES (?, ?, ?, ?)");
                    Statement statement = ps.Bind(BytesSerializer.Serialize(recordKey), 
                        commitTs, txId, BytesSerializer.Serialize(payload));
                    session.Execute(statement);
                }
                catch (DriverException e)
                {
                    return false;
                }
            }

            return true;
        }

        public bool WriteCommittedTx(long txId)
        {
            using (ISession session = this.CassandraCluster.Connect())
            {
                try
                {
                    PreparedStatement ps = session.Prepare($@"
                        INSERT INTO {this.KEYSPACE + "." + this.TRANSACTION_TABLE_NAME} (txId) VALUES (?)");
                    Statement statement = ps.Bind(txId);
                    session.Execute(statement);
                }
                catch (DriverException e)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
