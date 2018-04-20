using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace GraphView.Transaction
{
    public class CassandraLogStore : ILogStore
	{
		/// <summary>
		/// Provide an option to set log store in pipelineMode or not
		/// </summary>
		public bool PipelineMode { get; set; } = false;

		public static readonly int DEFAULT_CONNECTION_POOL_COUNT = 3;

		private CassandraLogConnectionPool[] connectionPool;

		/// <summary>
		/// The table name used to log the committed transaction' Id.
		/// </summary>
		private static readonly string TRANSACTION_TABLE_NAME = "transaction";

		/// <summary>
		/// The keyspace name used to store the transaction log table and all version log tables.
		/// </summary>
		private static readonly string KEYSPACE = "log";

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

		/// <summary>
		/// The session to perform read and write on the keyspace.
		/// </summary>
		private readonly ISession session;

		/// <summary>
		/// the prepare statement will be used in WriteCommittedTx().
		/// </summary>
		private readonly PreparedStatement insertTxIdToLog;

		private CassandraSessionManager SessionManager
        {
            get
            {
				return CassandraSessionManager.Instance;
            }
        }

        private CassandraLogStore()
        {
            this.tableSet = new HashSet<string>();
			this.connectionPool = new CassandraLogConnectionPool[CassandraLogStore.DEFAULT_CONNECTION_POOL_COUNT];
			for (int i = 0; i < CassandraLogStore.DEFAULT_CONNECTION_POOL_COUNT; i++)
			{
				int index = i;
				this.connectionPool[index] = new CassandraLogConnectionPool();
				new Thread(() =>
				{
					this.connectionPool[index].Monitor();
				}).Start();
			}

			this.session = this.SessionManager.GetSession(CassandraLogStore.KEYSPACE);

            // first create the log keyspace
            this.session.Execute($@"CREATE KEYSPACE IF NOT EXISTS {CassandraLogStore.KEYSPACE} WITH replication = "+
                                   "{'class':'SimpleStrategy', 'replication_factor':'3'};");

			// create the transaction log table
            this.session.Execute($@"
                        CREATE TABLE IF NOT EXISTS {CassandraLogStore.KEYSPACE + "." + CassandraLogStore.TRANSACTION_TABLE_NAME} (
                            txId bigint,
                            PRIMARY KEY (txId)
                        );");

			//initialize the prepare statement
			this.insertTxIdToLog = this.session.Prepare($@"
                        INSERT INTO {CassandraLogStore.KEYSPACE + "." + CassandraLogStore.TRANSACTION_TABLE_NAME} (txId) VALUES (?)");
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
			if (this.PipelineMode)
			{
				LogVersionEntry logEntry = new LogVersionEntry(recordKey, payload, txId);
				LogRequest request = new LogRequest(tableId, txId, logEntry, LogRequestType.WriteVersionLog);
				return this.connectionPool[txId % CassandraLogStore.DEFAULT_CONNECTION_POOL_COUNT].ProcessBoolRequest(request);
			}
			else
			{
				return this.InsertCommittedVersion(tableId, recordKey, payload, txId, commitTs);
			}
		}

		public bool WriteCommittedTx(long txId)
		{
			if (this.PipelineMode)
			{
				return this.connectionPool[txId % CassandraLogStore.DEFAULT_CONNECTION_POOL_COUNT].ProcessBoolRequest(new LogRequest(txId, LogRequestType.WriteTxLog));
			}
			else
			{
				return this.InsertCommittedTx(txId);
			}
		}

		internal bool InsertCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
		{
			if (!this.tableSet.Contains(tableId))
			{
				lock (this.tableLock)
				{
					if (!this.tableSet.Contains(tableId))
					{
						try
						{
							this.session.Execute($@"
									CREATE TABLE {CassandraLogStore.KEYSPACE + "." + tableId} (
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
				PreparedStatement ps = this.session.Prepare($@"
                        INSERT INTO {CassandraLogStore.KEYSPACE + "." + tableId} (recordKey, beginTs, txId, payload) VALUES (?, ?, ?, ?)");
				Statement statement = ps.Bind(BytesSerializer.Serialize(recordKey),
					commitTs, txId, BytesSerializer.Serialize(payload));
				session.Execute(statement);
			}
			catch (DriverException e)
			{
				return false;
			}

			return true;
		}

		internal bool InsertCommittedTx(long txId)
		{
			try
			{
				Statement statement = this.insertTxIdToLog.Bind(txId);
				session.Execute(statement);
			}
			catch (DriverException e)
			{
				return false;
			}

			return true;
		}

		internal void Dispose()
		{
			for (int i = 0; i < CassandraLogStore.DEFAULT_CONNECTION_POOL_COUNT; i++)
			{
				this.connectionPool[i].Dispose();
			}
		}
    }
}
