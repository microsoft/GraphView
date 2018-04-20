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
    public class CassandraLogStore : LogStore
    {
		public static readonly int DEFAULT_BATCH_SIZE = 1;

		public static readonly long DEFAULT_WINDOW_MICRO_SEC = 100L;

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

			this.RequestBatchSize = CassandraLogStore.DEFAULT_BATCH_SIZE;
			this.WindowMicroSec = CassandraLogStore.DEFAULT_WINDOW_MICRO_SEC;
			this.Active = true;

			this.requestQueue = new LogRequest[this.RequestBatchSize];
			this.currReqId = -1;

			this.spinLock = new SpinLock();
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

		internal override void Flush()
		{
			// Send queued requests to Cassandra, collect results and store each of them in the corresonding request
			for (int reqId = 0; reqId <= this.currReqId; reqId++)
			{
				LogRequest req = this.requestQueue[reqId];
				if (req.Type == LogRequestType.WriteTxLog)
				{
					req.IsSuccess = this.InsertCommittedTx(req.TxId);
					req.Finished = true;
				}
				else
				{
					LogVersionEntry entry = req.LogEntry;
					req.IsSuccess = this.InsertCommittedVersion(req.TableId, entry.RecordKey, entry.Payload, req.TxId, entry.CommitTs);
					req.Finished = true;
				}
			}

			// Release the request lock to make sure processRequest can keep going
			for (int reqId = 0; reqId <= this.currReqId; reqId++)
			{
				// Monitor.Wait must be called in sync block, here we should lock the 
				// request and release the it on time
				lock (this.requestQueue[reqId])
				{
					System.Threading.Monitor.PulseAll(this.requestQueue[reqId]);
				}
			}

			this.currReqId = -1;
		}

		internal override bool InsertCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
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

		internal override bool InsertCommittedTx(long txId)
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
    }
}
