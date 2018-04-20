using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
	internal class CassandraLogConnectionPool
	{
		public static readonly int DEFAULT_BATCH_SIZE = 1;

		public static readonly long DEFAULT_WINDOW_MICRO_SEC = 100L;

		/// <summary>
		/// Request Window Size, we can reset it by the propery
		/// </summary>
		internal int RequestBatchSize { get; set; }

		/// <summary>
		/// The request pending time threshold, we can set it by the propery
		/// 100 micro sec = 0.1 milli sec
		/// </summary>
		internal long WindowMicroSec { get; set; }

		/// <summary>
		/// Request queue holding the pending requests
		/// </summary>
		internal LogRequest[] requestQueue = null;

		/// <summary>
		/// The current request index
		/// </summary>
		internal int currReqId;

		/// <summary>
		/// The status of current connection pool, we can set it by the propery
		/// </summary>
		internal bool Active { get; set; }

		/// <summary>
		/// The spin lock to ensure that enqueue and flush are exclusive
		/// </summary>
		internal SpinLock spinLock;

		public CassandraLogConnectionPool()
		{
			this.RequestBatchSize = CassandraLogConnectionPool.DEFAULT_BATCH_SIZE;
			this.WindowMicroSec = CassandraLogConnectionPool.DEFAULT_WINDOW_MICRO_SEC;
			this.Active = true;

			this.requestQueue = new LogRequest[this.RequestBatchSize];
			this.currReqId = -1;

			this.spinLock = new SpinLock();
		}

		/// <summary>
		/// Enqueues an incoming cassandra request to a queue. Queued requests are periodically sent to log store.
		/// </summary>
		/// <param name="request">The incoming request</param>
		private void EnqueueRequest(LogRequest request)
		{
			int reqId = -1;
			// Spinlock until an empty spot is available in the queue
			while (reqId < 0 || reqId >= this.RequestBatchSize)
			{
				reqId = this.currReqId + 1;
				if (reqId >= this.RequestBatchSize)
				{
					continue;
				}
				else
				{
					bool lockTaken = false;
					try
					{
						this.spinLock.Enter(ref lockTaken);
						reqId = this.currReqId + 1;
						if (reqId < this.RequestBatchSize)
						{
							this.currReqId++;
							this.requestQueue[reqId] = request;
						}
					}
					finally
					{
						if (lockTaken)
						{
							this.spinLock.Exit();
						}
					}
				}
			}
		}

		internal void Flush()
		{
			// Send queued requests to Cassandra, collect results and store each of them in the corresonding request
			for (int reqId = 0; reqId <= this.currReqId; reqId++)
			{
				LogRequest req = this.requestQueue[reqId];
				if (req.Type == LogRequestType.WriteTxLog)
				{
					req.IsSuccess = CassandraLogStore.Instance.InsertCommittedTx(req.TxId);
					req.Finished = true;
				}
				else
				{
					LogVersionEntry entry = req.LogEntry;
					req.IsSuccess = CassandraLogStore.Instance.InsertCommittedVersion(req.TableId, entry.RecordKey, entry.Payload, req.TxId, entry.CommitTs);
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

		/// <summary>
		/// A daemon thread invokes the Monitor() method to monitor the request queue,  
		/// periodically flushes queued request to log store, and get back results for each request.
		/// </summary>
		public void Monitor()
		{
			long lastFlushTime = DateTime.Now.Ticks / 10;
			while (this.Active)
			{
				long now = DateTime.Now.Ticks / 10;
				if (now - lastFlushTime >= this.WindowMicroSec ||
					this.currReqId + 1 >= this.RequestBatchSize)
				{
					if (this.currReqId >= 0)
					{
						bool lockTaken = false;
						try
						{
							this.spinLock.Enter(ref lockTaken);
							this.Flush();
						}
						finally
						{
							if (lockTaken)
							{
								this.spinLock.Exit();
							}
						}
					}
					lastFlushTime = DateTime.Now.Ticks / 10;
				}
			}

			if (this.currReqId >= 0)
			{
				bool lockTaken = false;
				try
				{
					this.spinLock.Enter(ref lockTaken);
					this.Flush();
				}
				finally
				{
					if (lockTaken)
					{
						this.spinLock.Exit();
					}
				}
			}
		}

		public void Dispose()
		{
			this.Active = false;
		}

		internal bool ProcessBoolRequest(LogRequest request)
		{
			this.EnqueueRequest(request);
			lock (request)
			{
				while (!request.Finished)
				{
					System.Threading.Monitor.Wait(request);
				}
			}

			return request.IsSuccess;
		}
	}
}
