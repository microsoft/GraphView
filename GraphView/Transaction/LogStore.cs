using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
	public abstract class LogStore : ILogStore
	{
		/// <summary>
		/// Provide an option to set log store in pipelineMode or not
		/// </summary>
		public bool PipelineMode { get; set; }

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
		/// The status of current log store, we can set it by the propery
		/// </summary>
		internal bool Active { get; set; }

		/// <summary>
		/// The spin lock to ensure that enqueue and flush are exclusive
		/// </summary>
		internal SpinLock spinLock;

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

		internal virtual void Flush()
		{
			throw new NotImplementedException();
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

		private bool ProcessBoolRequest(LogRequest request)
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

		public bool WriteCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
		{
			if (this.PipelineMode)
			{
				LogVersionEntry logEntry = new LogVersionEntry(recordKey, payload, txId);
				LogRequest request = new LogRequest(tableId, txId, logEntry, LogRequestType.WriteVersionLog);
				return this.ProcessBoolRequest(request);
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
				return this.ProcessBoolRequest(new LogRequest(txId, LogRequestType.WriteTxLog));
			}
			else
			{
				return this.InsertCommittedTx(txId);
			}
		}

		internal virtual bool InsertCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
		{
			throw new NotImplementedException();
		}

		internal virtual bool InsertCommittedTx(long txId)
		{
			throw new NotImplementedException();
		}
	}
}
