namespace TransactionBenchmarkTest
{
    using GraphView.Transaction;
    using System;
	using System.Collections.Generic;
	using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    internal class Worker
    {
        public static readonly int DEFAULT_QUEUE_SIZE = 10000;

        /// <summary>
        /// The task queue size, can be set by constructor parameters
        /// </summary>
        internal int TaskQueueSize { get; set; }

        /// <summary>
        /// The worker Id
        /// </summary>
        internal int WorkerId;

        /// <summary>
        /// Task queue
        /// </summary>
        private TxTask[] txTaskQueue;

        /// <summary>
        /// The number of tasks
        /// </summary>
        private int taskCount;

		/// <summary>
		/// The exact ticks when the test starts
		/// </summary>
		private long testBeginTicks;

		/// <summary>
		/// The exact ticks when then test ends
		/// </summary>
		private long testEndTicks;

		internal double RunSeconds
		{
			get
			{
				return ((this.testEndTicks - this.testBeginTicks) * 1.0) / 10000000;
			}
		}

		internal List<double> throuputPerOneMillion;

		internal int FinishedTxs { get; private set; }

        internal int AbortedTxs { get; private set; }

        public Worker(int workerId, int queueSize = -1)
        {
            this.WorkerId = workerId;
            this.TaskQueueSize = Math.Max(queueSize, DEFAULT_QUEUE_SIZE);
            this.txTaskQueue = new TxTask[this.TaskQueueSize];
            this.taskCount = 0;

			this.throuputPerOneMillion = new List<double>();
        }

        internal void EnqueueTxTask(TxTask task)
        {
            int taskId = this.taskCount;
            if (taskId >= this.TaskQueueSize)
            {
                throw new IndexOutOfRangeException("The task queue is full now");
            }

            this.txTaskQueue[taskId] = task;
            this.taskCount++;
        }

        internal void Run()
        {
            // this.PinThreadOnCores();
            this.testBeginTicks = DateTime.Now.Ticks;
            for (int i = 0; i < this.taskCount; i++)
            {
                bool commited = (bool)txTaskQueue[i].Run();
                this.FinishedTxs++;
                if (!commited)
                {
                    this.AbortedTxs++;
                }
			}
			this.testEndTicks = DateTime.Now.Ticks;
		}

        internal void RunTxOnly()
        {
            VersionDb vdb = RedisVersionDb.Instance();

            for (int i = 0; i < this.txTaskQueue.Length; i++)
            {
                long txId = vdb.InsertNewTx();
                //vdb.UpdateCommitLowerBound(txId, 50);
                //vdb.SetAndGetCommitTime(txId, 90);
                //vdb.UpdateTxStatus(txId, TxStatus.Committed);
                
                this.FinishedTxs++;
            }
        }

        internal void PinThreadOnCores()
        {
            Thread.BeginThreadAffinity();
            Process Proc = Process.GetCurrentProcess();
            foreach (ProcessThread pthread in Proc.Threads)
            {
                if (pthread.Id == AppDomain.GetCurrentThreadId())
                {
                    long AffinityMask = (long)Proc.ProcessorAffinity;
                    AffinityMask &= 0x000F;
                    // AffinityMask &= 0x007F;
                    pthread.ProcessorAffinity = (IntPtr)AffinityMask;
                }
            }

            Thread.EndThreadAffinity();
        }
    }
}
