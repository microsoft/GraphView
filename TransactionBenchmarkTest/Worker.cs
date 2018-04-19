namespace TransactionBenchmarkTest
{
    using System;
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

        public Worker(int workerId, int queueSize = -1)
        {
            this.WorkerId = workerId;
            this.TaskQueueSize = queueSize == -1 ? Worker.DEFAULT_QUEUE_SIZE : queueSize;
            this.txTaskQueue = new TxTask[this.TaskQueueSize];
            this.taskCount = 0;
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
            for (int i = 0; i < this.taskCount; i++)
            {
                txTaskQueue[i].Run();
            }
        }
    }
}
