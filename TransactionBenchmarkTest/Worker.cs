namespace TransactionBenchmarkTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal class Worker : IDisposable
    {
        public static readonly int DEFAULT_QUEUE_SIZE = 10000;

        private Task<object>[] txTaskQueue;

        private int currTxId;

        /// <summary>
        /// The spinlock for the sync of task Queue
        /// </summary>
        private SpinLock spinLock;

        /// <summary>
        /// The status of current Worker, close the 
        /// </summary>
        internal bool Active { get; set; }

        internal int TaskQueueSize { get; set; }

        /// <summary>
        /// A flag to declare whether the worker has producer and consumer at the same time:
        /// (1) True: all tasks have been enqueued in advance, 
        ///           no concurrent threads add tasks when the daemon thread is working
        /// (2) False: both producer and consumer are working at the same time
        /// </summary>
        internal bool OnlyConsumer { get; set; } = true;

        internal double ExecutionTime { get; set; } = -1;

        public Worker(int queueSize = -1)
        {
            this.TaskQueueSize = queueSize == -1 ? Worker.DEFAULT_QUEUE_SIZE : queueSize;
            this.txTaskQueue = new Task<object>[this.TaskQueueSize];
            this.currTxId = -1;
            this.spinLock = new SpinLock();
        }

        internal void EnqueueTxTask(Task<object> task)
        {
            // there is only a consumer working
            if (this.OnlyConsumer)
            {
                int taskId = this.currTxId + 1;
                if (taskId >= this.TaskQueueSize)
                {
                    throw new IndexOutOfRangeException("The task queue is full now");
                }

                this.txTaskQueue[taskId] = task;
                this.currTxId++;
            }
            else
            {
                // TODO
            }
        }

        internal void Monitor()
        {
            long beginTime = DateTime.Now.Ticks;
            long endTime = -1; 
            while (this.Active)
            {
                if (this.OnlyConsumer)
                {
                    if (this.currTxId >= 0)
                    {
                        Task task = this.txTaskQueue[this.currTxId--];
                        task.Start();
                        task.Wait();
                    }
                    else
                    {
                        if (endTime == -1)
                        { 
                            endTime = DateTime.Now.Ticks;
                            this.ExecutionTime = (endTime - beginTime)*1.0/10000000;
                        }
                    }
                }
                else
                {
                    // TODO
                }
            }
        }

        public void Dispose()
        {
            this.Active = false;
        }
    }
}
