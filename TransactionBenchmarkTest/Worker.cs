namespace TransactionBenchmarkTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal class Worker : IDisposable
    {
        public static readonly int DEFAULT_QUEUE_SIZE = 10000;

        internal bool Active { get; set; }

        internal int TaskQueueSize { get; set; }

        /// <summary>
        /// A flag to declare whether the worker has producer and consumer at the same time:
        /// (1) True: all tasks have been enqueued in advance, 
        ///           no concurrent threads add tasks when the daemon thread is working
        /// (2) False: both producer and consumer are working at the same time
        /// </summary>
        internal bool OnlyConsumer { get; set; } = true;

        internal int RemainedTasks { get; private set; }

        internal int FinishedTasks { get; private set; }

        internal long TasksBeginTicks { get; private set; }

        internal long TasksEndTicks { get; private set; }

        internal bool Finished { get; private set; } = false;

        internal int WorkerId;

        private Task<object>[] txTaskQueue;

        private int currTxId;

        private int taskCount;

        private object waitExecutionLock = new object();
        /// <summary>
        /// The spinlock for the sync of task Queue
        /// </summary>
        private SpinLock spinLock;

        public Worker(int workerId, int queueSize = -1)
        {
            this.WorkerId = workerId;
            this.TaskQueueSize = queueSize == -1 ? Worker.DEFAULT_QUEUE_SIZE : queueSize;
            this.txTaskQueue = new Task<object>[this.TaskQueueSize];
            this.currTxId = -1;
            this.taskCount = 0;
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
                this.taskCount++;
            }
            else
            {
                // TODO
            }
        }

        internal void Monitor()
        {
            this.RemainedTasks = this.taskCount;
            this.FinishedTasks = 0;
            this.TasksBeginTicks = DateTime.Now.Ticks;
            this.TasksEndTicks = -1;

            while (this.Active)
            {
                if (this.OnlyConsumer)
                {
                    if (this.currTxId >= 0)
                    {
                        Task task = this.txTaskQueue[this.currTxId--];
                        task.Start();
                        task.Wait();

                        this.RemainedTasks--;
                        this.FinishedTasks++;
                    }
                    else
                    {
                        if (this.TasksEndTicks == -1)
                        {
                            this.TasksEndTicks = DateTime.Now.Ticks;
                            this.Finished = true;
                            lock (this.waitExecutionLock)
                            {
                                System.Threading.Monitor.PulseAll(this.waitExecutionLock);
                            }
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
