namespace TransactionBenchmarkTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    internal class Worker : IDisposable
    {
        public static readonly int DEFAULT_QUEUE_SIZE = 10000;

        private Task<object>[] txTaskQueue;

        private int workerId;

        private int currTxId;

        private int taskCount;
        /// <summary>
        /// The spinlock for the sync of task Queue
        /// </summary>
        private SpinLock spinLock;

        /// <summary>
        /// The status of current Worker, close the 
        /// </summary>
        internal bool Active { get; set; }

        internal int TaskQueueSize { get; set; }

        private object waitExecutionLock = new object();

        internal int Throughput
        {
            get
            {
                lock (this.waitExecutionLock)
                {
                    while (this.ExecutionTime == -1)
                    {
                        System.Threading.Monitor.Wait(this.waitExecutionLock);
                    };
                }
                return (int)(this.taskCount/this.ExecutionTime);
            }
        }

        /// <summary>
        /// A flag to declare whether the worker has producer and consumer at the same time:
        /// (1) True: all tasks have been enqueued in advance, 
        ///           no concurrent threads add tasks when the daemon thread is working
        /// (2) False: both producer and consumer are working at the same time
        /// </summary>
        internal bool OnlyConsumer { get; set; } = true;

        internal double ExecutionTime { get; set; } = -1;

        public Worker(int workerId, int queueSize = -1)
        {
            this.workerId = workerId;
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

                        //if (this.currTxId % 10 == 0)
                        //{
                        //    Console.WriteLine("Worker {0} finished {1}", this.workerId, this.taskCount - this.currTxId);
                        //}
                    }
                    else
                    {
                        if (endTime == -1)
                        { 
                            endTime = DateTime.Now.Ticks;
                            this.ExecutionTime = (endTime - beginTime)*1.0/10000000;
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
