using GraphView.Transaction;
using ServiceStack.Redis;
using ServiceStack.Redis.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    class RedisCommand
    {
        internal string hashId;
        internal byte[] key;
        internal byte[] value;
        internal int type;
    }

    class RedisBenchmarkTest : IDisposable
    {
        public static readonly long REDIS_DB_INDEX = 3L;

        public static Func<object, object> ACTION = (object obj) =>
        {
            RedisCommand cmd = (RedisCommand)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                if (cmd.type == 0)
                {
                    client.HSet(cmd.hashId, cmd.key, cmd.value);
                }
                else
                {
                    client.HGet(cmd.hashId, cmd.key);
                }
            }
            return null;
        };

        public static Func<object, object> PIPELINE_ACTION = (object obj) =>
        {
            List<RedisCommand> commands = (List<RedisCommand>)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                using (IRedisPipeline pipe = client.CreatePipeline())
                {
                    foreach (RedisCommand cmd in commands)
                    {
                        if (cmd.type == 0)
                        {
                            pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(cmd.hashId, cmd.key, cmd.value));
                        }
                        else
                        {
                            pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(cmd.hashId, cmd.key));
                        }
                    }
                    pipe.Flush();
                }
            }
            return null;
        };

        public static Random RAND = new Random();

        public static string CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public static Func<int, string> RandomString = (int length) =>
        {
            return new string(Enumerable.Repeat(CHARS, length)
                .Select(s => s[RAND.Next(s.Length)]).ToArray());
        };

        public static Func<int, byte[]> RandomBytes = (int length) =>
        {
            byte[] value = new byte[length];
            RAND.NextBytes(value);
            return value;
        };

        private int workerCount;

        private int workerTaskCount;

        private List<Worker> workers;

        private Dictionary<int, int> lastFinishedTasks;

        private bool pipelineMode;

        private int pipelineSize;

        public RedisBenchmarkTest(int workerCount, int taskCount, bool pipelineMode = false, int pipelineSize = 100)
        {
            this.workerCount = workerCount;
            this.workerTaskCount = taskCount;
            this.lastFinishedTasks = new Dictionary<int, int>();
            this.workers = new List<Worker>();
            this.pipelineMode = pipelineMode;
            this.pipelineSize = pipelineSize;

            for (int i = 0; i < workerCount; i++)
            {
                this.workers.Add(new Worker(i + 1, Math.Max(taskCount, Worker.DEFAULT_QUEUE_SIZE)));
                this.lastFinishedTasks[i+1] = 0;
            }
        }

        internal void Setup()
        {
            RedisClientManager manager = RedisClientManager.Instance;
            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushDb();
            }
            Console.WriteLine("Flushed the database");

            foreach (Worker worker in workers)
            {
                // non-pipeline mode
                if (!this.pipelineMode)
                {
                    for (int i = 0; i < this.workerTaskCount; i++)
                    {
                        RedisCommand cmd = new RedisCommand();
                        cmd.hashId = RandomString(4);
                        cmd.key = BitConverter.GetBytes(3);
                        cmd.value = RandomBytes(50);
                        cmd.type = RAND.Next(0, 1);

                        worker.EnqueueTxTask(new Task<object>(ACTION, cmd));
                    }
                }
                // pipeline mode
                else
                {
                    int batchs = this.workerTaskCount / this.pipelineSize;
                    for (int i = 0; i < batchs; i++)
                    {
                        List<RedisCommand> cmds = new List<RedisCommand>(this.pipelineSize);
                        for (int j = 0; j < this.pipelineSize; j++)
                        {
                            RedisCommand cmd = new RedisCommand();
                            cmd.hashId = RandomString(4);
                            cmd.key = BitConverter.GetBytes(3);
                            cmd.value = RandomBytes(50);
                            cmd.type = RAND.Next(0, 1);

                            cmds.Add(cmd);
                        }
                        worker.EnqueueTxTask(new Task<object>(PIPELINE_ACTION, cmds));
                    }
                }
                
            }
            Console.WriteLine("Filled the workers' queue");
        }

        internal void Run()
        {
            foreach(Worker worker in this.workers)
            {
                worker.Active = true;
                Thread thread = new Thread(new ThreadStart(worker.Monitor));
                thread.Start();
            }
        }

        /// <summary>
        /// Compute the throughout from the last time call this function.
        /// If at least a worker is finished, it will return -1
        /// Otherwrise return thr throughput
        /// </summary>
        /// <returns></returns>
        internal int ComputeThroughput()
        {
            int throughput = 0;
            foreach (Worker worker in workers)
            {
                if (worker.Finished)
                {
                    return -1;
                }

                int finishedTask = worker.FinishedTasks;
                int tasks = finishedTask - this.lastFinishedTasks[worker.WorkerId];
                this.lastFinishedTasks[worker.WorkerId] = finishedTask;
                throughput += tasks;
            }
            return throughput;
        }

        public void Dispose()
        {
            foreach (Worker worker in workers)
            {
                worker.Active = false;
            }
        }
    }
}
