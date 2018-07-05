using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using GraphView.Transaction;
using ServiceStack.Redis.Pipeline;

namespace TransactionBenchmarkTest.TPCC
{
    class TestRedis
    {
        static void Main(string[] args)
        {
            LocalRedisBenchmarkTest();
        }

        static void LocalRedisBenchmarkTest()
        {
            int count = 100000, pipelineSize = 100;
            Random rand = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Func<int, string> RandomString = (int length) =>
            {
                return new string(Enumerable.Repeat(chars, length)
                    .Select(s => s[rand.Next(s.Length)]).ToArray());
            };

            Func<int, byte[]> RandomBytes = (int length) =>
            {
                byte[] value = new byte[length];
                rand.NextBytes(value);
                return value;
            };

            RedisVersionDb redisVersionDb = RedisVersionDb.Instance();

            // Non-Pipeline Mode
            using (RedisClient client = redisVersionDb.RedisManager.GetClient(3, 0))
            {
                long now = DateTime.Now.Ticks;
                for (int i = 0; i < count; i++)
                {
                    string hashId = RandomString(3);
                    byte[] key = BitConverter.GetBytes(3);
                    byte[] value = RandomBytes(50);
                    int type = rand.Next(0, 1);
                    if (type == 0)
                    {
                        client.HSet(hashId, key, value);
                    }
                    else
                    {
                        client.HGet(hashId, key);
                    }
                }

                long time = DateTime.Now.Ticks - now;
                int throughput = (int)((count * 1.0) / (time * 1.0 / 10000000));

                Console.WriteLine("Redis Local Non Pipeline Throughput: {0} ops/s", throughput);
            }

            // Pipeline Mode
            
            using (RedisClient client = redisVersionDb.RedisManager.GetClient(3, 0))
            {
                long now = DateTime.Now.Ticks;

                int i = 0;
                while (i < count)
                {
                    using (IRedisPipeline pipeline = client.CreatePipeline())
                    {
                        for (int j = 0; j < pipelineSize; j++)
                        {
                            string hashId = RandomString(3);
                            byte[] key = BitConverter.GetBytes(3);
                            byte[] value = RandomBytes(50);
                            int type = rand.Next(0, 1);
                            if (type == 0)
                            {
                                pipeline.QueueCommand(
                                   r => ((RedisNativeClient)r).HSet(hashId, key, value));
                            }
                            else
                            {
                                pipeline.QueueCommand(
                                   r => ((RedisNativeClient)r).HGet(hashId, key));
                            }
                        }
                        pipeline.Flush();
                    }

                    i += pipelineSize;
                }
                long time = DateTime.Now.Ticks - now;
                int throughput = (int)((count * 1.0) / (time * 1.0 / 10000000));

                Console.WriteLine("Redis Local Pipeline({0}) Throughput: {1} ops/s", pipelineSize, throughput);
            }
        }

    }
}
