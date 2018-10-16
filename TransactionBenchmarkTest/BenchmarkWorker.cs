using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest
{
    abstract class BenchmarkWorker
    {
        public BenchmarkWorker()
        {
            this.commitCount = 0;
            this.abortCount = 0;
        }
        public abstract void Run();

        public int commitCount;
        public int abortCount;
        public volatile bool isFinished;
    }
    internal class WorkerMonitor
    {
        struct WorkerState
        {
            public long timeTicks;
            public int abortNum;
            public int commitNum;
            public int threadsAlive;

            public PeriodResult Difference(WorkerState lastState)
            {
                return new PeriodResult
                {
                    dTime = (this.timeTicks - lastState.timeTicks) / 10000000.0,
                    dAbort = this.abortNum - lastState.abortNum,
                    dCommit = this.commitNum - lastState.commitNum,
                    threadsAlive = this.threadsAlive
                };
            }
        }
        struct PeriodResult
        {
            public double dTime;
            public int dAbort;
            public int dCommit;
            public int threadsAlive;

            public int Finished
            {
                get { return this.dAbort + this.dCommit; }
            }
            public int Throughput
            {
                get { return (int)(this.Finished / this.dTime); }
            }
            public double AbortRate
            {
                get { return this.dAbort * 1.0 / this.Finished; }
            }
            public void Print()
            {
                Console.WriteLine($"Time:{this.dTime:F3}|Count:{this.Finished}|Throughput:{this.Throughput}|AbortRate:{this.AbortRate:F3}|ThreadsAlive:{this.threadsAlive}");
            }
        }
        public WorkerMonitor(BenchmarkWorker[] workers)
        {
            this.workers = workers;
        }
        public void StartBlocking(int intervalInMs)
        {
            this.throughputs = new List<int>(30 * 1000 / intervalInMs);
            WorkerState lastState = Capture();
            for (; lastState.threadsAlive != 0;)
            {
                System.Threading.Thread.Sleep(intervalInMs);
                WorkerState currentState = Capture();
                PeriodResult tempResult = currentState.Difference(lastState);
                tempResult.Print();
                this.throughputs.Add(tempResult.Throughput);
                lastState = currentState;
            }
        }
        private WorkerState Capture()
        {
            WorkerState state = new WorkerState();
            state.threadsAlive = this.workers.Length;
            state.abortNum = 0;
            state.commitNum = 0;
            state.timeTicks = DateTime.Now.Ticks;
            for (int i = 0; i < workers.Length; ++i)
            {
                var worker = workers[i];
                state.abortNum += worker.abortCount;
                state.commitNum += worker.commitCount;
                if (worker.isFinished) --state.threadsAlive;
            }
            return state;
        }
        public int SuggestThroughput()
        {
            Console.WriteLine($"Capture {throughputs.Count} times");
            int validSampleNum = this.throughputs.Count / 4;
            int[] validSamples = throughputs
                .Skip(throughputs.Count / 10)
                .OrderByDescending(a => a)
                .Take(validSampleNum).ToArray();
            return validSamples[validSampleNum / 2];
        }

        private List<int> throughputs;
        private BenchmarkWorker[] workers;
    }
}
