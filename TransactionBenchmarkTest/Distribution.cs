namespace TransactionBenchmarkTest
{
    using System;
    using System.Threading;

    /// <summary>
    /// A thread-safe zipf distribution generator based on the following paper.
    /// https://jimgray.azurewebsites.net/papers/SyntheticDataGen.pdf
    /// and tictoc code
    /// https://github.com/shingjan/DBx1000/blob/master/benchmarks/ycsb_query.cpp#L24-L58
    /// </summary>
    /// 

    internal interface IDistribution
    {
        int Next();
    }

    internal class Zipf : IDistribution
    {
        private static int seed = Environment.TickCount;

        private static readonly ThreadLocal<Random> random =
            new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

        private int n;

        private double theta;

        /// <summary>
        /// Motiviations: By following the above algorithm, the generated zipf distribution has the
        /// such a feature. The key 0 always has the largest frequency, and 1 has the second largest, 
        /// and so one. It means from key 0 to key n, its frequencies are in descending order.
        /// In some cases, we don't hope it's like this. We hope those keys with high frequency are 
        /// distributed uniformly. Hence we put an random map here, to convert generated keys to the uniform
        /// keys. 
        /// </summary>
        private readonly int[] keysMap;

        /// <summary>
        /// A switch to control whether convert high frequency keys' distribution
        /// </summary>
        private bool reshuffleKeys;

        /// <summary>
        /// zeta(n, theta)
        /// </summary>
        private double zetan;

        /// <summary>
        /// zeta(2, theta)
        /// </summary>
        private double zeta2Theta;

        public Zipf(int n, double theta, bool reshuffleKeys = true)
        {
            this.n = n;
            this.theta = theta;
            this.reshuffleKeys = reshuffleKeys;
            
            this.zetan = Zeta(n, theta);
            this.zeta2Theta = Zeta(2, theta);

            this.keysMap = new int[this.n];

            // TODO: Here ensure that those random numbers in 5 second are same. 
            // This is a special requirement as we want to generate 
            // same numbers in zipf distribution to run with multiple processes.
            // Those processes will be started by batch scripts and ensure they will be 
            // loaded in 5 seconds.

            // If you don't need it, just new rand without any params
            int seed = (int)(DateTime.Now.Ticks / 50000000);
            Random rand = new Random(seed);
            for (int i = 0; i < n; i++)
            {
                this.keysMap[i] = i;
            }

            // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
            for (int t = n - 1; t > 0; t--)
            {
                int tmp = keysMap[t];
                int r = rand.Next(t + 1);
                keysMap[t] = keysMap[r];
                keysMap[r] = tmp;
            }
        }

        /// <summary>
        /// Generate integers from 0 to n-1 on zipf distribution
        /// </summary>
        /// <returns></returns>
        public int Next()
        {
            int key = this.GenZipf(this.n, this.theta) - 1;
            if (this.reshuffleKeys)
            {
                return this.keysMap[key];
            }
            return key;
        }

        private double Zeta(int n, double theta)
        {
            double sum = 0;
            for (int i = 1; i <= n; i++)
                sum += Math.Pow(1.0 / i, theta);
            return sum;
        }

        /// <summary>
        /// generate integers from 1 to n
        /// </summary>
        /// <param name="n"></param>
        /// <param name="theta"></param>
        /// <returns></returns>
        private int GenZipf(int n, double theta)
        {
            double alpha = 1 / (1 - theta);
            double eta = (1 - Math.Pow(2.0 / n, 1 - theta)) /
                (1 - zeta2Theta / zetan);
            double u = random.Value.NextDouble();
            double uz = u * zetan;
            if (uz < 1) return 1;
            if (uz < 1 + Math.Pow(0.5, theta)) return 2;
            return 1 + (int)(n * Math.Pow(eta * u - eta + 1, alpha));
        }
    }

    class Uniform : IDistribution
    {
        private static int seed = Environment.TickCount + 0x3f3f3f3f;

        private static readonly ThreadLocal<Random> random =
            new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

        private int n;

        public Uniform(int n)
        {
            this.n = n;
        }

        /// <summary>
        /// generate numbers from 0 to n-1 on uniform distribution
        /// </summary>
        /// <returns></returns>
        public int Next()
        {
            return random.Value.Next(0, n);
        }
    }
}
