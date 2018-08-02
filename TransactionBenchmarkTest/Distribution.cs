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
        /// zeta(n, theta)
        /// </summary>
        private double zetan;

        /// <summary>
        /// zeta(2, theta)
        /// </summary>
        private double zeta2Theta;

        public Zipf(int n, double theta)
        {
            this.n = n;
            this.theta = theta;

            this.zetan = Zeta(n, theta);
            this.zeta2Theta = Zeta(2, theta);
        }

        /// <summary>
        /// Generate integers from 0 to n-1 on zipf distribution
        /// </summary>
        /// <returns></returns>
        public int Next()
        {
            return this.GenZipf(this.n, this.theta) - 1;
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
        private static int seed = Environment.TickCount;

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
