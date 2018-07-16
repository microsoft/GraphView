using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    internal enum Distribution
    {
        Uniform,
        Zipf
    }

    internal class YCSBKeyGenerator
    {
        /// <summary>
        /// The number of records in test
        /// </summary>
        private int recordCount;

        /// <summary>
        /// The distribute 
        /// </summary>
        private Distribution dist;

        private Random random;

        public YCSBKeyGenerator(int recordCount, Distribution dist = Distribution.Uniform)
        {
            this.recordCount = recordCount;
            this.dist = dist;
            this.random = new Random();
        }

        internal int Next()
        {
            switch (this.dist)
            {
                case Distribution.Uniform:
                    return this.NextKeyInUniform();

                case Distribution.Zipf:
                    return this.NextKeyInZipf();

                default:
                    return -1;
            }
        }

        private int NextKeyInUniform()
        {
            return random.Next() % this.recordCount;
        }

        private int NextKeyInZipf()
        {
            throw new NotImplementedException();
        }
    }
}
