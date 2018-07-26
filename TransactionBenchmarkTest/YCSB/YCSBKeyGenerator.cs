using MathNet.Numerics.Distributions;
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

    internal class YCSBDataGenerator : IDataGenerator
    {
        private static int RAND_UPPER_BOUND = 10;

        /// <summary>
        /// The number of records in test
        /// </summary>
        private int recordCount;

        /// <summary>
        /// The distribute 
        /// </summary>
        private Distribution dist;

        /// <summary>
        /// random number generator for uniform distribution
        /// </summary>
        private Random random;

        /// <summary>
        /// zipf generator for zipf distribution
        /// </summary>
        private Zipf zipf;

        /// <summary>
        /// The read and write percentage
        /// </summary>
        private double readWritePerc;

        private int readWriteRandBound;

        public YCSBDataGenerator(
            int recordCount, 
            double readWritePerc = 0.5, 
            Distribution dist = Distribution.Uniform, 
            double alpha = 0.5)
        {
            this.recordCount = recordCount;
            this.dist = dist;
            this.readWritePerc = readWritePerc;
            this.readWriteRandBound = (int)(this.readWritePerc * RAND_UPPER_BOUND);

            this.random = new Random();
            if (this.dist == Distribution.Zipf)
            {
                this.zipf = new Zipf(alpha, recordCount);
            }
        }

        public int NextIntKey()
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

        public string NextStringKey()
        {
            throw new NotImplementedException();
        }

        public string NextOperation()
        {
            int randInt = random.Next(0, RAND_UPPER_BOUND);
            return (randInt < this.readWriteRandBound ? "READ" : "UPDATE");
        }

        private int NextKeyInUniform()
        {
            return random.Next(0, this.recordCount);
        }

        private int NextKeyInZipf()
        {
            return this.zipf.Sample();
        }
    }
}
