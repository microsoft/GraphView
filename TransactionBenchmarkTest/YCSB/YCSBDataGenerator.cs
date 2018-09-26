namespace TransactionBenchmarkTest.YCSB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

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
        /// The uniform generator for operations
        /// </summary>
        private Uniform operationDist;

        /// <summary>
        /// Uniform generator for uniform distribution
        /// </summary>
        private Uniform uniform;

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
            double theta = 0.5)
        {
            this.recordCount = recordCount;
            this.dist = dist;
            this.readWritePerc = readWritePerc;
            this.readWriteRandBound = (int)(this.readWritePerc * RAND_UPPER_BOUND);

            if (this.dist == Distribution.Zipf)
            {
                // Console.WriteLine("theta = {0}", theta);
                this.zipf = new Zipf(recordCount, theta);
            }
            else
            {
                this.uniform = new Uniform(recordCount);
            }
            this.operationDist = new Uniform(RAND_UPPER_BOUND);
        }

        public int NextIntKey()
        {
            switch (this.dist)
            {
                case Distribution.Uniform:
                    return this.uniform.Next();

                case Distribution.Zipf:
                    return this.zipf.Next();

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
            int randInt = operationDist.Next();
            return (randInt < this.readWriteRandBound ? "READ" : "UPDATE");
        }
    }
}
