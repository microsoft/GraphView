﻿using NDesk.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    internal class BenchmarkTestConfig
    {
        /// <summary>
        /// The number of workers in the test
        /// </summary>
        internal int WorkerCount { get; private set; } = 4;

        /// <summary>
        /// Whether to load records before the test
        /// </summary>
        internal bool LoadRecords { get; private set; } = true;

        /// <summary>
        /// Whether to clear the version db before the test
        /// </summary>
        internal bool ClearVersionDb { get; private set; } = true;

        /// <summary>
        /// The type of stored procedure
        /// </summary>
        internal string Type { get; private set; } = "READ";

        /// <summary>
        /// The number of records totally
        /// </summary>
        internal int RecordCount { get; private set; } = 100000;

        /// <summary>
        /// The number of operations per worker
        /// </summary>
        internal int OperationCount { get; private set; } = 100000;

        /// <summary>
        /// The pipeline size for redis batch commands
        /// </summary>
        internal int PipelineSize { get; private set; } = 50;

        /// <summary>
        /// The scale to execute zipf distribution
        /// </summary>
        internal double Scale { get; private set; } = 0.5;

        /// <summary>
        /// Whether to run the test
        /// </summary>
        internal bool RunTest { get; private set; } = true;

        /// <summary>
        /// The redis instance host
        /// </summary>
        internal string RedisHost { get; private set; } = 
            "xnke5SdHz5xcsBF+OlZPL7PdzI7Vz3De7ntGI2fIye0=@elastas.redis.cache.windows.net:6379";

        /// <summary>
        /// The distribution of keys
        /// </summary>
        internal Distribution Dist { get; private set; } = Distribution.Uniform;

        /// <summary>
        /// The percentage of read in all workloads
        /// </summary>
        internal double ReadPercentage { get; private set; } = 0.5;

        public BenchmarkTestConfig(string[] givenParams = null)
        {
            if (givenParams == null)
            {
                return;
            }

            this.Parse(givenParams);
        }

        private void Parse(string[] args)
        {
            OptionSet optionSet = new OptionSet()
            {
                {
                    "r|record=", "the number of records", v => this.RecordCount = int.Parse(v)
                },
                {
                    "o|operation=", "the number of operations per worker", v => this.OperationCount = int.Parse(v)
                },
                {
                    "w|worker=", "the number of workers", v => this.WorkerCount = int.Parse(v)
                },
                {
                    "p|pipeline=", "the batch size under pipeline mode", v => this.PipelineSize = int.Parse(v)
                },
                {
                    "t|type=", "the workload type for YCSB", v =>
                    {
                        switch(v)
                        {
                            case "read":
                                this.Type = "READ";
                                break;
                            case "update":
                                this.Type = "UPDATE";
                                break;
                            case "hybrid":
                                this.Type = "HYBRID";
                                break;

                            default:
                                throw new ArgumentException("wrong workload type");
                        }
                    }
                },
                {
                    "s|scale=", "the scale option of zipf distribution", v => this.Scale = float.Parse(v)
                },
                {
                    "l|load=", "whether loading data", v => this.LoadRecords = ("true".Equals(v) ? true : false)
                },
                {
                    "c|clear=", "whether clear the versionDb", v => this.ClearVersionDb = ("true".Equals(v) ? true : false)
                },
                {
                    "r|run=", "whether to run the test", v => this.RunTest = ("true".Equals(v) ? true : false)
                },
                {
                    "h|host=", "the redis connection string", v => this.RedisHost = v
                },
                {
                    "d|dist=", "the distribution of generated keys", v =>
                    {
                        switch(v)
                        {
                            case "uniform":
                                this.Dist = Distribution.Uniform;
                                break;
                            case "zipf":
                                this.Dist = Distribution.Zipf;
                                break;
                            default:
                                throw new ArgumentException("wrong key distribution");
                        }
                    }
                },
                {
                    "rp|readperc=", "the percentage of read", v => double.Parse(v)
                }
            };

            optionSet.Parse(args);
        }

        public override string ToString()
        {
            return String.Format(
                "\n----------------------------------" + 
                "\nWorkerCount: {0}" +
                "\nRecordCount: {1}" +
                "\nTxCount: {2}" +
                "\nTxType: {3}" +
                "\n" + 
                "\nLoadRecords: {4}" +
                "\nClearVersionDb: {5}" +
                "\nRunTest: {6}" +
                "\n" + 
                "\nPipeline: {7}" +
                "\nScale: {8}" +
                "\nHost: {9}" +
                "\nDistribution: {10}" +
                "\nRead Percentage: {11}" + 
                "\n----------------------------------",
                this.WorkerCount,
                this.RecordCount,
                this.OperationCount,
                this.Type,
                this.LoadRecords,
                this.ClearVersionDb,
                this.RunTest,
                this.PipelineSize,
                this.Scale,
                this.RedisHost,
                this.Dist,
                this.ReadPercentage);
        }
    }
}