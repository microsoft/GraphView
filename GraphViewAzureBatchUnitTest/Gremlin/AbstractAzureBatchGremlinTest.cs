using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StartAzureBatch;

namespace GraphViewAzureBatchUnitTest.Gremlin
{
    [TestClass]
    public class AbstractAzureBatchGremlinTest
    {
        protected AzureBatchJobManager jobManager;
        protected GraphViewAzureBatchJob job;

        internal const bool TEST_USE_REVERSE_EDGE = true;
        internal const string TEST_PARTITION_BY_KEY = "name";
        internal const int TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI = 1;

        [TestInitialize]
        public void Setup()
        {
            string batchAccountName = ConfigurationManager.AppSettings["BatchAccountName"];
            string batchAccountKey = ConfigurationManager.AppSettings["BatchAccountKey"];
            string batchAccountUrl = ConfigurationManager.AppSettings["BatchAccountUrl"];
            string storageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
            string storageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];
            string poolId = "GraphViewPool";
            int virtualMachineNumber = 2;
            string virtualMachineSize = "small";

            this.jobManager = new AzureBatchJobManager(batchAccountName, batchAccountKey, batchAccountUrl,
                storageAccountName, storageAccountKey, poolId, virtualMachineNumber, virtualMachineSize);

            int parallelism = 2;
            string docDBEndPoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string docDBKey = ConfigurationManager.AppSettings["DocDBKey"];
            string docDBDatabaseId = ConfigurationManager.AppSettings["DocDBDatabaseId"];
            string docDBCollectionId = ConfigurationManager.AppSettings["DocDBCollectionId"];

            List<NodePlan> nodePlans = new List<NodePlan>();
            NodePlan nodeA = new NodePlan(
                TEST_PARTITION_BY_KEY, 
                new List<string>() {"marko", "vadas"}, 
                new PartitionPlan(new HashSet<int>() {0, 1, 2, 3, 4}, new Tuple<int, int, PartitionBetweenType>(0, 5, PartitionBetweenType.IncludeLeft)));
            NodePlan nodeB = new NodePlan(
                TEST_PARTITION_BY_KEY,
                new List<string>() { "lop", "peter", "josh", "ripple" },
                new PartitionPlan(new HashSet<int>() { 5, 6, 7, 8, 9 }, new Tuple<int, int, PartitionBetweenType>(5, 10, PartitionBetweenType.IncludeLeft)));
            nodePlans.Add(nodeA);
            nodePlans.Add(nodeB);

            this.job = new GraphViewAzureBatchJob(parallelism, nodePlans, null, docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId,
                AbstractAzureBatchGremlinTest.TEST_USE_REVERSE_EDGE, AbstractAzureBatchGremlinTest.TEST_PARTITION_BY_KEY,
                AbstractAzureBatchGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI);

            //If test graph doesn't exist in the cosmosDb, create test graph.
            //GraphViewCommand graphCommand = this.job.Command;
            //graphCommand.g().AddV("person").Property("id", "dummy").Property("name", "marko").Property("age", 29).Next();
            //graphCommand.g().AddV("person").Property("id", "特殊符号").Property("name", "vadas").Property("age", 27).Next();
            //graphCommand.g().AddV("software").Property("id", "这是一个中文ID").Property("name", "lop").Property("lang", "java").Next();
            //graphCommand.g().AddV("person").Property("id", "引号").Property("name", "josh").Property("age", 32).Next();
            //graphCommand.g().AddV("software").Property("id", "中文English").Property("name", "ripple").Property("lang", "java").Next();
            //graphCommand.g().AddV("person").Property("name", "peter").Property("age", 35).Next();  // Auto generate document id
            //graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
            //graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
            //graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            //graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
            //graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            //graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();
        }

        [TestCleanup]
        public void Cleanup()
        {
            //string docDBEndPoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            //string docDBKey = ConfigurationManager.AppSettings["DocDBKey"];
            //string docDBDatabaseId = ConfigurationManager.AppSettings["DocDBDatabaseId"];
            //string docDBCollectionId = ConfigurationManager.AppSettings["DocDBCollectionId"];
            //GraphViewConnection.ResetGraphAPICollection(docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId,
            //    AbstractAzureBatchGremlinTest.TEST_USE_REVERSE_EDGE, AbstractAzureBatchGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI,
            //    AbstractAzureBatchGremlinTest.TEST_PARTITION_BY_KEY);
        }

        public static void CheckUnOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            CheckUnOrderedResults(expected, actual, EqualityComparer<T>.Default);
        }

        public static void CheckPathResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count());
            List<T> expectedList = new List<T>();
            foreach (var item in expected)
            {
                expectedList.Add(item);
            }
            List<T> actualList = new List<T>();
            foreach (var item in actual)
            {
                actualList.Add(item);
            }
            for (var i = 0; i < expectedList.Count(); i++)
            {
                Assert.AreEqual(expectedList[i], actualList[i]);
            }
        }

        public static void CheckOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            Assert.AreEqual(expected.Count(), actual.Count());
            List<T> expectedList = new List<T>();
            foreach (var item in expected)
            {
                expectedList.Add(item);
            }
            List<T> actualList = new List<T>();
            foreach (var item in actual)
            {
                actualList.Add(item);
            }
            for (var i = 0; i < expectedList.Count(); i++)
            {
                Assert.AreEqual(expectedList[i], actualList[i]);
            }
        }

        public static void CheckUnOrderedResults<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> comparer)
        {
            Assert.AreEqual(expected.Count(), actual.Count());

            Dictionary<T, int> expectedHashMap = GetHashMap(expected, comparer);
            Dictionary<T, int> actualHashMap = GetHashMap(actual, comparer);

            foreach (T key in expectedHashMap.Keys)
            {
                Assert.AreEqual(expectedHashMap[key], actualHashMap[key]);
            }
        }

        private static Dictionary<T, int> GetHashMap<T>(IEnumerable<T> list, IEqualityComparer<T> comparer)
        {
            Dictionary<T, int> hashMap = new Dictionary<T, int>(comparer);
            foreach (T listVal in list)
            {
                if (!hashMap.ContainsKey(listVal))
                {
                    hashMap[listVal] = 0;
                }
                hashMap[listVal]++;
            }
            return hashMap;
        }

        public string ConvertToVertexId(GraphViewCommand command, string name)
        {
            // To save time
            switch (name)
            {
                case "marko":
                    return "dummy";
                case "vadas":
                    return "特殊符号";
                case "lop":
                    return "这是一个中文ID";
                case "josh":
                    return "引号";
                case "ripple":
                    return "中文English";
            }

            GraphTraversal originalTraversal = job.Traversal;
            OutputFormat originalFormat = command.OutputFormat;
            command.OutputFormat = OutputFormat.Regular;

            job.Traversal = command.g().V().Has("name", name).Id();
            string id = this.jobManager.TestQuery(job).FirstOrDefault();

            command.OutputFormat = originalFormat;
            job.Traversal = originalTraversal;
            return id;
        }

        public string ConvertToEdgeId(GraphViewCommand command, string outVertexName, string edgeLabel, string inVertexName)
        {
            Debug.Assert(job.Traversal == null);
            OutputFormat originalFormat = command.OutputFormat;
            command.OutputFormat = OutputFormat.Regular;

            job.Traversal = command.g().V().Has("name", outVertexName).OutE(edgeLabel).As("e").InV().Has("name", inVertexName).Select("e").Values("id");
            string id = this.jobManager.TestQuery(job).FirstOrDefault();

            job.Traversal = null;
            command.OutputFormat = originalFormat;

            return id;
        }

        public string ConvertToPropertyId(GraphViewCommand command, string vertexName, string property, string propertyValue)
        {
            Debug.Assert(job.Traversal == null);
            OutputFormat originalFormat = command.OutputFormat;
            command.OutputFormat = OutputFormat.Regular;

            job.Traversal = command.g().V().Has("name", vertexName).Properties(property).HasValue(propertyValue).Id();
            string propertyId = this.jobManager.TestQuery(job).FirstOrDefault();

            job.Traversal = null;
            command.OutputFormat = originalFormat;

            return propertyId;
        }

        public string getVertexString(GraphViewCommand command, string vertexName)
        {
            Debug.Assert(job.Traversal == null);
            command.OutputFormat = OutputFormat.GraphSON;
            job.Traversal = command.g().V().Has("name", vertexName);
            string result = this.jobManager.TestQuery(job).FirstOrDefault();
            job.Traversal = null;
            return JsonConvert.DeserializeObject<dynamic>(result).First.ToString();
        }

        public static List<string> ConvertToList(dynamic result)
        {
            return ((JArray)result).Select(p => p.ToString()).ToList();
        }

        public static string GetLongestResult(List<string> results)
        {
            string longest = results.FirstOrDefault();
            foreach (string res in results)
            {
                if (res.Length > longest.Length)
                {
                    longest = res;
                }
            }
            return longest;
        }
    }
}