using System.Collections.Generic;
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
        protected GraphViewAzureBatchJob job;

        [TestInitialize]
        public void Setup()
        {
            this.job = new GraphViewAzureBatchJob();
        }

        [TestCleanup]
        public void Cleanup() { }

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
            string id = StartAzureBatch.AzureBatchJobManager.TestQuery(job).FirstOrDefault();

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
            string id = StartAzureBatch.AzureBatchJobManager.TestQuery(job).FirstOrDefault();

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
            string propertyId = StartAzureBatch.AzureBatchJobManager.TestQuery(job).FirstOrDefault();

            job.Traversal = null;
            command.OutputFormat = originalFormat;

            return propertyId;
        }

        public string getVertexString(GraphViewCommand command, string vertexName)
        {
            Debug.Assert(job.Traversal == null);
            command.OutputFormat = OutputFormat.GraphSON;
            job.Traversal = command.g().V().Has("name", vertexName);
            string result = StartAzureBatch.AzureBatchJobManager.TestQuery(job).FirstOrDefault();
            job.Traversal = null;
            return JsonConvert.DeserializeObject<dynamic>(result).First.ToString();
        }

        public static List<string> ConvertToList(dynamic result)
        {
            return ((JArray)result).Select(p => p.ToString()).ToList();
        }
    }
}