using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewAzureBatchUnitTest.Gremlin.Branch
{
    [TestClass]
    public class RepeatTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void VerticesRepeatOutTimes2EmitPath()
        {
            string query = "g.V().repeat(__.out()).times(2).emit().path().by('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string>
            {
                "[marko, lop]",
                "[marko, vadas]",
                "[marko, josh]",
                "[marko, josh, ripple]",
                "[marko, josh, lop]",
                "[josh, ripple]",
                "[josh, lop]",
                "[peter, lop]"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatOutTimes2RepeatInTimes2ValuesName()
        {
            string query = "g.V().repeat(__.out()).times(2).repeat(__.in()).times(2).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string> { "marko", "marko" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatOutTimes2()
        {
            string query = "g.V().repeat(__.out()).times(2).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string> { "ripple", "lop" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatOutTimes2Emit()
        {
            string query = "g.V().repeat(__.out()).times(2).emit().values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }

            Dictionary<string, long> map = new Dictionary<string, long>();
            foreach (string name in results)
            {
                long count;
                map.TryGetValue(name, out count);
                map[name] = count + 1;
            }

            Assert.AreEqual(4, map.Count);
            Assert.IsTrue(map.ContainsKey("vadas"));
            Assert.IsTrue(map.ContainsKey("josh"));
            Assert.IsTrue(map.ContainsKey("ripple"));
            Assert.IsTrue(map.ContainsKey("lop"));
            Assert.AreEqual(1, map["vadas"]);
            Assert.AreEqual(1, map["josh"]);
            Assert.AreEqual(2, map["ripple"]);
            Assert.AreEqual(4, map["lop"]);
        }

        [TestMethod]
        public void HasVertexIdTimes2RepeatOutValuesName()
        {
            string query = "g.V().has('name', 'marko').times(2).repeat(__.out()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string> { "ripple", "lop" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesEmitTimes2RepeatOutPath()
        {
            string query = "g.V().emit().times(2).repeat(__.out()).path().by('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string>
            {
                "[marko]",
                "[marko, lop]",
                "[marko, vadas]",
                "[marko, josh]",
                "[marko, josh, ripple]",
                "[marko, josh, lop]",
                "[vadas]",
                "[lop]",
                "[josh]",
                "[josh, ripple]",
                "[josh, lop]",
                "[ripple]",
                "[peter]",
                "[peter, lop]"
            };
            CheckUnOrderedResults(correctResults, results);
        }


        [TestMethod]
        public void VerticesEmitRepeatOutTimes2Path()
        {
            string query = "g.V().emit().repeat(__.out()).times(2).path().by('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string>
            {
                "[marko]",
                "[marko, lop]",
                "[marko, vadas]",
                "[marko, josh]",
                "[marko, josh, ripple]",
                "[marko, josh, lop]",
                "[vadas]",
                "[lop]",
                "[josh]",
                "[josh, ripple]",
                "[josh, lop]",
                "[ripple]",
                "[peter]",
                "[peter, lop]"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void HasVertexIdEmitHasLabelPersonRepeatOutValuesName()
        {
            string query = "g.V().has('name', 'marko').emit(__.hasLabel('person')).repeat(__.out()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string>
            {
                "marko", "josh", "vadas"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        [Ignore]
        public void VerticesRepeatGroupCountMByNameOutTimes2CapM()
        {
            string query = "g.V().repeat(__.groupCount('m').by('name').out()).times(2).cap('m')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in result)
            {
                Console.WriteLine(res);
            }

            Assert.AreEqual(1, result.Count);
            dynamic results = JsonConvert.DeserializeObject<dynamic>(result[0]);
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(2, (int)results[0]["ripple"]);
            Assert.AreEqual(1, (int)results[0]["peter"]);
            Assert.AreEqual(2, (int)results[0]["vadas"]);
            Assert.AreEqual(2, (int)results[0]["josh"]);
            Assert.AreEqual(4, (int)results[0]["lop"]);
            Assert.AreEqual(1, (int)results[0]["marko"]);
        }

        [TestMethod]
        [Ignore]
        public void VerticesRepeatBothTimes10AsAOutAsBSelectAB()
        {
            string query = "g.V().repeat(__.both()).times(10).as('a').out().as('b').select('a', 'b')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in result)
            {
                Console.WriteLine(res);
            }

            int counter = 0;
            dynamic results = JsonConvert.DeserializeObject<dynamic>(result[0]);
            foreach (var res in results)
            {
                Assert.IsTrue(res["a"] != null);
                Assert.IsTrue(res["b"] != null);
                counter++;
            }
            Assert.IsTrue(counter == 43958);
        }

        [TestMethod]
        public void HasVertexIdRepeatOutUntilOutECountIs0ValuesName()
        {
            string query = "g.V().has('name', 'marko').repeat(__.out()).until(__.outE().count().is(0)).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string> {"lop", "lop", "ripple", "vadas"};
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void RepeatTimes2Repeat()
        {
            string query = "g.V().repeat(__.out()).times(2).repeat(__.in()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }

            CheckUnOrderedResults(new string[] { }, results);
        }

        [TestMethod]
        public void RepeatTimesLessThanZero()
        {
            string query = "g.V().repeat(__.out()).times(-1).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }

            CheckUnOrderedResults(new string[] { "lop", "lop", "lop", "vadas", "josh", "ripple" }, results);
        }
    }
}
