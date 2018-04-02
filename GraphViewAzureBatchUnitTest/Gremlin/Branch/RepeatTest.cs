using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
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
            this.job.Query = "g.V().repeat(__.out()).times(2).emit().path().by('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
            this.job.Query = "g.V().repeat(__.out()).times(2).repeat(__.in()).times(2).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string> { "marko", "marko" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatOutTimes2()
        {
            this.job.Query = "g.V().repeat(__.out()).times(2).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string> { "ripple", "lop" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatOutTimes2Emit()
        {
            this.job.Query = "g.V().repeat(__.out()).times(2).emit().values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
            this.job.Query = "g.V().has('name', 'marko').times(2).repeat(__.out()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string> { "ripple", "lop" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesEmitTimes2RepeatOutPath()
        {
            this.job.Query = "g.V().emit().times(2).repeat(__.out()).path().by('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
            this.job.Query = "g.V().emit().repeat(__.out()).times(2).path().by('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
            this.job.Query = "g.V().has('name', 'marko').emit(__.hasLabel('person')).repeat(__.out()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string>
            {
                "marko", "josh", "vadas"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void VerticesRepeatGroupCountMByNameOutTimes2CapM()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Repeat(GraphTraversal.__().GroupCount("m").By("name").Out()).Times(2).Cap("m");
                dynamic results = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job)[0]);
                Assert.AreEqual(1, results.Count);
                var result = results[0];
                Assert.AreEqual(2, (int)result["ripple"]);
                Assert.AreEqual(1, (int)result["peter"]);
                Assert.AreEqual(2, (int)result["vadas"]);
                Assert.AreEqual(2, (int)result["josh"]);
                Assert.AreEqual(4, (int)result["lop"]);
                Assert.AreEqual(1, (int)result["marko"]);
            }
        }

        [TestMethod]
        public void VerticesRepeatBothTimes10AsAOutAsBSelectAB()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Repeat(GraphTraversal.__().Both()).Times(3).As("a").Out().As("b").Select("a", "b");

                int counter = 0;
                dynamic results = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job)[0]);
                foreach (var result in results)
                {
                    Assert.IsTrue(result["a"] != null);
                    Assert.IsTrue(result["b"] != null);
                    counter++;
                }
                Assert.IsTrue(counter == 92);
            }
        }

        [TestMethod]
        public void HasVertexIdRepeatOutUntilOutECountIs0ValuesName()
        {
            this.job.Query = "g.V().has('name', 'marko').repeat(__.out()).until(__.outE().count().is(0)).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string> { "lop", "lop", "ripple", "vadas" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void RepeatTimes2Repeat()
        {
            this.job.Query = "g.V().repeat(__.out()).times(2).repeat(__.in()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new string[] { }, results);
        }

        [TestMethod]
        public void RepeatTimesLessThanZero()
        {
            this.job.Query = "g.V().repeat(__.out()).times(-1).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new string[] { "lop", "lop", "lop", "vadas", "josh", "ripple" }, results);
        }
    }
}