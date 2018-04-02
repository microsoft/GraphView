using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class CoalesceTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void CoalesceWithNonexistentTraversals()
        {
            this.job.Query = "g.V().coalesce(__.out('foo'), __.out('bar'))";
            List<string> results = this.jobManager.TestQuery(this.job);

            Assert.IsFalse(results.Any());
        }

        [TestMethod]
        public void CoalesceWithTwoTraversals()
        {
            this.job.Query = "g.V().has('name', 'marko').coalesce(__.out('knows'), __.out('created')).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new string[] { "josh", "vadas" }, results);
        }

        [TestMethod]
        public void CoalesceWithTraversalsInDifferentOrder()
        {
            this.job.Query = "g.V().has('name', 'marko').coalesce(__.out('created'), __.out('knows')).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new string[] { "lop" }, results);
        }

        [TestMethod]
        public void CoalesceWithGroupCount()
        {
            using (GraphViewCommand graphViewCommand = this.job.Command)
            {
                graphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphViewCommand.g().V()
                    .Coalesce(
                        GraphTraversal.__().Out("likes"),
                        GraphTraversal.__().Out("knows"),
                        GraphTraversal.__().Out("created"))
                    .GroupCount()
                    .By("name");

                var result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());
                Assert.AreEqual(1, (int)result[0]["josh"]);
                Assert.AreEqual(2, (int)result[0]["lop"]);
                Assert.AreEqual(1, (int)result[0]["ripple"]);
                Assert.AreEqual(1, (int)result[0]["vadas"]);
            }
        }

        [TestMethod]
        public void CoalesceWithPath()
        {
            // gremlin: g.V().coalesce(__.outE('knows'), __.outE('created')).otherV().path().by('name').by(label)
            this.job.Query = "g.V().coalesce(__.outE('knows'), __.outE('created')).otherV().path().by('name').by('label')";
            List<string> results = this.jobManager.TestQuery(this.job);

            Assert.IsTrue(results.Count == 5);
            List<string> correctResults = new List<string>
            {
                "[marko, knows, vadas]",
                "[marko, knows, josh]",
                "[josh, created, ripple]",
                "[josh, created, lop]",
                "[peter, created, lop]"
            };
            CheckUnOrderedResults(correctResults, results);
        }
    }
}
