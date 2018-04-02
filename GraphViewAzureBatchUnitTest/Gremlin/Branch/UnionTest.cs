using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Branch
{
    /// <summary>
    /// Tests for the Union Step.
    /// </summary>
    [TestClass]
    public class UnionTest : AbstractAzureBatchGremlinTest
    {

        [TestMethod]
        public void UnionOutAndInVertices()
        {
            this.job.Query = "g.V().union(__.out(), __.in()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string>
            {
                "marko",
                "marko",
                "marko",
                "lop",
                "lop",
                "lop",
                "peter",
                "ripple",
                "josh",
                "josh",
                "josh",
                "vadas"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void HasVertexIdUnionOutAndOutRepeatTimes2()
        {
            this.job.Query = "g.V().has('name', 'marko').union(__.repeat(__.out()).times(2), __.out()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string> { "lop", "lop", "ripple", "josh", "vadas" };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabel()
        {
            this.job.Query =
                "g.V().choose(__.label().is('person'), __.union(__.out().values('lang'), __.out().values('name')), __.in().label())";
            List<string> results = this.jobManager.TestQuery(this.job);

            List<string> correctResults = new List<string>
            {
                "lop",
                "lop",
                "lop",
                "ripple",
                "java",
                "java",
                "java",
                "java",
                "josh",
                "vadas",
                "person",
                "person",
                "person",
                "person"
            };
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabelGroupCount()
        {
            using (GraphViewCommand graphViewCommand = this.job.Command)
            {
                graphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphViewCommand.g().V().Choose(
                    GraphTraversal.__().Label().Is("person"),
                    GraphTraversal.__().Union(
                        GraphTraversal.__().Out().Values("lang"),
                        GraphTraversal.__().Out().Values("name")),
                    GraphTraversal.__().In().Label())
                    .GroupCount();

                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());

                Assert.AreEqual(4, (int)result[0]["java"]);
                Assert.AreEqual(1, (int)result[0]["ripple"]);
                Assert.AreEqual(4, (int)result[0]["person"]);
                Assert.AreEqual(1, (int)result[0]["vadas"]);
                Assert.AreEqual(1, (int)result[0]["josh"]);
                Assert.AreEqual(3, (int)result[0]["lop"]);
            }
        }

        [TestMethod]
        public void UnionRepeatUnionOutCreatedInCreatedTimes2RepeatUnionInCreatedOutCreatedTimes2LabelGroupCount()
        {
            using (GraphViewCommand graphViewCommand = this.job.Command)
            {
                graphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphViewCommand.g().V().Union(
                    GraphTraversal.__().Repeat(
                        GraphTraversal.__().Union(
                            GraphTraversal.__().Out("created"),
                            GraphTraversal.__().In("created"))).Times(2),
                    GraphTraversal.__().Repeat(
                        GraphTraversal.__().Union(
                            GraphTraversal.__().In("created"),
                            GraphTraversal.__().Out("created"))).Times(2))
                    .Label().GroupCount();

                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());

                Assert.AreEqual(12, (int)result[0]["software"]);
                Assert.AreEqual(20, (int)result[0]["person"]);
            }
        }


        [TestMethod]
        public void HasVId1VId2LocalUnionOutECountInECountOutEWeightSum()
        {
            this.job.Query =
                "g.V().has('name', within('marko', 'vadas')).local(__.union(__.outE().count(), __.inE().count(), __.outE().values('weight').sum()))";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new double[] { 0d, 0d, 0d, 3d, 1d, 1.9d }, results.Select(value => double.Parse(value)).ToList());
        }

        [TestMethod]
        public void HasVId1VId2UnionOutECountInECountOutEWeightSum()
        {
            this.job.Query =
                "g.V().has('name', within('marko', 'vadas')).union(__.outE().count(), __.inE().count(), __.outE().values('weight').sum())";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new double[] { 3d, 1.9d, 1d }, results.Select(value => double.Parse(value)).ToList());
        }

        [TestMethod]
        public void UnionWithoutBranch()
        {
            this.job.Query = "g.V().union().V().count()";
            List<string> results = this.jobManager.TestQuery(this.job);

            CheckUnOrderedResults(new string[] { "0" }, results);
        }
    }
}
