using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            string query = "g.V().union(__.out(), __.in()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
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
            string query = "g.V().has('name', 'marko').union(__.repeat(__.out()).times(2), __.out()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResults = new List<string> {"lop", "lop", "ripple", "josh", "vadas"};
            CheckUnOrderedResults(correctResults, results);
        }

        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabel()
        {
            string query =
                "g.V().choose(__.label().is('person'), __.union(__.out().values('lang'), __.out().values('name')), __.in().label())";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
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
            string query =
                "g.V().choose(__.label().is('person'), union(__.out().values('lang'), __.out().values('name')), __.in().label()).groupCount()";
            // todo
        }

        [TestMethod]
        public void UnionRepeatUnionOutCreatedInCreatedTimes2RepeatUnionInCreatedOutCreatedTimes2LabelGroupCount()
        {
            string query =
                "g.V().union(__.repeat(__.union(__.out('created'), __.in('created'))).times(2), __.repeat(__.union(__.in('created'), __.out('created'))).times(2)).label().groupCount()";
            // todo
        }


        [TestMethod]
        public void HasVId1VId2LocalUnionOutECountInECountOutEWeightSum()
        {
            string query =
                "g.V().has('name', within('marko', 'vadas')).local(__.union(__.outE().count(), __.inE().count(), __.outE().values('weight').sum()))";
            // todo
        }

        [TestMethod]
        public void HasVId1VId2UnionOutECountInECountOutEWeightSum()
        {
            string query =
                "g.V().has('name', within('marko', 'vadas')).union(__.outE().count, __.inE().count, __.outE().values('weight').sum())";
            // todo
        }

        [TestMethod]
        public void UnionWithoutBranch()
        {
            string query = "g.V().union().V().count()";
            // todo
        }
    }
}
