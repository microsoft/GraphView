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
        [Ignore]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabelGroupCount()
        {
            string query =
                "g.V().choose(__.label().is('person'), union(__.out().values('lang'), __.out().values('name')), __.in().label()).groupCount()";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in result)
            {
                Console.WriteLine(res);
            }

            dynamic results = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
            Assert.AreEqual(4, (int)results[0]["java"]);
            Assert.AreEqual(1, (int)results[0]["ripple"]);
            Assert.AreEqual(4, (int)results[0]["person"]);
            Assert.AreEqual(1, (int)results[0]["vadas"]);
            Assert.AreEqual(1, (int)results[0]["josh"]);
            Assert.AreEqual(3, (int)results[0]["lop"]);
        }

        [TestMethod]
        [Ignore]
        public void UnionRepeatUnionOutCreatedInCreatedTimes2RepeatUnionInCreatedOutCreatedTimes2LabelGroupCount()
        {
            string query =
                "g.V().union(__.repeat(__.union(__.out('created'), __.in('created'))).times(2), __.repeat(__.union(__.in('created'), __.out('created'))).times(2)).label().groupCount()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }

            dynamic result = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());

            Assert.AreEqual(12, (int)result[0]["software"]);
            Assert.AreEqual(20, (int)result[0]["person"]);
        }


        [TestMethod]
        public void HasVId1VId2LocalUnionOutECountInECountOutEWeightSum()
        {
            string query =
                "g.V().has('name', within('marko', 'vadas')).local(__.union(__.outE().count(), __.inE().count(), __.outE().values('weight').sum()))";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new double[] { 0d, 0d, 0d, 3d, 1d, 1.9d }, results.Select(value => double.Parse(value)).ToList());
        }

        [TestMethod]
        public void HasVId1VId2UnionOutECountInECountOutEWeightSum()
        {
            string query =
                "g.V().has('name', within('marko', 'vadas')).union(__.outE().count(), __.inE().count(), __.outE().values('weight').sum())";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new double[] { 3d, 1.9d, 1d }, results.Select(value => double.Parse(value)).ToList());
        }

        [TestMethod]
        public void UnionWithoutBranch()
        {
            string query = "g.V().union().V().count()";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new string[] { "0" }, results);
        }
    }
}
