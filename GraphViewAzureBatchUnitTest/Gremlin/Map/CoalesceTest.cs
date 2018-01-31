using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
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
            string query = "g.V().coalesce(__.out('foo'), __.out('bar'))";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            Assert.IsFalse(results.Any());
        }

        [TestMethod]
        public void CoalesceWithTwoTraversals()
        {
            string query = "g.V().has('name', 'marko').coalesce(__.out('knows'), __.out('created')).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            CheckUnOrderedResults(new string[] { "josh", "vadas" }, results);
        }

        [TestMethod]
        public void CoalesceWithTraversalsInDifferentOrder()
        {
            string query = "g.V().has('name', 'marko').coalesce(__.out('created'), __.out('knows')).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            CheckUnOrderedResults(new string[] { "lop" }, results);
        }

        [TestMethod]
        public void CoalesceWithGroupCount()
        {
            string query = "g.V().coalesce(__.out('likes'), __.out('knows'), __.out('created')).groupCount().by('name')";
            // todo
            //List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            //Console.WriteLine("-------------Test Result-------------");
            //foreach (string result in results)
            //{
            //    Console.WriteLine(result);
            //}
            //var convertResults = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());
            //Assert.AreEqual(1, (int)convertResults[0]["josh"]);
            //Assert.AreEqual(2, (int)convertResults[0]["lop"]);
            //Assert.AreEqual(1, (int)convertResults[0]["ripple"]);
            //Assert.AreEqual(1, (int)convertResults[0]["vadas"]);
        }

        [TestMethod]
        public void CoalesceWithPath()
        {
            // gremlin: g.V().coalesce(__.outE('knows'), __.outE('created')).otherV().path().by('name').by(label)
            string query = "g.V().coalesce(__.outE('knows'), __.outE('created')).otherV().path().by('name').by('label')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in results)
            {
                Console.WriteLine(row);
            }
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
