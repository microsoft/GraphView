using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class FlatMapTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void FlatMapWithSelect()
        {
            string query = "g.V().as('a').flatMap(__.select('a'))";
            // todo
            //List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            //Console.WriteLine("-------------Test Result-------------");
            //foreach (string result in results)
            //{
            //    Console.WriteLine(result);
            //}
            //Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void FlatMapOut()
        {
            string query = "g.V().flatMap(__.out()).values('name')";
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
                "vadas",
                "josh",
                "ripple"
            };
            CheckUnOrderedResults(correctResults, results);
        }
    }
}
