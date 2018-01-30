using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Branch
{
    [TestClass]
    public class OptionalTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void OptionalTest1()
        {
            string query = "g.V().hasLabel('person').optional(__.out('knows')).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResult = new List<string> {
                "vadas",
                "josh",
                "vadas",
                "josh",
                "peter"
            };
            CheckUnOrderedResults(correctResult, results);
        }

        [TestMethod]
        public void OptionalTest2()
        {
            string query = "g.V().hasLabel('person').optional(__.out('knows').optional(__.out('created'))).path().by('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            List<string> correctResult = new List<string> {
                "[marko, vadas]",
                "[marko, josh, ripple]",
                "[marko, josh, lop]",
                "[vadas]",
                "[josh]",
                "[peter]"
            };
            CheckUnOrderedResults(correctResult, results);
        }
    }
}
