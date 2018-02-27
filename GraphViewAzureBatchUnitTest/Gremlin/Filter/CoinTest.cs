using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    [TestClass]
    public class CoinTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void CoinWithProbabilityEq1()
        {
            string query = "g.V().coin(1.0).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            CheckUnOrderedResults(new[] {"marko", "vadas", "lop", "josh", "ripple", "peter"}, results);
        }

        [TestMethod]
        public void CoinWithProbabilityEq0()
        {
            string query = "g.V().coin(0).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            Assert.AreEqual(results.Count, 0);
        }

        [TestMethod]
        public void CoinWithProbabilityEqHalf()
        {
            string query = "g.V().coin(0.5).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string res in results)
            {
                Console.WriteLine(res);
            }
            HashSet<string> allNames = new HashSet<string> {"marko", "vadas", "lop", "josh", "ripple", "peter"};
            Assert.IsTrue(results.Count >= 0 && results.Count <= 6);
            Assert.IsTrue(results.TrueForAll(name => allNames.Contains(name)));
        }
    }
}
