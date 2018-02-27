using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Barrier
{
    [TestClass]
    public class CapTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        [Ignore]
        public void GroupCountCap1()
        {
            string query = "g.V().groupCount('a').by('label').cap('a').unfold()";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in result)
            {
                Console.WriteLine(row);
            }
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        [Ignore]
        public void GroupCountCap2()
        {
            string query = "g.V().groupCount('a').by('label').groupCount('b').by(__.outE().count()).cap('a', 'b')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in result)
            {
                Console.WriteLine(row);
            }
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("[a:[person:4, software:2], b:[3:1, 0:3, 2:1, 1:1]]", result[0]);
        }

        [TestMethod]
        [Ignore]
        public void AggregateCap()
        {
            string query = "g.V().out('knows').aggregate('x').by('name').cap('x')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in result)
            {
                Console.WriteLine(row);
            }
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("[vadas, josh]", result[0]);
        }

        [TestMethod]
        [Ignore]
        public void AggregateCapValues()
        {
            string query = "g.V().out('knows').aggregate('x').cap('x').unfold().values('age')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in result)
            {
                Console.WriteLine(row);
            }
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        [Ignore]
        public void RepeatCap()
        {
            string query = "g.V().repeat(__.both().groupCount('m').by('label')).times(10).cap('m')";
            List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string row in result)
            {
                Console.WriteLine(row);
            }
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("[person:39196, software:19598]", result[0]);
        }
    }
}
