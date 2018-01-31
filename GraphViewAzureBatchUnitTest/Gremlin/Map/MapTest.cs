using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    // tinkerpop/gremlin-test/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/map/MapTest.java
    [TestClass]
    public class MapTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void g_VX1X_mapXnameX()
        {
            string query = "g.V().has('name', 'marko').map(__.values('name'))";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            Assert.IsTrue(results.Count == 1);
            Assert.IsTrue(results[0] == "marko");
        }

        [TestMethod]
        public void g_V_mapXselectXaXX()
        {
            string query = "g.V().as('a').map(__.select('a'))";
            // todo 
            //List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            //Console.WriteLine("-------------Test Result-------------");
            //foreach (string result in results)
            //{
            //    Console.WriteLine(result);
            //}
            //Assert.IsTrue(results.Count == 6);
        }
    }
}
