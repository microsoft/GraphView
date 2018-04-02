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
            this.job.Query = "g.V().has('name', 'marko').map(__.values('name'))";
            List<string> results = this.jobManager.TestQuery(this.job);

            Assert.IsTrue(results.Count == 1);
            Assert.IsTrue(results[0] == "marko");
        }

        [TestMethod]
        public void g_V_mapXselectXaXX()
        {
            this.job.Query = "g.V().as('a').map(__.select('a'))";
            List<string> results = this.jobManager.TestQuery(this.job);

            Assert.IsTrue(results.Count == 6);
        }
    }
}
