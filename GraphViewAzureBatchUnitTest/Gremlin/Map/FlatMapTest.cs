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
            this.job.Query = "g.V().as('a').flatMap(__.select('a'))";
            List<string> results = this.jobManager.TestQuery(this.job);

            Assert.AreEqual(6, results.Count);
        }

        [TestMethod]
        public void FlatMapOut()
        {
            this.job.Query = "g.V().flatMap(__.out()).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
