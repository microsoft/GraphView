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
            this.job.Query = "g.V().hasLabel('person').optional(__.out('knows')).values('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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
            this.job.Query = "g.V().hasLabel('person').optional(__.out('knows').optional(__.out('created'))).path().by('name')";
            List<string> results = this.jobManager.TestQuery(this.job);

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