using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Barrier
{
    [TestClass]
    public class CapTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void GroupCountCap1()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().GroupCount("a").By(GremlinKeyword.T.Label).Cap("a").Unfold();
                List<string> result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(2, result.Count);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <remarks> 
        /// Now the results might like following:
        /// [a:[person:4, software:2], b:[0:3, 3:1, 2:1, 1:1]]
        /// [a:[], b:[]]
        /// The results need be merged. The result.count should be 1.
        /// </remarks>
        [TestMethod]
        public void GroupCountCap2()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().GroupCount("a").By(GremlinKeyword.T.Label).GroupCount("b").By(GraphTraversal.__().OutE().Count()).Cap("a", "b");
                List<string> result = this.jobManager.TestQuery(this.job);

                //Assert.AreEqual(1, result.Count);
                // [a:[person:4, software:2], b:[3:1, 0:3, 2:1, 1:1]]
                Assert.IsTrue(result.Count >= 1);
                string res = GetLongestResult(result);// = result[0]
                Assert.IsTrue(res.Contains("a:[person:4, software:2]"));
                Assert.IsTrue(res.Contains("3:1") && res.Contains("0:3") && res.Contains("2:1") && res.Contains("1:1"));
            }
        }

        [TestMethod]
        public void AggregateCap()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Out("knows").Aggregate("x").By("name").Cap("x");
                List<string> result = this.jobManager.TestQuery(this.job);

                Assert.IsTrue(result.Count == 2 || result.Count == 1);
                if (result.Count == 2)
                {
                    CheckUnOrderedResults(new string[] {"[vadas]", "[josh]"}, result);
                }
                else
                {
                    Assert.IsTrue("[vadas, josh]" == result[0] || "[josh, vadas]" == result[0]);
                }
            }
        }

        [TestMethod]
        public void AggregateCapValues()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Out("knows").Aggregate("x").Cap("x").Unfold().Values("age");
                List<string> result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void RepeatCap()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V()
                    .Repeat(GraphTraversal.__().Both().GroupCount("m").By(GremlinKeyword.T.Label)).Times(3).Cap("m");
                List<string> result = this.jobManager.TestQuery(this.job);

                //Assert.AreEqual(1, result.Count);
                Assert.IsTrue(result.Count >= 1);
                string res = GetLongestResult(result); // = result[0];
                Assert.AreEqual("[person:76, software:38]", res);
            }
        }
    }
}