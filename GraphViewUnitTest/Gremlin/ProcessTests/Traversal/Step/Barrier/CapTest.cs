using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Barrier
{
    [TestClass]
    public class CapTest : AbstractGremlinTest
    {
        [TestMethod]
        public void GroupCountCap1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().GroupCount("a").By(GremlinKeyword.T.Label).Cap("a").Unfold();
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void GroupCountCap2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().GroupCount("a").By(GremlinKeyword.T.Label).GroupCount("b").By(GraphTraversal.__().OutE().Count()).Cap("a", "b");
                var result = traversal.Next();
                foreach (var res in result)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(1, result.Count);
                // [a:[person:4, software:2], b:[3:1, 0:3, 2:1, 1:1]]
                Assert.IsTrue(result[0].Contains("a:[person:4, software:2]"));
                Assert.IsTrue(result[0].Contains("3:1") && result[0].Contains("0:3") && result[0].Contains("2:1") && result[0].Contains("1:1"));
            }
        }

        [TestMethod]
        public void AggregateCap()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Out("knows").Aggregate("x").By("name").Cap("x");
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("[vadas, josh]", result[0]);
            }
        }

        [TestMethod]
        public void AggregateCapValues()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Out("knows").Aggregate("x").Cap("x").Unfold().Values("age");
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        public void RepeatCap()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V()
                    .Repeat(GraphTraversal.__().Both().GroupCount("m").By(GremlinKeyword.T.Label)).Times(10).Cap("m");
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("[person:39196, software:19598]", result[0]);
            }
        }
    }
}
