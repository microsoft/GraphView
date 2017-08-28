using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    [TestClass]
    public class LimitTest : AbstractGremlinTest
    {
        [TestMethod]
        [TestModernCompatible]
        public void StoreLimit()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Store("x").Limit(1).Cap("x").Unfold();
                var result = traversal.Next();

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }

                Assert.AreEqual(2, result.Count);
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void LimitStore()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Limit(1).Store("x").Cap("x").Unfold();
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void AggregateLimit()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Aggregate("x").Limit(1).Cap("x").Unfold();
                var result = traversal.Next();

                Assert.AreEqual(6, result.Count);
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void LimitAggregate()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Limit(1).Aggregate("x").Cap("x").Unfold();
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
            }
        }
    }
}
