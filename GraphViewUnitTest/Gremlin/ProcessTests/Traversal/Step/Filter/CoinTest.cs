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
    public class CoinTest : AbstractGremlinTest
    {
        [TestMethod]
        public void CoinWithProbabilityEq1()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                GraphTraversal2 traversal = command.g().V().Coin(1.0).Values("name");
                List<string> result = traversal.Next();
                CheckUnOrderedResults(new[] { "marko", "vadas", "lop", "josh", "ripple", "peter" }, result);
            }
        }

        [TestMethod]
        public void CoinWithProbabilityEq0()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                GraphTraversal2 traversal = command.g().V().Coin(0.0).Values("name");
                List<string> result = traversal.Next();

                Assert.AreEqual(result.Count, 0);
            }
        }

        [TestMethod]
        public void CoinWithProbabilityEqHalf()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection)) {
                GraphTraversal2 traversal = command.g().V().Coin(0.5).Values("name");
                List<string> result = traversal.Next();

                HashSet<string> allNames = new HashSet<string> {"marko", "vadas", "lop", "josh", "ripple", "peter"};
                Assert.IsTrue(result.Count >= 0 && result.Count <= 6);
                Assert.IsTrue(result.TrueForAll(name => allNames.Contains(name)));
            }
        }
    }
}
