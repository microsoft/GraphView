using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class FlatMapTest : AbstractGremlinTest
    {
        /// <summary>
        /// g_V_asXaX_flatMapXselectXaXX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/FlatMapTest.java
        /// Gremlin: g.V().as("a").flatMap(select("a"));
        /// </summary>
        [TestMethod]
        public void FlatMapWithSelect()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V()
                    .As("a")
                    .FlatMap(GraphTraversal2.__().Select("a"));
                var result = traversal.Next();
                Assert.AreEqual(6, result.Count);
            }
        }
    }
}
