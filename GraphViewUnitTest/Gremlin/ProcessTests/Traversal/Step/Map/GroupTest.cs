using System;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class GroupTest : AbstractGremlinTest
    {
        [TestMethod]
        public void g_V_Group()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                var traversal = GraphViewCommand.g().V().Group();
                var results = traversal.Next();

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        [TestMethod]
        public void g_V_Group_by()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                var traversal = GraphViewCommand.g().V().Group().By(GraphTraversal2.__().Values("name")).By();
                var results = traversal.Next();

                Console.WriteLine(traversal.SqlScript);
                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }
    }
}