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
    public class DropTest : AbstractGremlinTest
    {
        [TestMethod]
        public void DropEdge()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                //GraphTraversal2 traversal = command.g().E().Drop();


                //GraphTraversal2 traversal = command.g().V().Has("name", "marko").Property("name", "twet");

                //command.OutputFormat = OutputFormat.GraphSON;
                List<string> result = command.g().V().Has("name", "marko").Property("name", "twet").Values("name").Next();
                
                foreach (string s in result)
                {
                    Console.WriteLine(s);
                }

                //Assert.AreEqual(0, GetEdgeCount(command));
                //Assert.AreEqual(6, GetVertexCount(command));
            }
        }

        [TestMethod]
        public void DropVertex()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                GraphTraversal2 traversal = command.g().V().Drop();
                List<string> result = traversal.Next();
                Assert.AreEqual(0, GetEdgeCount(command));
                Assert.AreEqual(0, GetVertexCount(command));
            }
        }

        [TestMethod]
        public void DropEdgeProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                List<string> result = command.g().E().Properties().Next();
                Assert.AreEqual(12, result.Count);
                result = command.g().E().Properties().Drop().Next();
                Assert.AreEqual(0, result.Count);
            }
        }

        [TestMethod]
        public void DropMetaProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                List<string> result = command.g().AddV().Property("name", "jinjin", "meta1", "metavalue")
                                                        .Property("name", "jinjin", "meta1", "metavalue").Next();
               
                result = command.g().V().Has("name", "jinjin").Properties("name").Properties().Next();
                Assert.AreEqual(2, result.Count);

                command.g().V().Has("name", "jinjin").Properties("name").Properties().Drop().Next();
                result = command.g().V().Has("name", "jinjin").Properties("name").Properties().Next();

                //var temp = command.g().V().Has("name", "jinjin").Next();
                //Console.WriteLine(temp);

                Assert.AreEqual(0, result.Count);
            }
        }

        [TestMethod]
        public void DropVertexProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                GraphTraversal2 traversal = command.g().V().Coin(1.0).Values("name");
                List<string> result = traversal.Next();
                CheckUnOrderedResults(new[] { "marko", "vadas", "lop", "josh", "ripple", "peter" }, result);
            }
        }
    }
}
