using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin
{
    [TestClass]
    public class AddVertexTest : AbstractGremlinTest
    {
        [TestMethod]
        [TestModernCompatible]
        public void Test_g_V_AddV()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                GraphViewCommand.g().AddV("V1").Property(TEST_PARTITION_BY_KEY, "Value1").Next();
                GraphViewCommand.g().AddV("V2").Property(TEST_PARTITION_BY_KEY, "Value2").Next();
                GraphViewCommand.g().AddV("V3").Property(TEST_PARTITION_BY_KEY, "Value3").Next();
                var traversal = GraphViewCommand.g().V().AddV().Property(TEST_PARTITION_BY_KEY, "PV");
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void Test_g_V_AddV_With_Subtraversal()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {

                var traversal = command.g().V().Has("name", "marko").Property("boolproperty", false);
                traversal.Next();

                traversal = command.g().Inject(100).AddV("person")
                    .Property("name", GraphTraversal.__().V().Has("name", "marko").Values("name"))
                    .Property("p1", "v1")
                    .Property("num", GraphTraversal.__().V().Count())
                    .Property("p2", "v2", "meta1", GraphTraversal.__().V().Count(), "meta2", "meta2value")
                    .Property("p3", GraphTraversal.__(), "meta3", GraphTraversal.__().V().OutE("knows").Count())
                    .Property("p4", GraphTraversal.__().V().Has("name", "marko").Values("boolproperty"))
                    .Properties().Union(
                        GraphTraversal.__(),
                        GraphTraversal.__().HasKey("p2").Properties(),
                        GraphTraversal.__().HasKey("p3").Properties());
                var result = traversal.Next();

                Assert.IsTrue(result.Count == 9);
                Assert.IsTrue(result.Contains("vp[name->marko]"));
                Assert.IsTrue(result.Contains("vp[p1->v1]"));
                Assert.IsTrue(result.Contains("vp[num->6]"));
                Assert.IsTrue(result.Contains("vp[p2->v2]"));
                Assert.IsTrue(result.Contains("vp[p3->100]"));
                Assert.IsTrue(result.Contains("vp[p4->False]"));
                Assert.IsTrue(result.Contains("p[meta1->6]"));
                Assert.IsTrue(result.Contains("p[meta2->meta2value]"));
                Assert.IsTrue(result.Contains("p[meta3->2]"));
                foreach (var row in result)
                {
                    Console.WriteLine(row);
                }
            }
        }
    }
}
