using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    /// <summary>
    /// Tests for the Project Step.
    /// </summary>
    [TestClass]
    public class ProjectTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/ProjectTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('person').project('a','b').by(outE().count).by('age')"
        /// </summary>
        /// <remarks>
        /// Test fails because of the following bugs:
        /// 1. By(string key) not implemented for Project Op.
        /// \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\map\GremlinProjectOp.cs, Line 48.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37476
        /// </remarks>
        [TestMethod]
        [TestModernCompatible]
        public void HasLabelPersonProjectABByOutECountByAge()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().HasLabel("person")
                                                    .Project("a", "b")
                                                    .By(GraphTraversal2.__().OutE().Count())
                                                    .By("age");

                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.FirstOrDefault());

                Assert.AreEqual(3, (int)result[0]["a"]);
                Assert.AreEqual(29, (int)result[0]["b"]);
                Assert.AreEqual(0, (int)result[1]["a"]);
                Assert.AreEqual(27, (int)result[1]["b"]);
                Assert.AreEqual(2, (int)result[2]["a"]);
                Assert.AreEqual(32, (int)result[2]["b"]);
                Assert.AreEqual(1, (int)result[3]["a"]);
                Assert.AreEqual(35, (int)result[3]["b"]);
            }
        }

        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/ProjectTest.java.
        /// Equivalent gremlin: "g.V.out('created').project('a', 'b').by('name').by(__.in('created').count).order.by(select('b'),decr).select('a')"
        /// </summary>
        [TestMethod]
        [TestModernCompatible]
        public void VerticesOutCreatedProjectABByNameByInCreatedCountOrderBySelectB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Out("created")
                                                     .Project("a", "b")
                                                     .By("name")
                                                     .By(GraphTraversal2.__().In("created").Count())
                                                     .Order().By(
                                                               GraphTraversal2.__().Select("b"),
                                                               GremlinKeyword.Order.Decr)
                                                     .Select("a");

                var result = traversal.Next();
                
                CheckUnOrderedResults(new [] {"lop", "lop", "lop", "ripple"}, result);
            }
        }
    }
}
