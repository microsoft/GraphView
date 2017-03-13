using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        [Ignore]
        public void HasLabelPersonProjectABByOutECountByAge()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().HasLabel("person")
                                                    .Project("a", "b")
                                                    .By(GraphTraversal2.__().OutE().Count())
                                                    .By("age");

                var result = traversal.Next();

                // Skipping Asserts until we fix the above listed bugs.

                ////dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/ProjectTest.java.
        /// Equivalent gremlin: "g.V.out('created').project('a', 'b').by('name').by(__.in('created').count).order.by(select('b'),decr).select('a')"
        /// </summary>
        /// <remarks>
        /// Test fails because of the following bugs:
        /// 1. By(string key) not implemented for Project Op.
        /// \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\map\GremlinProjectOp.cs, Line 48.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37476
        /// 2. GraphTraversal2 by(Traversal&lt;?, ?&gt; traversal, Comparator comparator) not implemented.
        /// \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\map\GremlinProjectOp.cs, Line 348.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37477
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesOutCreatedProjectABByNameByInCreatedCountOrderBySelectB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                ////var traversal = graphCommand.g().V().Out("created")
                ////                                     .Project("a", "b")
                ////                                     .By("name")
                ////                                     .By(GraphTraversal2.__().In("created").Count())
                ////                                     .Order().By(
                ////                                               GraphTraversal2.__().Select("b"),
                ////                                               GremlinKeyword.Order.Decr)
                ////                                     .Select("a");

                ////var result = traversal.Next();

                // Skipping Asserts until we fix the above listed bugs.

                ////dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
            }
        }
    }
}
