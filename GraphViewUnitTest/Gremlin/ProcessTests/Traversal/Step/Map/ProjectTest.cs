using System.Collections.Generic;
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
        [TestModernCompatible(false)]
        public void HasLabelPersonProjectABByOutECountByAge()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().HasLabel("person")
                                                    .Project("a", "b")
                                                    .By(GraphTraversal.__().OutE().Count())
                                                    .By("age");

                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.FirstOrDefault());

                List<string> ans = new List<string>();
                foreach (dynamic result in results)
                {
                    List<string> temp = new List<string>();
                    temp.Add(result["a"].ToString());
                    temp.Add(result["b"].ToString());
                    ans.Add(string.Join(",", temp));
                    temp.Clear();
                }

                List<string> expect = new List<string> {
                    "3,29",
                    "0,27",
                    "2,32",
                    "1,35"
                };

                CheckUnOrderedResults(expect, ans);
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
                                                     .By(GraphTraversal.__().In("created").Count())
                                                     .Order().By(
                                                               GraphTraversal.__().Select("b"),
                                                               GremlinKeyword.Order.Decr)
                                                     .Select("a");

                var result = traversal.Next();
                
                CheckUnOrderedResults(new [] {"lop", "lop", "lop", "ripple"}, result);
            }
        }

        [TestMethod]
        [TestModernCompatibleAttribute]
        public void ProjectWithoutByClause()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Project("a").Select("a").Values("name");

                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "marko", "vadas", "lop", "josh" , "ripple" , "peter" }, result);
            }
        }
    }
}
