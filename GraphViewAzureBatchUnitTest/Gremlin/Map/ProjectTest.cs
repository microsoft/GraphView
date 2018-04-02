using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class ProjectTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/ProjectTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('person').project('a','b').by(outE().count).by('age')"
        /// </summary>
        [TestMethod]
        public void HasLabelPersonProjectABByOutECountByAge()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().HasLabel("person")
                                                   .Project("a", "b")
                                                   .By(GraphTraversal.__().OutE().Count())
                                                   .By("age");

                dynamic results = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());

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
        public void VerticesOutCreatedProjectABByNameByInCreatedCountOrderBySelectB()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Out("created")
                                                    .Project("a", "b")
                                                    .By("name")
                                                    .By(GraphTraversal.__().In("created").Count())
                                                    .Order().By(
                                                              GraphTraversal.__().Select("b"),
                                                              GremlinKeyword.Order.Decr)
                                                    .Select("a");

                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "lop", "lop", "lop", "ripple" }, result);
            }
        }

        [TestMethod]
        public void ProjectWithoutByClause()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V().Project("a").Select("a").Values("name");

                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "marko", "vadas", "lop", "josh", "ripple", "peter" }, result);
            }
        }
    }
}
