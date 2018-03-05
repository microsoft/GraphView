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
    public class UnfoldTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_V_localXoutE_foldX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/UnfoldTest.java.
        /// Equivalent gremlin: "g.V.local(__.outE.fold).unfold"
        /// </summary>
        [TestMethod]
        public void LocalOutEFoldUnfold()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().Local(GraphTraversal.__().OutE().Fold()).Unfold();

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                HashSet<string> edgeIds = new HashSet<string>();

                foreach (var res in dynamicResult)
                {
                    edgeIds.Add(string.Format("{0}_{1}", res["inV"].ToString(), res["id"].ToString()));
                }

                Assert.AreEqual(6, dynamicResult.Count);
                Assert.AreEqual(6, edgeIds.Count);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/UnfoldTest.java.
        /// Equivalent gremlin: "g.V(v1Id).repeat(__.both.simplePath).until(hasId(v6Id)).path.by('name').unfold", "v1Id", v1Id, "v6Id", v6Id
        /// </summary>
        [TestMethod]
        public void HasVIdRepeatBothSimplePathUntilHasIdVPathByNameUnfold()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(graphCommand, "peter");

                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).Repeat(GraphTraversal.__().Both().SimplePath()).Until(GraphTraversal.__().HasId(vertexId2)).Path().By("name").Unfold();


                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "marko", "lop", "peter", "marko", "josh", "lop", "peter" }, result);
            }
        }
    }
}
