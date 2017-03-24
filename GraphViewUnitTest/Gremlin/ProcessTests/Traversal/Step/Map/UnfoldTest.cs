using System.Collections.Generic;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    /// <summary>
    /// Tests for the Unfold Step.
    /// </summary>
    [TestClass]
    public class UnfoldTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_localXoutE_foldX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/UnfoldTest.java.
        /// Equivalent gremlin: "g.V.local(__.outE.fold).unfold"
        /// </summary>
        [TestMethod]
        public void LocalOutEFoldUnfold()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().Local(GraphTraversal2.__().OutE().Fold()).Unfold();

                var result = traversal.Next();
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
        /// Port of the g_V_valueMap_unfold_mapXkeyX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/UnfoldTest.java.
        /// Equivalent gremlin: "g.V.valueMap.unfold.map { it.key }"
        /// </summary>
        /// <remarks>
        /// Test fails becuase of the following:
        /// 1. ValueMap is not implemented. WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37472
        /// 2. Test is disabled since the actual gremlin uses Map with a iterator function overload, which is not implemented. WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37473
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void ValueMapUnfoldMapKey()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                ////var traversal = graphCommand.g().V().ValueMap().Unfold().Map(m->m.get().getKey());

                ////var result = traversal.Next();

                // Skipping Asserts until we fix the above listed bugs.

                ////dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/UnfoldTest.java.
        /// Equivalent gremlin: "g.V(v1Id).repeat(__.both.simplePath).until(hasId(v6Id)).path.by('name').unfold", "v1Id", v1Id, "v6Id", v6Id
        /// </summary>
        [TestMethod]
        public void HasVIdRepeatBothSimplePathUntilHasIdVPathByNameUnfold()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(graphCommand, "peter");

                var traversal = graphCommand.g().V().HasId(vertexId1).Repeat(GraphTraversal2.__().Both().SimplePath()).Until(GraphTraversal2.__().HasId(vertexId2)).Path().By("name").Unfold();


                var result = traversal.Next();

                CheckUnOrderedResults(new [] {"marko", "lop", "peter", "marko", "josh", "lop", "peter"}, result);
            }
        }
    }
}
