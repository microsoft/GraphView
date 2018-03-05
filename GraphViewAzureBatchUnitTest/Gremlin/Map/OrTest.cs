using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class OrTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_V_orXhasXage_gt_27X__outE_count_gte_2X_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V.or(has('age',gt(27)), outE().count.is(gte(2l))).name"
        /// </summary>
        [TestMethod]
        public void VerticesOrHasAgeGT27OutECountIsGTE2ValuesName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Or(
                                                       GraphTraversal.__().Has("age", Predicate.gt(27)),
                                                       GraphTraversal.__().OutE()
                                                                           .Count()
                                                                           .Is(Predicate.gte(2L)))
                                                   .Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "marko", "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V.or(outE('knows'), has(T.label, 'software') | has('age',gte(35))).name"
        /// </summary>
        [TestMethod]
        public void VerticesOrOutEKnowsHasLabelSoftwareOrHasAgeGTE35ValuesName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V()
                   .Or(
                       GraphTraversal.__()
                           .OutE("knows"),
                       GraphTraversal.__()
                           .Where(
                               GraphTraversal.__()
                                   .Or(
                                       GraphTraversal.__()
                                           .Has("label", "software"),
                                       GraphTraversal.__()
                                       .Has("age", Predicate.gte(35)))))
                   .Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "marko", "ripple", "lop", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_orXselectXaX_selectXaXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V().as('a').or(__.select('a'), __.select('a'))"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOrSelectASelectA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().As("a").Or(
                                                               GraphTraversal.__().Select("a"),
                                                               GraphTraversal.__().Select("a"));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                Assert.AreEqual(6, result.Count);
            }
        }
    }
}
