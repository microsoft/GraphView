using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using GraphView;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class OrTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_orXhasXage_gt_27X__outE_count_gte_2X_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V.or(has('age',gt(27)), outE().count.is(gte(2l))).name"
        /// </summary>
        [TestMethod]
        [TestModernCompatible(false)]
        public void VerticesOrHasAgeGT27OutECountIsGTE2ValuesName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Or(
                                                        GraphTraversal.__().Has("age", Predicate.gt(27)),
                                                        GraphTraversal.__().OutE()
                                                                            .Count()
                                                                            .Is(Predicate.gte(2L)))
                                                    .Values("name");

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "marko", "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V.or(outE('knows'), has(T.label, 'software') | has('age',gte(35))).name"
        /// </summary>
        [TestMethod]
        [TestModernCompatible(false)]
        public void VerticesOrOutEKnowsHasLabelSoftwareOrHasAgeGTE35ValuesName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V()
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

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "marko", "ripple", "lop", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_orXselectXaX_selectXaXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/OrTest.java.
        /// Equivalent gremlin: "g.V().as('a').or(__.select('a'), __.select('a'))"
        /// </summary>
        [TestMethod]
        [TestModernCompatible]
        public void VerticesAsAOrSelectASelectA()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().As("a").Or(
                                                                GraphTraversal.__().Select("a"),
                                                                GraphTraversal.__().Select("a"));

                var result = traversal.Next();
                Assert.AreEqual(6, result.Count);
            }
        }
    }
}
