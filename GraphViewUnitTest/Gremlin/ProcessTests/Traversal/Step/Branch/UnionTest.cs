using System.Collections.Generic;
using System.Configuration;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Branch
{
    /// <summary>
    /// Tests for the Union Step.
    /// </summary>
    [TestClass]
    public class UnionTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_unionXout__inX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.union(__.out, __.in).name"
        /// </summary>
        [TestMethod]
        public void UnionOutAndInVertices()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Union(
                    GraphTraversal2.__().Out(),
                    GraphTraversal2.__().In()).Values("name");

                var result = traversal.Next();
                var expectedResult = new List<string>() { "marko", "marko", "marko", "lop", "lop", "lop", "peter", "ripple", "josh", "josh", "josh", "vadas" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_unionXrepeatXoutX_timesX2X__outX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id).union(repeat(__.out).times(2), __.out).name", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// It appears that the Repeat Times 2 steps aren't working, as this the result is only returing marko's immediate Out vertices.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36529
        /// </remarks>
        [TestMethod]
        public void HasVertexIdUnionOutAndOutRepeatTimes2()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                var traversal = GraphViewCommand.g().V().HasId(vertexId)
                    .Union(
                        GraphTraversal2.__().Repeat(GraphTraversal2.__().Out()).Times(2),
                        GraphTraversal2.__().Out())
                    .Values("name");

                var result = traversal.Next();
                var expectedResult = new List<string>() { "lop", "lop", "ripple", "josh", "vadas" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label)"
        /// </summary>
        /// <remarks>
        /// This fails because Choose is not implemented.
        /// \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\branch\GremlinChooseOp.cs Line 45, in GetContext().
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36531
        /// </remarks>
        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabel()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                Assert.Fail();
                var traversal = GraphViewCommand.g().V().Choose(
                    GraphTraversal2.__().Label().Is("person"),
                    GraphTraversal2.__().Union(
                            GraphTraversal2.__().Out().Values("lang"),
                            GraphTraversal2.__().Out().Values("name")),
                    GraphTraversal2.__().In().Label());

                var result = traversal.Next();
                var expectedResult = new List<string>() { "lop", "lop", "lop", "ripple", "java", "java", "java", "java", "josh", "vadas", "person", "person", "person", "person" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label).groupCount"
        /// </summary>
        /// <remarks>
        /// This fails because Choose is not implemented.
        /// \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\branch\GremlinChooseOp.cs Line 45, in GetContext().
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36531
        /// GroupCount() is not implemented on GraphTraversal2. Replacing the functionality by using Group().By(__().Count())
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36609
        /// </remarks>
        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabelGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                Assert.Fail();
                var traversal = GraphViewCommand.g().V().Choose(
                    GraphTraversal2.__().Label().Is("person"),
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__().Out().Values("lang"),
                        GraphTraversal2.__().Out().Values("name")),
                    GraphTraversal2.__().In().Label())
                    .Group().By(GraphTraversal2.__().Count());

                var result = traversal.Next();
                // ASIDE: Below Assertions are incorrect, revisit this once we actually get the above traversal to work.
                var expectedResult = new List<string>() { "lop", "lop", "lop", "ripple", "java", "java", "java", "java", "josh", "vadas", "person", "person", "person", "person" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.union(repeat(union(out('created'), __.in('created'))).times(2), repeat(union(__.in('created'), out('created'))).times(2)).label.groupCount()"
        /// </summary>
        /// <remarks>
        /// 1. GroupCount() is not implemented on GraphTraversal2. Replacing the functionality by using Group().By(__().Count())
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36609
        /// 2. This Test seems to throw an exception during compilation, more details on the error in the below work item:
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36710
        /// </remarks>
        
        [TestMethod]
        [Ignore]
        public void UnionRepeatUnionOutCreatedInCreatedTimes2RepeatUnionInCreatedOutCreatedTimes2LabelGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Union(
                    GraphTraversal2.__().Repeat(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().Out("created"),
                            GraphTraversal2.__().In("created"))).Times(2),
                    GraphTraversal2.__().Repeat(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().In("created"),
                            GraphTraversal2.__().Out("created"))).Times(2))
                    .Label().Group().By(GraphTraversal2.__().Count());

                var result = traversal.Next();

                // Assertions missing, revisit this once we actually get the above traversal to work.
                //assertEquals(12l, groupCount.get("software").longValue());
                //assertEquals(20l, groupCount.get("person").longValue());
                //assertEquals(2, groupCount.size());
            }
        }

        /// <summary>
        /// Port of the g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id, v2Id).local(union(outE().count, inE().count, outE().weight.sum))", "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        /// <remarks>
        /// The traversal fails because GremlinSumOp.GetContext() throws a NotImplementedException.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36732
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void HasVId1VId2LocalUnionOutECountInECountOutEWeightSum()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "vadas");

                var traversal = GraphViewCommand.g().V(vertexId1, vertexId2)
                    .Local(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().OutE().Count(),
                            GraphTraversal2.__().InE().Count(),
                            GraphTraversal2.__().OutE().Values("weight").Sum()));

                var result = traversal.Next();

                // Assertions missing, revisit this once we actually get the above traversal to work.
                //checkResults(Arrays.asList(0l, 0l, 0, 3l, 1l, 1.9d), traversal);
            }
        }

        /// <summary>
        /// Port of the g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id, v2Id).union(outE().count, inE().count, outE().weight.sum)", "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        /// <remarks>
        /// The traversal fails because GremlinSumOp.GetContext() throws a NotImplementedException.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36732
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void HasVId1VId2UnionOutECountInECountOutEWeightSum()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "vadas");

                var traversal = GraphViewCommand.g().V(vertexId1, vertexId2)
                    .Union(
                        GraphTraversal2.__().OutE().Count(),
                        GraphTraversal2.__().InE().Count(),
                        GraphTraversal2.__().OutE().Values("weight").Sum());

                var result = traversal.Next();

                // Assertions missing, revisit this once we actually get the above traversal to work.
                // checkResults(Arrays.asList(3l, 1.9d, 1l), traversal);
            }
        }
    }
}

