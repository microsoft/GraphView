using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using GraphViewUnitTest.Gremlin;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    /// <summary>
    /// Tests for Where Step.
    /// </summary>
    [TestClass]
    public class WhereTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', eq('b'))"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAEqB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b").Where("a", Predicate.eq("b"));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', neq('b'))"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereANeqB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b").Where("a", Predicate.neq("b"));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('b').has('name', 'marko'))"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAsBHasNameMarko()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b")
                                                    .Where(GraphTraversal2.__().As("b").Has("name", "marko"));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('a').out('knows').as('b'))"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAsAOutKnowsB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b")
                                                    .Where(GraphTraversal2.__().As("a").Out("knows").As("b"));

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').where(__.as('a').name.is('josh')).in('created').name"
        /// </summary>
        /// <remarks>
        /// This test's traversal yields no results.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38570
        /// </remarks>
        [TestMethod]
        public void VerticesAsAOutCreatedWhereAsANameIsJoshInCreatedName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().As("a")
                                                    .Out("created")
                                                    .Where(GraphTraversal2.__().As("a").Values("name").Is("josh"))
                                                    .In("created").Values("name");

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                // Skipping this validation until we can fix the bugs.

                Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.withSideEffect('a', ['josh','peter']).V(v1Id).out('created').in('created').name.where(within('a'))", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// WithSideEffect Step not supported, hence test cannot run.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38573
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void WithSideEffectAJoshPeterHasVIdOutCreatedInCreatedNameWhereWithinA()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                ////var traversal = graphCommand.g().WithSideEffect("a", new List<string>(){"josh", "peter"}).V().HasId(markoVertexId).Out("created").In("created").Values("name").Where(Predicate.within("a"));

                ////var result = traversal.Next();
                ////dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                ////Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').as('b').where('a', neq('b')).name", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// This test fails because Where(StartKey, Predicate) is currently not implemented.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38575
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void HasVextexIdAsAOutCreatedInCreatedAsBWhereANeqBValuesName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                var traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created").As("b")
                                                    .Where("a", Predicate.neq("b")).Values("name");

                var result = traversal.Next();
                CheckOrderedResults(new List<string> { "josh", "peter" }, result);

                // Skipping this validation until we can fix the bugs.
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').as('b').where(__.as('b').out('created').has('name','ripple')).values('age','name')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVextexIdAsAOutCreatedInCreatedAsBWhereAsBOutCreatedHasNameRippleValuesAgeName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                // Skipping this validation until we can fix the bugs.

                var traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created").As("b")
                                                    .Where(GraphTraversal2.__().As("b").Out("created").Has("name", "ripple"))
                                                    .Values("age", "name");

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "josh", "32" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').where(eq('a')).name", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutCreatedInCreatedWhereEqAVaulesName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                var traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created")
                                                    .Where(Predicate.eq("a")).Values("name");

                var result = traversal.Next();
                CheckOrderedResults(new List<string> { "marko"}, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').where(neq('a')).name", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutCreatedInCreatedWhereNeqAVaulesName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                var traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created")
                                                    .Where(Predicate.neq("a")).Values("name");

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "peter", "josh" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_out_aggregateXxX_out_whereXwithout_xX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out.aggregate('x').out.where(P.not(within('x')))", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// This test seems to hit an interesting scenario. Because the predicate within the where is nested, we're getting confused about the correct
        /// Where method within GremlinVariable.cs to use.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38576
        /// </remarks>
        [TestMethod]
        public void HasVertexIdOutAggregateXOutWhereNotWithinX()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                var traversal = graphCommand.g().V().HasId(markoVertexId).Out()
                                                    .Aggregate("x").Out()
                                                    .Where(Predicate.not(Predicate.within("x")));

                var result = traversal.Values("name").Next();
                CheckOrderedResults(new List<string> { "ripple" }, result);

                // Skipping this validation until we can fix the bugs.
            }
        }

        /// <summary>
        /// Port of the g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.withSideEffect('a'){g.V(v2Id).next()}.V(v1Id).out.where(neq('a'))", "graph", graph, "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        /// <remarks>
        /// WithSideEffect Step not supported, hence test cannot run.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38573
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void WithSideEffectAHasVertexId2HasVertexId1OutWhereNeqA()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");
                string vadasVertexId = this.ConvertToVertexId(graphCommand, "vadas");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                ////var traversal = graphCommand.g().WithSideEffect("a", g.V(vadasVertexId).Next()).V(markoVertexId).Out().Where(Predicate.neq("a"));

                ////var result = traversal.Next();
                ////dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                ////Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).repeat(__.bothE('created').where(without('e')).aggregate('e').otherV).emit.path", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// This test seems to fail because the Predicate cannot determine the second expression for the where step.
        /// Where method within GremlinVariable.cs.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38578
        /// </remarks>
        [TestMethod]
        public void HasVertexIdRepeatBothECreatedWhereWithoutEAggregateEOtherVEmitPath()
        {
            //==>[v[1], e[9][1 - created->3], v[3]]
            //==>[v[1], e[9][1 - created->3], v[3], e[11][4 - created->3], v[4]]
            //==>[v[1], e[9][1 - created->3], v[3], e[12][6 - created->3], v[6]]
            //==>[v[1], e[9][1 - created->3], v[3], e[11][4 - created->3], v[4], e[10][4 - created->5], v[5]]
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                var traversal = graphCommand.g().V().HasId(markoVertexId)
                                                    .Repeat(GraphTraversal2.__().BothE("created")
                                                                                .Where(Predicate.without("e"))
                                                                                .Aggregate("e")
                                                                                .OtherV())
                                                    .Emit().Path();

                // Skipping this validation until we can fix the bugs.

                var result = traversal.Next();
                //dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                //Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_V_whereXnotXoutXcreatedXXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.where(__.not(out('created'))).name"
        /// </summary>
        [TestMethod]
        public void VerticesWhereNotOutCreatedValuesN()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Where(GraphTraversal2.__().Not(
                                                                GraphTraversal2.__().Out("created")))
                                                    .Values("name");

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "vadas", "lop", "ripple" }, result);

                // Skipping this validation until we can fix the bugs.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out.as('b').where(and(__.as('a').out('knows').as('b'),or(__.as('b').out('created').has('name','ripple'),__.as('b').in('knows').count.is(P.not(eq(0)))))).select('a','b')"
        /// </summary>
        /// /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutABWhereAndAsAOutKnowsAsBOrAsBOutCreatedHasNameRippleAsBInKnowsCountIsNotEq0SelectAB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                //string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                var traversal = graphCommand.g().V()
                    .As("a")
                    .Out()
                    .As("b")
                    .Where(
                        GraphTraversal2.__()
                            .And(
                                GraphTraversal2.__()
                                    .As("a")
                                    .Out("knows")
                                    .As("b"),
                                GraphTraversal2.__()
                                    .Or(
                                        GraphTraversal2.__()
                                            .As("b")
                                            .Out("created")
                                            .Has("name", "ripple"),
                                        GraphTraversal2.__().As("b")
                                            .In("knows")
                                            .Count()
                                            .Is(Predicate.not(Predicate.eq(0))))))
                    .Select("a", "b");

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_selectXaX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "gremlin-groovy", "g.V.where(out('created').and.out('knows').or.in('knows')).name"
        /// </summary>
        /// <remarks>
        /// This test fails because a traversal with a combination of AND and OR within a Where step seems to fail.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38580
        /// </remarks>
        [TestMethod]
        public void VerticesWhereOutCreatedAndOutKnowsORInKnowsValueName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Where(GraphTraversal2.__().Out("created")
                                                                               .And()
                                                                               .Out("knows")
                                                                               .Or()
                                                                               .In("knows"))
                                                    .Values("name");

                var result = traversal.Next();
                CheckUnOrderedResults(new List<string> { "marko", "vadas", "josh" }, result);

                // Skipping this validation until we can fix the bugs.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').as('b').where(and(__.as('b').in,__.not(__.as('a').out('created').has('name','ripple')))).select('a','b')"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutCreatedAsBWhereAndAsBInNotAsAOutCreatedHasNameRippleSelectAB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().As("a")
                                                    .Out("created").As("b")
                                                    .Where(GraphTraversal2.__().And(
                                                                                    GraphTraversal2.__().As("b")
                                                                                                        .In(),
                                                                                    GraphTraversal2.__().Not(
                                                                                                        GraphTraversal2.__().As("a")
                                                                                                                            .Out("created")
                                                                                                                            .Has("name", "ripple"))))
                                                    .Select("a", "b");

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').as('b').in('created').as('c').both('knows').both('knows').as('d').where('c',P.not(eq('a').or(eq('d')))).select('a','b','c','d')"
        /// </summary>
        /// <remarks>
        /// In this test there is ORing of predicates, but since we don't support that yet, we're replacing the traversal
        /// where(c not(eq a or eq d)) with, where(where(c not eq a) and where(c not eq d)). (NOTE: The And is within a Where since it's infix).
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38582
        /// This fails also because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutCreatedAsBInCreatedAsCBothKnowsBothKnowsAsDWhereCNotEqAOrEqDSelectABCD()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().As("a")
                                                    .Out("created").As("b")
                                                    .In("created").As("c")
                                                    .Both("knows").Both("knows").As("d")
                                                    .Where(
                                                        GraphTraversal2.__().Where("c", Predicate.not(Predicate.eq("a")))
                                                                            .And()
                                                                            .Where("c", Predicate.not(Predicate.eq("d"))))
                                                    .Select("a", "b", "c", "d");

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(4, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out.as('b').where(__.as('b').in.count.is(eq(3)).or.where(__.as('b').out('created').and.as('b').has(label,'person'))).select('a','b')"
        /// </summary>
        /// <remarks>
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutAsBWhereInCountIsEq3OrWhereOutCreatedAndHasLabelPersonSelectAB()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                var traversal = graphCommand.g().V().As("a")
                                                    .Out().As("b")
                                                    .Where(
                                                        GraphTraversal2.__().As("b")
                                                                            .In()
                                                                            .Count().Is(Predicate.eq(3))
                                                                                         .Or()
                                                                                         .Where(
                                                                                            GraphTraversal2.__().As("b")
                                                                                                                .Out("created")
                                                                                                                .And().As("b")
                                                                                                                .HasLabel("person")))
                                                    .Select("a", "b");

                var result = traversal.Next();
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);

                // Skipping this validation until we can fix the multi-key select.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').in('created').as('b').where('a', gt('b')).by('age').select('a', 'b').by('name')"
        /// </summary>
        /// <remarks>
        /// Commenting out the traversal as Where with a Predicate and Modulating By is currently not supported.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38585
        /// This fails because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutCreatedInCreatedAsBWhereAGtBByAgeSelectABByName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                ////var traversal = graphCommand.g().V().As("a")
                ////                                    .Out("created").In("created").As("b")
                ////                                    .Where("a", Predicate.gt("b"))
                ////                                    .By("age")
                ////                                    .Select("a", "b").By("name");

                //var result = traversal.Next();
                //dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                //Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').outE('created').as('b').inV().as('c').where('a', gt('b').or(eq('b'))).by('age').by('weight').by('weight').select('a', 'c').by('name')"
        /// </summary>
        /// <remarks>
        /// Commenting out the traversal as Where with a Predicate and Modulating By is currently not supported.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38585
        /// Aditionally, in this test there is ORing of predicates.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38582
        /// This fails also because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutECreatedAsBInVAsCWhereAGtBOrEqBByAgeByWeightByWeightSelectACByName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                //string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                //var traversal = graphCommand.g().V().as("a").outE("created").as("b").inV().as("c").where("a", gt("b").or(eq("b"))).by("age").by("weight").by("weight").<String>select("a", "c").by("name");

                //var result = traversal.Next();
                //dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                //Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().as('a').outE('created').as('b').inV().as('c').in('created').as('d').where('a', lt('b').or(gt('c')).and(neq('d'))).by('age').by('weight').by(__.in('created').values('age').min()).select('a', 'c', 'd').by('name')"
        /// </summary>
        /// <remarks>
        /// Commenting out the traversal as Where with a Predicate and Modulating By is currently not supported.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38585
        /// Aditionally, in this test there is ORing of predicates.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38582
        /// This fails also because Select for more than 1 key is not yet implemented.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37285
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void VerticesAsAOutECreatedAsBInVAsCInCreatedAsDWhereALtBOrGtCAndNeqDByAgeByWeightByInCreatedValuesAgeMinSelectACDByName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                //string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                //var traversal = graphCommand.g().V().as("a").outE("created").as("b").inV().as("c").in("created").as("d").where("a", lt("b").or(gt("c")).and(neq("d"))).by("age").by("weight").by(in("created").values("age").min()).<String>select("a", "c", "d").by("name");

                //var result = traversal.Next();
                //dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                //Assert.AreEqual(2, dynamicResult.Count);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out.has('age').where(gt('a')).by('age').name", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// Commenting out the traversal as Where with a Predicate and Modulating By is currently not supported.
        /// WorkItem: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/38585
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void HasVertexIdAsAOutHasAgeWhereGtAByAgeName()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;

                // Skipping this validation until we can fix the bugs.

                //var traversal = graphCommand.g().V().HasId(markoVertexId).As("a").Out().Has("age").Where(Predicate.gt("a")).By("age").Values("name");

                //var result = traversal.Next();
                //dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                //Assert.AreEqual(2, dynamicResult.Count);
            }
        }
    }
}
