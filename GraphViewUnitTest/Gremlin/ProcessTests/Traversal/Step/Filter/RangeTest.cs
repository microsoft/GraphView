//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    /// <summary>
    /// Tests for Range Step.
    /// Main problem for this step is Scope.local is not supported, so most of the tests can't run
    /// Range for edge has problems also
    /// </summary>
    [TestClass]
    public sealed class RangeTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_VX1X_out_limitX2X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out().limit(2)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void VIdOutLimit2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().HasId(vertexId).Out().Has("name").Limit(2);

                var result = traversal.Next();
                Assert.AreEqual(2, result.Count);
            }
        }

        /// <summary>
        /// Port of g_V_localXoutE_limitX1X_inVX_limitX3X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().local(outE().limit(1)).inV().limit(3)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void LocalOutELimit1InVLimit3()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Local(GraphTraversal2.__().OutE().Limit(1)).InV().Limit(3);

                var result = traversal.Next();
                Assert.AreEqual(3, result.Count);
            }
        }

        /// <summary>
        /// Port of g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out("knows").outE("created").range(0, 1).inV()"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void VIdOutKnowsOutECreatedRange0_1InV()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().HasId(vertexId).Out("knows").OutE("created").Range(0, 1).InV();

                var results = traversal.Values("name").Next();
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual(true, string.Equals(results.FirstOrDefault(), "lop") || string.Equals(results.FirstOrDefault(), "ripple"));
            }
        }

        /// <summary>
        /// Port of g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out("knows").out("created").range(0, 1)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void VIdOutKnowsOutCreatedRange0_1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().HasId(vertexId).Out("knows").Out("created").Range(0, 1);

                var results = traversal.Values("name").Next();
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual(true, string.Equals(results.FirstOrDefault(), "lop") || string.Equals(results.FirstOrDefault(), "ripple"));
            }
        }

        /// <summary>
        /// Port of g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out("created").in("created").range(1, 3)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void VIdOutCreatedInCreatedRange1_3()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().HasId(vertexId).Out("created").In("created").Range(1, 3);

                var results = traversal.Values("name").Next();
                Assert.AreEqual(2, results.Count);
                Assert.AreEqual(true, string.Equals(results.FirstOrDefault(), "marko") || string.Equals(results.FirstOrDefault(), "josh") || string.Equals(results.FirstOrDefault(), "peter"));
            }
        }

        /// <summary>
        /// Port of g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out("created").inE("created").range(1, 3).outV()"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void VIdOutCreatedInECreatedRange1_3OutV()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(graphCommand, "marko");
                var traversal = graphCommand.g().V().HasId(vertexId).Out("created").InE("created").Range(1, 3).OutV();

                var results = traversal.Values("name").Next();
                Assert.AreEqual(2, results.Count);
                Assert.AreEqual(true, string.Equals(results.FirstOrDefault(), "marko") || string.Equals(results.FirstOrDefault(), "josh") || string.Equals(results.FirstOrDefault(), "peter"));
            }
        }

        /// <summary>
        /// Port of g_V_repeatXbothX_timesX3X_rangeX5_11X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().repeat(both()).times(3).range(5, 11)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void RepeatBothTimes3Range5_11()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().Repeat(GraphTraversal2.__().Both()).Times(3).Range(5, 11);

                var results = traversal.Next();
                Assert.AreEqual(6, results.Count);
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_2X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").in().as("a").in().as("a").<List<String>>select("a").by(unfold().values("name").fold()).limit(local, 2)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAInAsAInASASelectAByUnfoldValuesNameFoldLimitLocal_2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").In().As("a").In().As("a").Select("a").By(GraphTraversal2.__().Unfold().Values("name").Fold()).Limit(GremlinKeyword.Scope.Local, 2);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new [] {"lop,josh", "ripple,josh"}, ((JArray)results).Select(p=> $"{p[0]},{p[1]}").ToList());
            }
        }

        /// <summary>
        /// Port of get_g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_1X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").in().as("a").in().as("a").<List<String>>select("a").by(unfold().values("name").fold()).limit(local, 1)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAInAsAInASASelectAByUnfoldValuesNameFoldLimitLocal_1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().As("a").In().As("a").In().As("a").Select("a").By(GraphTraversal2.__().Unfold().Values("name").Fold()).Limit(GremlinKeyword.Scope.Local, 1);
                var results = traversal.Next();

                CheckUnOrderedResults(new [] {"lop", "ripple"}, results);
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").out().as("a").out().as("a").<List<String>>select("a").by(unfold().values("name").fold()).range(local, 1, 3)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAOutAsAOutASASelectAByUnfoldValuesNameFoldLimitRangeLocal_1_3()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").Out().As("a").Out().As("a").Select("a").By(GraphTraversal2.__().Unfold().Values("name").Fold()).Range(GremlinKeyword.Scope.Local, 1, 3);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new [] {"josh,ripple", "josh,lop"}, ((JArray)results).Select(p=> $"{p[0]},{p[1]}").ToList());
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").out().as("a").out().as("a").<List<String>>select("a").by(unfold().values("name").fold()).range(local, 1, 2)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAOutAsAOutASASelectAByUnfoldValuesNameFoldLimitRangeLocal_1_2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().As("a").Out().As("a").Out().As("a").Select("a").By(GraphTraversal2.__().Unfold().Values("name").Fold()).Range(GremlinKeyword.Scope.Local, 1, 2);
                CheckUnOrderedResults(new [] {"josh", "josh"}, traversal.Next());
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").out().as("a").out().as("a").<List<String>>select("a").by(unfold().values("name").fold()).range(local, 4, 5)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAOutAsAOutASASelectAByUnfoldValuesNameFoldLimitRangeLocal_4_5()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V().As("a").Out().As("a").Out().As("a").Select("a").By(GraphTraversal2.__().Unfold().Values("name").Fold()).Range(GremlinKeyword.Scope.Local, 4, 5);
                Assert.IsTrue(traversal.Next().Count == 0);
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").in().as("b").in().as("c").<Map<String, String>>select("a","b","c").by("name").limit(local, 2)"
        /// </summary>
        [TestMethod]
        public void AsAInAsBInASCSelectA_B_CByNameLimitLocal_2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").In().As("b").In().As("c").Select("a", "b", "c").By("name").Limit(GremlinKeyword.Scope.Local, 2);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new[] { "lop,josh", "ripple,josh" }, ((JArray)results).Select(p => $"{p["a"]},{p["b"]}").ToList());
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").in().as("b").in().as("c").<Map<String, String>>select("a","b","c").by("name").limit(local, 1)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAInAsBInASCSelectA_B_CByNameLimitLocal_1()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").In().As("b").In().As("c").Select("a", "b", "c").By("name").Limit(GremlinKeyword.Scope.Local, 1);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new [] {"lop", "ripple"}, ((JArray)results).Select(p => (string)p["a"]).ToList());
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").out().as("b").out().as("c").<Map<String, String>>select("a","b","c").by("name").range(local, 1, 3)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAOutAsBOutASCSelectA_B_CByNameRangeLocal_1_3()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").Out().As("b").Out().As("c").Select("a", "b", "c").By("name").Range(GremlinKeyword.Scope.Local, 1, 3);
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new[] { "josh,ripple", "josh,lop" }, ((JArray)results).Select(p => $"{p["b"]},{p["c"]}").ToList());
            }
        }

        /// <summary>
        /// Port of g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/RangeTest.java.
        /// Equivalent gremlin: "g.V().as("a").out().as("b").out().as("c").<Map<String, String>>select("a","b","c").by("name").range(local, 1, 2)"
        /// </summary>
        [TestMethod]
        [Owner("zhlian")]
        public void AsAOutAsBOutASCSelectA_B_CByNameRangeLocal_1_2()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = graphCommand.g().V().As("a").Out().As("b").Out().As("c").Select("a", "b", "c").By("name")/*.Range(GremlinKeyword.Scope.Local, 1, 2)*/;
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                CheckUnOrderedResults(new [] {"josh", "josh"}, ((JArray)results).Select(p=>p["b"].ToString()).ToList());
            }
        }

    }
}
