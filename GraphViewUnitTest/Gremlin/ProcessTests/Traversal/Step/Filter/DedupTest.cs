using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{
    [TestClass]
    public class DedupTest : AbstractGremlinTest
    {
        /// <summary>
        /// g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().out().in().values("name").fold().dedup(Scope.local).unfold();
        /// </summary>
        /// <remarks>
        /// dedup(Scope scope, params string[] dedupLabels) is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36911
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupLocalScope()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement GraphTraversal2 dedup(Scope scope, params string[] dedupLabels)

                //var traversal = GraphViewCommand.g().V()
                //    .Out()
                //    .In()
                //    .Values("name")
                //    .Fold()
                //    .Dedup(Scope.local)
                //    .Unfold();
                //var result = traversal.Next();

                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "marko", "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().out().as("x").in().as("y").select("x", "y").by("name").fold().dedup(Scope.local, "x", "y").unfold();
        /// </summary>
        /// <remarks>
        /// dedup(Scope scope, params string[] dedupLabels) is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36911
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupLocalMultipleLabels()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement GraphTraversal2 dedup(Scope scope, params string[] dedupLabels)

                var expected = new List<Dictionary<string, string>>()
                {
                    new Dictionary<string, string>()
                    {
                        { "x", "lop" },
                        { "y", "marko" }
                    },
                    new Dictionary<string, string>()
                    {
                        { "x", "lop" },
                        { "y", "josh" }
                    },
                    new Dictionary<string, string>()
                    {
                        { "x", "lop" },
                        { "y", "peter" }
                    },
                    new Dictionary<string, string>()
                    {
                        { "x", "vadas" },
                        { "y", "marko" }
                    },
                    new Dictionary<string, string>()
                    {
                        { "x", "josh" },
                        { "y", "marko" }
                    },
                    new Dictionary<string, string>()
                    {
                        { "x", "ripple" },
                        { "y", "josh" }
                    },
                };

                //var traversal = GraphViewCommand.g().V()
                //    .Out()
                //    .As("x")
                //    .In()
                //    .As("y")
                //    .Select("x", "y")
                //    .By("name")
                //    .Fold()
                //    .Dedup("x", "y")
                //    .Unfold();
                // var result = traversal.Next();

                //AbstractGremlinTest.CheckUnOrderedResults(expected, result, new AbstractGremlinTest.DicionaryEqualityComparer<string, string>());
            }
        }

        /// <summary>
        /// g_V_both_dedup_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().dedup().values("name");
        /// </summary>
        [TestMethod]
        public void DedupWithBoth()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .Dedup()
                    .Values("name");
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, result);
            }
        }

        /// <summary>
        /// g_V_both_hasXlabel_softwareX_dedup_byXlangX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().has(T.label, "software").dedup().by("lang").values("name");
        /// </summary>
        /// <remarks>
        /// Dedup().By() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37139
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupBy()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement Dedup().By()

                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .HasLabel("software")
                    .Dedup()
                    .By("lang")
                    .Values("name");
                var result = traversal.Next();

                Assert.IsTrue(result.Contains("lop") || result.Contains("ripple"));
            }
        }

        /// <summary>
        /// g_V_both_name_order_byXa_bX_dedup_value()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().<String>properties("name").order().by((a, b) -> a.value().compareTo(b.value())).dedup().value();
        /// </summary>
        /// <remarks>
        /// by(Function<V, Object> function) is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37143
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupByWithCustomFunction()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement public GraphTraversal2 by(Function<V, Object> function)

                //var traversal = GraphViewCommand.g().V()
                //    .Both()
                //    .Properties("name")
                //    .Order()
                //    .By((a, b) => a.Value().CompareTo(b.Value()))
                //    .Dedup()
                //    .Value();
                //var result = traversal.Next();

                //CollectionAssert.AreEqual(new string[] { "josh", "lop", "marko", "peter", "ripple", "vadas" }, result);
            }
        }

        /// <summary>
        /// g_V_both_both_name_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().<String>values("name").dedup();
        /// </summary>
        [TestMethod]
        public void DedupLargeAmountStrings()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Values("name")
                    .Dedup();
                var result = traversal.Next();

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, result);
            }
        }

        /// <summary>
        /// g_V_both_both_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().dedup()
        /// </summary>
        [TestMethod]
        public void DedupLargeAmountVertexes()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup();
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                var names = new List<string>();
                foreach (dynamic v in result)
                {
                    names.Add((string)v.properties.name[0].value);
                }

                AbstractGremlinTest.CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, names);
            }
        }

        /// <summary>
        /// g_V_both_both_dedup_byXlabelX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().dedup().by(T.label);
        /// </summary>
        /// <remarks>
        /// Dedup().By() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37139
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupByLabel()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement Dedup().By()

                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup()
                    .By("label");
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count());
            }
        }

        /// <summary>
        /// g_V_group_byXlabelX_byXbothE_weight_dedup_foldX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().<String, List<Double>>group().by(T.label).by(bothE().values("weight").dedup().fold());
        /// </summary>
        /// <remarks>
        /// Deserialize fold() result
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37155
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupInsideBy()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Deserialize fold() result
                // The result in graphson is "[{{\"label\": \"person\"}: [0.5, 1, 0.4, 0.2], {\"label\": \"software\"}: [0.4, 0.2, 1]}]", 
                // which cannot be deserialized by Newtonsoft.
                // The exception Newtonsoft.Json.JsonReaderException: Invalid property identifier character: {. Path '[0]', line 1, position 2.
                var traversal = GraphViewCommand.g().V()
                    .Group()
                    .By("label")
                    .By(GraphTraversal2.__().BothE()
                        .Values("weight")
                        .Dedup()
                        .Fold());
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count());
                //CollectionAssert.AreEqual(new double[] { 0.2, 0.4, 1.0}, result["software"]);
                //CollectionAssert.AreEqual(new double[] { 0.2, 0.4, 0.5, 1.0 }, result["person"]);
            }
        }

        /// <summary>
        /// g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b");
        /// </summary>
        /// <remarks>
        /// Dedup().By() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37139
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupMultipleLabels()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement Dedup().By()
                // TODO: Use graphson to parse result.

                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Both()
                    .As("b")
                    .Dedup("a", "b")
                    .By("label")
                    .Select("a", "b");
                //IEnumerable<Dictionary<string, object>> result = traversal.Next();
                //IEnumerable<string> resultInString = result.Select(d=> string.Format("{0},{1}",d["a"].Label(), d["b"].Label());

                //Assert.AreEqual(3, result.Count());
                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "person,person", "person,software", "software,person"}, resultInString);
            }
        }

        /// <summary>
        /// g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").out("created").as("b").in("created").as("c").dedup("a", "b").path();
        /// </summary>
        /// <remarks>
        /// GremlinDedupVariable.ToTableReference() is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37160
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupTwoOutOfThreeLabels()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Implement GremlinDedupVariable.ToTableReference()
                // TODO: Use graphson to parse result.

                //var traversal = GraphViewCommand.g().V()
                //    .As("a")
                //    .Out("created")
                //    .As("b")
                //    .In("created")
                //    .As("c")
                //    .Dedup("a", "b")
                //    .Path();
                //IEnumerable<Path> result = traversal.Next();
                //IEnumerable<string> resultInString = result.Select(p=> string.Format("{0},{1}",p.Get("a").Value("name"), p.Get("b").Value("name"));

                //Assert.AreEqual(4, result.Count());
                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "marko,created", "vadas,created", "josh,created", "peter,created"}, resultInString);
            }
        }

        /// <summary>
        /// g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_incrX_selectXvX_valuesXnameX_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().outE().as("e").inV().as("v").select("e").order().by("weight", Order.incr).select("v").<String>values("name").dedup();
        /// </summary>
        /// <remarks>
        /// GremlinOrderOp.ModulateBy() is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37165
        /// GraphTraversal2.By(string key, GremlinKeyword.Order order) is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37166
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupWithOrder()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: GremlinOrderOp.ModulateBy() is not implemented
                // TODO: GraphTraversal2.By(string key, GremlinKeyword.Order order) is not implemented

                //var traversal = GraphViewCommand.g().V()
                //    .OutE()
                //    .As("e")
                //    .InV()
                //    .As("v")
                //    .Select("e")
                //    .Order()
                //    .By("weight", Order.Incr)
                //    .Select("v")
                //    .Values("name")
                //    .Dedup();
                //var result = traversal.Next();

                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "marko", "vadas", "josh", "peter"}, result);
            }
        }

        /// <summary>
        /// g_V_both_both_dedup_byXoutE_countX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().dedup().by(__.outE().count()).values("name");
        /// </summary>
        /// <remarks>
        /// GremlinOrderOp.ModulateBy() is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37165
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupByAnonymousTraversal()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: GremlinOrderOp.ModulateBy() is not implemented

                var traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup()
                    .By(GraphTraversal2.__()
                        .OutE()
                        .Count())
                    .Values("name");
                var result = traversal.Next();

                Assert.AreEqual(4, result.Count());
                Assert.IsTrue(result.Contains("josh"));
                Assert.IsTrue(result.Contains("peter"));
                Assert.IsTrue(result.Contains("marko"));
                Assert.IsTrue(result.Contains("vadas") || result.Contains("ripple") || result.Contains("lop"));
            }
        }

        /// <summary>
        /// g_V_groupCount_selectXvaluesX_unfold_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().groupCount().select(values).<Long>unfold().dedup();
        /// </summary>
        /// <remarks>
        /// GroupCount() Not Implemented on GraphTraversal2
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36609
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupWithGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: GroupCount() Not Implemented on GraphTraversal2

                //var traversal = GraphViewCommand.g().V()
                //    .GroupCount()
                //    .Select("values")
                //    .Unfold()
                //    .Dedup();
                //var result = traversal.Next();

                //AbstractGremlinTest.CheckUnOrderedResults(new string[] { "1" }, result);
            }
        }

        /// <summary>
        /// g_V_asXaX_repeatXbothX_timesX3X_emit_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_byXidX_foldX_selectXvaluesX_unfold_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").repeat(both()).times(3).emit().values("name").as("b").group().by(select("a")).by(select("b").dedup().order().fold()).select(values).<Collection<String>>unfold().dedup();
        /// </summary>
        /// <remarks>
        /// GremlinOrderOp.ModulateBy() is not implemented
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37165
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void TwoDedups()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: GremlinOrderOp.ModulateBy() is not implemented

                var traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Repeat(GraphTraversal2.__().Both())
                    .Times(3)
                    .Emit()
                    .Values("name")
                    .As("b")
                    .Group()
                    .By(GraphTraversal2.__().Select("a"))
                    .By(GraphTraversal2.__().Select("b")
                        .Dedup()
                        .Order()
                        .Fold())
                    .Select("values")
                    .Unfold()
                    .Dedup();

                var result = traversal.Next();

                CollectionAssert.AreEqual(new string[] { "josh", "lop", "marko", "peter", "ripple", "vadas" }, result);
            }
        }
        /// <summary>
        /// g_V_repeatXdedupX_timesX2X_count()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().repeat(dedup()).times(2).count();
        /// </summary>
        /// <remarks>
        /// Repeat(Dedup()) does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37181
        /// </remarks>
        [Ignore]
        [TestMethod]
        public void DedupInsideRepeat()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Repeat(Dedup()) does not work

                var traversal = GraphViewCommand.g().V()
                    .Repeat(GraphTraversal2.__().Dedup())
                    .Times(2)
                    .Count();

                var result = traversal.Next();

                CollectionAssert.AreEqual(new string[] { "josh", "lop", "marko", "peter", "ripple", "vadas" }, result);
            }
        }
    }
}
