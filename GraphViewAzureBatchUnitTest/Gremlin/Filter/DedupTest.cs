using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    [TestClass]
    public class DedupTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().out().in().values("name").fold().dedup(Scope.local).unfold();
        /// </summary>
        [TestMethod]
        public void DedupLocalScope()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Out()
                    .In()
                    .Values("name")
                    .Fold()
                    .Dedup(GremlinKeyword.Scope.Local)
                    .Unfold();
                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "marko", "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().out().as("x").in().as("y").select("x", "y").by("name").fold().dedup(Scope.local, "x", "y").unfold();
        /// </summary>
        [TestMethod]
        public void DedupLocalMultipleLabels()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .Out()
                    .As("x")
                    .In()
                    .As("y")
                    .Select("x", "y")
                    .By("name")
                    .Fold()
                    .Dedup(GremlinKeyword.Scope.Local, "x", "y")
                    .Unfold();
                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());
                List<string> expected = new List<string>
                {
                    "lop,marko",
                    "lop,josh",
                    "lop,peter",
                    "vadas,marko",
                    "josh,marko",
                    "ripple,josh"
                };
                CheckUnOrderedResults(expected, ((JArray)result).Select(p => string.Format("{0},{1}", p["x"], p["y"])).ToList());
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
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .Dedup()
                    .Values("name");
                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, result);
            }
        }

        /// <summary>
        /// g_V_both_hasXlabel_softwareX_dedup_byXlangX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().has(T.label, "software").dedup().by("lang").values("name");
        /// </summary>
        [TestMethod]
        public void DedupBy()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .HasLabel("software")
                    .Dedup()
                    .By("lang")
                    .Values("name");
                var result = this.jobManager.TestQuery(this.job);

                Assert.IsTrue(result.Contains("lop") || result.Contains("ripple"));
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
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Values("name")
                    .Dedup();
                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, result);
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
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup();
                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());
                GraphViewCommand.OutputFormat = OutputFormat.Regular;

                var names = new List<string>();
                foreach (dynamic v in result)
                {
                    names.Add((string)v.properties.name[0].value);
                }

                CheckUnOrderedResults(new string[] { "vadas", "josh", "lop", "marko", "peter", "ripple" }, names);
            }
        }

        /// <summary>
        /// g_V_both_both_dedup_byXlabelX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().dedup().by(T.label);
        /// </summary>
        [TestMethod]
        public void DedupByLabel()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup()
                    .By("label");
                var result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(2, result.Count());
            }
        }

        /// <summary>
        /// g_V_group_byXlabelX_byXbothE_weight_dedup_foldX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().<String, List<Double>>group().by(T.label).by(bothE().values("weight").dedup().fold());
        /// </summary>
        [TestMethod]
        public void DedupInsideBy()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .Group()
                    .By("label")
                    .By(GraphTraversal.__().BothE()
                        .Values("weight")
                        .Dedup()
                        .Fold());

                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());

                Assert.AreEqual(1, result.Count);
                CheckUnOrderedResults(new double[] { 0.2, 0.4, 1.0 }, ((JArray)result[0]["software"]).Select(p => (double)p).ToList());
                CheckUnOrderedResults(new double[] { 0.2, 0.4, 0.5, 1.0 }, ((JArray)result[0]["person"]).Select(p => (double)p).ToList());
            }
        }

        /// <summary>
        /// g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b");
        /// </summary>
        [TestMethod]
        public void DedupMultipleLabels()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Both()
                    .As("b")
                    .Dedup("a", "b")
                    .By("label")
                    .Select("a", "b");
                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());
                Assert.AreEqual(3, result.Count);

                IEnumerable<string> resultInString = ((JArray)result).Select(d => string.Format("{0},{1}", (string)d["a"]["label"], (string)d["b"]["label"]));

                CheckUnOrderedResults(new string[] { "person,person", "person,software", "software,person" }, resultInString);
            }
        }

        /// <summary>
        /// g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").out("created").as("b").in("created").as("c").dedup("a", "b").path();
        /// </summary>
        [TestMethod]
        public void DedupTwoOutOfThreeLabels()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Out("created")
                    .As("b")
                    .In("created")
                    .As("c")
                    .Dedup("a", "b")
                    .Path()
                    .By("name");

                dynamic results = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());

                List<string[]> AandBinPath = new List<string[]>();

                foreach (dynamic result in results)
                {
                    Assert.AreEqual(3, result["objects"].Count);
                    AandBinPath.Add(new[] { (string)result["objects"][0], (string)result["objects"][1] });
                }

                Assert.AreEqual(4, results.Count);

                List<Tuple<string, string>> correctUnOrderedResults = new List<Tuple<string, string>>
                {
                    {new Tuple<string, string>("marko", "lop")},
                    {new Tuple<string, string>("josh", "ripple")},
                    {new Tuple<string, string>("josh", "lop")},
                    {new Tuple<string, string>("peter", "lop")},
                };

                int counter = 0;
                foreach (string[] AandB in AandBinPath)
                {
                    string a = AandB[0];
                    string b = AandB[1];

                    foreach (Tuple<string, string> t in correctUnOrderedResults)
                    {
                        if (a.Equals(t.Item1) && b.Equals(t.Item2))
                        {
                            counter++;
                            break;
                        }
                    }
                }

                Assert.AreEqual(4, counter);
            }
        }

        /// <summary>
        /// g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_incrX_selectXvX_valuesXnameX_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().outE().as("e").inV().as("v").select("e").order().by("weight", Order.incr).select("v").<String>values("name").dedup();
        /// </summary>
        [TestMethod]
        public void DedupWithOrder()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .OutE()
                    .As("e")
                    .InV()
                    .As("v")
                    .Select("e")
                    .Order()
                    .By("weight", GremlinKeyword.Order.Incr)
                    .Select("v")
                    .Values("name")
                    .Dedup();
                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "lop", "vadas", "josh", "ripple" }, result);
            }
        }

        /// <summary>
        /// g_V_both_both_dedup_byXoutE_countX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().both().both().dedup().by(__.outE().count()).values("name");
        /// </summary>
        [TestMethod]
        public void DedupByAnonymousTraversal()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Both()
                    .Both()
                    .Dedup()
                    .By(GraphTraversal.__()
                        .OutE()
                        .Count())
                    .Values("name");
                var result = this.jobManager.TestQuery(this.job);

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
        [TestMethod]
        public void DedupWithGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V()
                    .GroupCount()
                    .Select(GremlinKeyword.Column.Values)
                    .Unfold()
                    .Dedup();
                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "1" }, result);
            }
        }

        /// <summary>
        /// g_V_asXaX_repeatXbothX_timesX3X_emit_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_byXidX_foldX_selectXvaluesX_unfold_dedup()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().as("a").repeat(both()).times(3).emit().values("name").as("b").group().by(select("a")).by(select("b").dedup().order().fold()).select(values).<Collection<String>>unfold().dedup();
        /// </summary>
        [TestMethod]
        public void TwoDedups()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = GraphViewCommand.g().V()
                    .As("a")
                    .Repeat(GraphTraversal.__().Both())
                    .Times(3)
                    .Emit()
                    .Values("name")
                    .As("b")
                    .Group()
                    .By(GraphTraversal.__().Select("a"))
                    .By(GraphTraversal.__().Select("b")
                        .Dedup()
                        .Order()
                        .Fold())
                    .Select(GremlinKeyword.Column.Values)
                    .Unfold()
                    .Dedup();

                dynamic result = JsonConvert.DeserializeObject<dynamic>(this.jobManager.TestQuery(this.job).FirstOrDefault());
                var temp = ((JArray)result[0]).Select(j => j.ToString()).ToList();
                CollectionAssert.AreEqual(new string[] { "josh", "lop", "marko", "peter", "ripple", "vadas" }, temp);
            }
        }
        /// <summary>
        /// g_V_repeatXdedupX_timesX2X_count()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/filter/DedupTest.java
        /// Gremlin: g.V().repeat(dedup()).times(2).count();
        /// </summary>
        [TestMethod]
        public void DedupInsideRepeat()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V()
                    .Repeat(GraphTraversal.__().Dedup())
                    .Times(2)
                    .Values("name");

                var result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "marko", "vadas", "lop", "josh", "ripple", "peter" }, result);
            }
        }
    }
}
