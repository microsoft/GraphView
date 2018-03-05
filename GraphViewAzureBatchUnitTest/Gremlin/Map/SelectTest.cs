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
    public class SelectTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('knows').as('b').select('a','b')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutKnowsAsBSelectAB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");
                string vadasVertexId = this.ConvertToVertexId(graphCommand, "vadas");
                string joshVertexId = this.ConvertToVertexId(graphCommand, "josh");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .HasId(markoVertexId).As("a")
                    .Out("knows").As("b")
                    .Select("a", "b");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual(markoVertexId, res["a"]["id"].ToString());
                    Assert.IsTrue(vadasVertexId.Equals(res["b"]["id"].ToString()) ||
                                  joshVertexId.Equals(res["b"]["id"].ToString()));
                }
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('knows').as('b').select('a','b').by('name')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutKnowsAsBSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .HasId(markoVertexId).As("a")
                    .Out("knows").As("b")
                    .Select("a", "b").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual("marko", res["a"].ToString());
                    Assert.IsTrue("vadas".Equals(res["b"].ToString()) || "josh".Equals(res["b"].ToString()));
                }
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXknowsX_asXbX_selectXaX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('knows').as('b').select('a')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutKnowsAsBSelectA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .HasId(markoVertexId).As("a")
                    .Out("knows").As("b")
                    .Select("a");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual(markoVertexId, res["id"].ToString());
                }
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('knows').as('b').select('a').by('name')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsAOutKnowsAsBSelectAByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .HasId(markoVertexId).As("a")
                    .Out("knows").As("b")
                    .Select("a").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual("marko", res.ToString());
                }
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_asXbX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.as('a').out.as('b').select('a','b').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutAsBSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .As("a")
                    .Out().As("b")
                    .Select("a", "b").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";b," + temp["b"].ToString());
                }

                AssertCommonA(ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.as('a').out.aggregate('x').as('b').select('a','b').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutAggregateXAsBSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .As("a")
                    .Out().Aggregate("x").As("b")
                    .Select("a", "b").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";b," + temp["b"].ToString());
                }

                AssertCommonA(ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V().as('a').name.order().as('b').select('a','b').by('name').by"
        /// </summary>
        [TestMethod]
        public void VerticesAsAValuesNameOrderAsBSelectABByNameBy()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .As("a")
                    .Values("name")
                    .Order().As("b").Select("a", "b").By("name")
                    .By();

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";b," + temp["b"].ToString());
                }

                List<string> expected = new List<string>
                {
                    "a,marko;b,marko",
                    "a,vadas;b,vadas",
                    "a,josh;b,josh",
                    "a,ripple;b,ripple",
                    "a,lop;b,lop",
                    "a,peter;b,peter"
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXname_isXmarkoXX_asXaX_selectXaX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.has('name',__.is('marko')).as('a').select('a')"
        /// </summary>
        [TestMethod]
        public void HasNameIsMarkoAsASelectA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .Has("name", GraphTraversal.__().Is("marko"))
                    .As("a").Select("a");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                Assert.AreEqual(1, dynamicResult.Count);
                Assert.AreEqual(ConvertToVertexId(graphCommand, "marko"), (string) dynamicResult[0]["id"]);
            }
        }

        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('person').as('p').map(__.bothE.label.groupCount()).as('r').select('p','r')"
        /// </summary>
        [TestMethod]
        public void HasLabelPersonAsPMapBothELabelGroupCountAsRSelectPR()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V()
                    .HasLabel("person").As("p")
                    .Map(GraphTraversal.__().BothE().Label().GroupCount()).As("r").Select("p", "r");

                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());

                Assert.AreEqual(4, dynamicResult.Count);
                foreach (var result in dynamicResult)
                {
                    if ((string) result.p.id == ConvertToVertexId(graphCommand, "marko"))
                    {
                        Assert.AreEqual(1, (int) result.r["created"]);
                        Assert.AreEqual(2, (int) result.r["knows"]);
                    }
                    else if ((string) result.p.id == ConvertToVertexId(graphCommand, "vadas"))
                    {
                        Assert.AreEqual(1, (int) result.r["knows"]);
                    }
                    else if ((string) result.p.id == ConvertToVertexId(graphCommand, "josh"))
                    {
                        Assert.AreEqual(2, (int) result.r["created"]);
                        Assert.AreEqual(1, (int) result.r["knows"]);
                    }
                    else if ((string) result.p.id == ConvertToVertexId(graphCommand, "peter"))
                    {
                        Assert.AreEqual(1, (int) result.r["created"]);
                    }
                    else
                    {
                        Assert.Fail("Unknown vertex result");
                    }
                }
            }
        }

        /// <summary>
        /// Port of the g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.choose(__.outE().count().is(0L), __.as('a'), __.as('b')).choose(select('a'),select('a'),select('b'))"
        /// </summary>
        [TestMethod]
        public void ChooseOutECountIs0AsAsBChooseSelectASelectASelectB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Choose(
                        GraphTraversal.__().OutE().Count().Is(0L),
                        GraphTraversal.__().As("a"),
                        GraphTraversal.__().As("b"))
                    .Choose(
                        GraphTraversal.__().Select("a"),
                        GraphTraversal.__().Select("a"),
                        GraphTraversal.__().Select("b"));

                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());

                int counter = 0;
                int xCounter = 0;
                int yCounter = 0;

                foreach (var result in dynamicResult)
                {
                    counter++;
                    if ((string) result.id == ConvertToVertexId(graphCommand, "vadas")
                        || (string) result.id == ConvertToVertexId(graphCommand, "lop")
                        || (string) result.id == ConvertToVertexId(graphCommand, "ripple"))
                        xCounter++;
                    else if ((string) result.id == ConvertToVertexId(graphCommand, "marko")
                             || (string) result.id == ConvertToVertexId(graphCommand, "josh")
                             || (string) result.id == ConvertToVertexId(graphCommand, "peter"))
                        yCounter++;
                    else
                        Assert.Fail("This state should not have occurred");
                }

                Assert.AreEqual(6, counter);
                Assert.AreEqual(3, yCounter);
                Assert.AreEqual(3, xCounter);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXhereX_out_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('here').out.select('here')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdAsHereOutSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).As("here").Out().Select("here");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(3, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual("marko", res["properties"]["name"][0]["value"].ToString());
                }
            }
        }

        /// <summary>
        /// Port of the g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v4Id).out.as('here').has('lang', 'java').select('here')", "v4Id", v4Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdOutAsHereHasLangJavaSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "josh");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).Out().As("here").Has("lang", "java")
                    .Select("here");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual("java", res["properties"]["lang"][0]["value"].ToString());
                    string propertiesNameVal = res["properties"]["name"][0]["value"].ToString();
                    Assert.IsTrue(string.Equals("ripple", propertiesNameVal) ||
                                  string.Equals("lop", propertiesNameVal));
                }
            }
        }

        /// <summary>
        /// Port of the g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).outE.as('here').inV.has('name', 'vadas').select('here')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdOutEAsHereInVHasNameVadasSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(graphCommand, "vadas");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).OutE().As("here").InV().Has("name", "vadas")
                    .Select("here");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);
                var edge = dynamicResult[0];
                Assert.AreEqual("knows", edge["label"].ToString());
                Assert.AreEqual(vertexId2, edge["inV"].ToString());
                Assert.AreEqual(vertexId1, edge["outV"].ToString());
                Assert.AreEqual("0.5", edge["properties"]["weight"].ToString());
            }
        }

        /// <summary>
        /// Port of the g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v4Id).out.as('here').has('lang', 'java').select('here').name", "v4Id", v4Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdOutAsHereHasLangJavaSelectHereName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "josh");
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).Out().As("here").Has("lang", "java")
                    .Select("here").Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                Assert.AreEqual(2, result.Count);
                Assert.IsTrue(result.Contains("ripple"));
                Assert.IsTrue(result.Contains("lop"));
            }
        }

        /// <summary>
        /// Port of the g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').select('here')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdOutEKnowsHasWeight1AsHereInVHasNameJoshSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).OutE("knows").Has("weight", 1.0d).As("here")
                    .InV().Has("name", "josh").Select("here");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                AssertCommonB(dynamicResult);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name', 'josh').select('here')", "v1Id", v1Id)
        /// </summary>
        [TestMethod]
        public void HasVertexIdOutEKnowsAsHereHasWeight1AsFakeInVHasNameJoshSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasId(vertexId1).OutE("knows").As("here").Has("weight", 1.0d)
                    .As("fake").InV().Has("name", "josh").Select("here");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());
                AssertCommonB(dynamicResult);
            }
        }

        /// <summary>
        /// Port of the g_V_asXhereXout_name_selectXhereX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V().as('here').out.name.select('here')"
        /// </summary>
        [TestMethod]
        public void VerticesAsHereOutValuesNameSelectHere()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().As("here").Out().Values("name").Select("here").Values("name");

                // NOTE: actual tests for complete vertice, but here we just validate that the names are correct. We can do so since the names are unique.
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                var expectedResult = new List<string> {"marko", "marko", "marko", "josh", "josh", "peter"};
                CheckUnOrderedResults(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.as('a').has('name', 'marko').as('b').as('c').select('a','b','c').by().by('name').by('age')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAHasNameMarkoAsBAsCSelectABCByByNameByAge()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().As("a").Has("name", "marko").As("b").As("c")
                    .Select("a", "b", "c").By().By("name").By("age");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);
                foreach (var res in dynamicResult)
                {
                    Assert.AreEqual(ConvertToVertexId(graphCommand, "marko"), res["a"]["id"].ToString());
                    Assert.AreEqual("marko", res["b"].ToString());
                    Assert.AreEqual(29, (int) res["c"]);
                }
            }
        }

        /// <summary>
        /// Port of the g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('software').as('name').as('language').as('creators').select('name','language','creators').by('name').by('lang').
        ///                         by(__.in('created').values('name').fold().order(local))"
        /// </summary>
        [TestMethod]
        public void
            HasLabelSoftwareAsNameAsLanguageAsCreatorsSelectNameLanguageCreatorsByNameByLangByInCreatedValuesNameFoldOrderLocal()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().HasLabel("software").As("name").As("language").As("creators")
                    .Select("name", "language", "creators").By("name").By("lang").By(GraphTraversal.__().In("created")
                        .Values("name").Fold().Order(GremlinKeyword.Scope.Local));

                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var result in dynamicResult)
                {
                    if ((string) result["name"] == "lop")
                    {
                        Assert.AreEqual("java", (string) result["language"]);
                        CheckOrderedResults(new[] {"josh", "marko", "peter"}, ConvertToList(result["creators"]));
                    }
                    else if ((string) result["name"] == "ripple")
                    {
                        Assert.AreEqual("java", (string) result["language"]);
                        CheckOrderedResults(new[] {"josh"}, ConvertToList(result["creators"]));
                    }
                    else
                    {
                        Assert.Fail("Unknown result");
                    }
                }
            }
        }

        /// <summary>
        /// Port of the g_V_selectXaX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V." + (null == pop ? "select('a')" : "select(${pop}, 'a')"
        /// </summary>
        [TestMethod]
        public void VerticesSelectA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                GremlinKeyword.Pop?[] pops =
                    {null, GremlinKeyword.Pop.All, GremlinKeyword.Pop.First, GremlinKeyword.Pop.Last};
                foreach (var pop in pops)
                {
                    var root = graphCommand.g().V();
                    this.job.Traversal = (!pop.HasValue) ? root.Select("a") : root.Select(pop.Value, "a");

                    List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                    Assert.AreEqual(0, result.Count);
                }
            }
        }


        // NOTE: g_V_selectXa_bX, g_V_valueMap_selectXpop_aX, g_V_valueMap_selectXpop_a_bX UTs also have the the same issue as the above method.
        // Hence I'm not adding them right now.


        /// <summary>
        /// Port of the g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.until(__.out.out).repeat(__.in.as('a')).select('a').by(tail(local).name)"
        /// </summary>
        [TestMethod]
        public void VerticesUntilOutOutRepeatInAsASelectAByTailLocalName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Until(GraphTraversal.__().Out().Out())
                    .Repeat(GraphTraversal.__().In().As("a")).Select("a")
                    .By(GraphTraversal.__().Tail(GremlinKeyword.Scope.Local).Unfold().Values("name"));

                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                Assert.AreEqual(5, results.Count);
                foreach (var result in results)
                {
                    Assert.AreEqual("marko", result);
                }
            }
        }

        /// <summary>
        /// Port of the g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.until(__.out.out).repeat(__.in.as('a').in.as('b')).select('a','b').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesUntilOutOutRepeatInAsAInAsBSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Until(GraphTraversal.__().Out().Out())
                    .Repeat(GraphTraversal.__().In().As("a").In().As("b")).Select("a", "b").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var temp in dynamicResult)
                {
                    Assert.AreEqual("josh", temp["a"].ToString());
                    Assert.AreEqual("marko", temp["b"].ToString());
                }
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_whereXoutXknowsXX_selectXaX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V().as('a').where(out('knows')).select('a')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAWhereOutKnowsSelectA()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().As("a").Where(GraphTraversal.__().Out("knows")).Select("a");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);
                var vertex = dynamicResult[0];
                Assert.AreEqual("marko", vertex["properties"]["name"][0]["value"].ToString());
            }
        }

        /// <summary>
        /// Port of the g_V_outE_weight_groupCount_selectXkeysX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.outE.weight.groupCount.select(keys).unfold"
        /// </summary>
        [TestMethod]
        public void VerticesOutEValuesWeightGroupCountSelectKeys()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().OutE().Values("weight").GroupCount()
                    .Select(GremlinKeyword.Column.Keys).Unfold();
                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] {"0.5", "1", "0.4", "0.2"}, results);
            }
        }

        // NOTE: g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold, get_g_V_asXa_bX_out_asXcX_path_selectXkeysX porting is skipped for the same reason as above.

        /// <summary>
        /// Port of the g_V_outE_weight_groupCount_selectXvaluesX_unfold UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.outE.weight.groupCount.select(values).unfold"
        /// </summary>
        [TestMethod]
        public void VerticesOutEValuesWeightGroupCountSelectValues()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().OutE().Values("weight").GroupCount()
                    .Select(GremlinKeyword.Column.Values).Unfold();
                var results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] {"1", "1", "2", "2"}, results);
            }
        }

        // NOTE: g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold,g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold
        //porting is skipped for the same reason as above.

        /// <summary>
        /// Port of the g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/SelectTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('knows').as('b').local(select('a', 'b').by('name'))"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutKnowsAsBLocalSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().As("a").Out("knows").As("b")
                    .Local(GraphTraversal.__().Select("a", "b").By("name"));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                foreach (var temp in dynamicResult)
                {
                    if (temp["a"].ToString() == "marko")
                    {
                        Assert.IsTrue("vadas".Equals(temp["b"].ToString()) || "josh".Equals(temp["b"].ToString()));
                    }
                    else
                    {
                        Assert.Fail("Unknown result");
                    }
                }
            }
        }

        private void AssertCommonA(dynamic traversalResult)
        {
            List<string> expected = new List<string>
            {
                "a,marko;b,lop",
                "a,marko;b,vadas",
                "a,marko;b,josh",
                "a,josh;b,ripple",
                "a,josh;b,lop",
                "a,peter;b,lop"
            };

            CheckUnOrderedResults(expected, traversalResult);
        }

        private void AssertCommonB(dynamic dynamicResult)
        {
            Assert.AreEqual(1, dynamicResult.Count);
            var edge = dynamicResult[0];
            Assert.AreEqual("knows", edge["label"].ToString());
            Assert.AreEqual(1.0D, double.Parse(edge["properties"]["weight"].ToString()));
        }
    }
}