using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    /// <summary>
    /// Tests for Where Step.
    /// </summary>
    [TestClass]
    public class WhereTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]
        public void WhereNeqTest()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V("dummy").As("a").Out("created").In("created").Where(Predicate.neq("a"));
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                Console.WriteLine("Result Count: " + result.Count);
            }
        }
        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', eq('b'))"
        /// </summary>
        [TestMethod]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAEqB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b").Where("a", Predicate.eq("b"));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(6, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + markoId,
                    "a," + markoId + ";b," + markoId,
                    "a," + markoId + ";b," + markoId,
                    "a," + joshId + ";b," + joshId,
                    "a," + joshId + ";b," + joshId,
                    "a," + peterId + ";b," + peterId,
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', neq('b'))"
        /// </summary>
        [TestMethod]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereANeqB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b").Where("a", Predicate.neq("b"));
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(6, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + joshId,
                    "a," + markoId + ";b," + peterId,
                    "a," + joshId + ";b," + markoId,
                    "a," + joshId + ";b," + peterId,
                    "a," + peterId + ";b," + markoId,
                    "a," + peterId + ";b," + joshId,
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('b').has('name', 'marko'))"
        /// </summary>
        [TestMethod]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAsBHasNameMarko()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b")
                                                    .Where(GraphTraversal.__().As("b").Has("name", "marko"));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(5, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + markoId,
                    "a," + markoId + ";b," + markoId,
                    "a," + markoId + ";b," + markoId,
                    "a," + joshId + ";b," + markoId,
                    "a," + peterId + ";b," + markoId,
                };
                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('a').out('knows').as('b'))"
        /// </summary>
        [TestMethod]
        public void VerticesHasAgeAsAOutInHasAgeAsBSelectABWhereAsAOutKnowsB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().Has("age").As("a")
                                                    .Out().In().Has("age").As("b")
                                                    .Select("a", "b")
                                                    .Where(GraphTraversal.__().As("a").Out("knows").As("b"));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(1, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + joshId,
                };
                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().as('a').out('created').where(__.as('a').name.is('josh')).in('created').name"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutCreatedWhereAsANameIsJoshInCreatedName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().As("a")
                                                    .Out("created")
                                                    .Where(GraphTraversal.__().As("a").Values("name").Is("josh"))
                                                    .In("created").Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new[] { "josh", "josh", "marko", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').as('b').where('a', neq('b')).name", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        
        public void HasVextexIdAsAOutCreatedInCreatedAsBWhereANeqBValuesName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created").As("b")
                                                    .Where("a", Predicate.neq("b")).Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "josh", "peter" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').as('b').where(__.as('b').out('created').has('name','ripple')).values('age','name')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVextexIdAsAOutCreatedInCreatedAsBWhereAsBOutCreatedHasNameRippleValuesAgeName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                // Skipping this validation until we can fix the bugs.

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created").As("b")
                                                    .Where(GraphTraversal.__().As("b").Out("created").Has("name", "ripple"))
                                                    .Values("age", "name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
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
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created")
                                                    .Where(Predicate.eq("a")).Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckOrderedResults(new List<string> { "marko" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).as('a').out('created').in('created').where(neq('a')).name", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        
        public void HasVertexIdAsAOutCreatedInCreatedWhereNeqAVaulesName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId).As("a")
                                                    .Out("created").In("created")
                                                    .Where(Predicate.neq("a")).Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "peter", "josh" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_out_aggregateXxX_out_whereXwithout_xX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out.aggregate('x').out.where(P.not(within('x')))", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        
        public void HasVertexIdOutAggregateXOutWhereNotWithinX()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId).Out()
                                                    .Aggregate("x").Out()
                                                    .Where(Predicate.not(Predicate.within("x")));

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckOrderedResults(new List<string> { "ripple" }, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V(v1Id).repeat(__.bothE('created').where(without('e')).aggregate('e').otherV).emit.path", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVertexIdRepeatBothECreatedWhereWithoutEAggregateEOtherVEmitPath()
        {
            //==>[v[1], e[9][1 - created->3], v[3]]
            //==>[v[1], e[9][1 - created->3], v[3], e[11][4 - created->3], v[4]]
            //==>[v[1], e[9][1 - created->3], v[3], e[12][6 - created->3], v[6]]
            //==>[v[1], e[9][1 - created->3], v[3], e[11][4 - created->3], v[4], e[10][4 - created->5], v[5]]
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.Regular;

                this.job.Traversal = graphCommand.g().V().HasId(markoVertexId)
                                                    .Repeat(GraphTraversal.__().BothE("created")
                                                                                .Where(Predicate.without("e"))
                                                                                .Aggregate("e")
                                                                                .OtherV())
                                                    .Emit().Path();

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                Assert.AreEqual(4, result.Count);

                //No idea how to evaluate the results.
            }
        }

        /// <summary>
        /// Port of the g_V_whereXnotXoutXcreatedXXX_name UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.where(__.not(out('created'))).name"
        /// </summary>
        [TestMethod]
        
        public void VerticesWhereNotOutCreatedValuesN()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Where(GraphTraversal.__().Not(
                                                                GraphTraversal.__().Out("created")))
                                                    .Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "vadas", "lop", "ripple" }, result);

                // Skipping this validation until we can fix the bugs.
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().as('a').out().as('b').where(and(__.as('a').out('knows').as('b'),or(__.as('b').out('created').has('name','ripple'),__.as('b').in('knows').count().is(P.not(eq(0)))))).select('a','b')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutABWhereAndAsAOutKnowsAsBOrAsBOutCreatedHasNameRippleAsBInKnowsCountIsNotEq0SelectAB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V()
                    .As("a")
                    .Out()
                    .As("b")
                    .Where(
                        GraphTraversal.__()
                            .And(
                                GraphTraversal.__()
                                    .As("a")
                                    .Out("knows")
                                    .As("b"),
                                GraphTraversal.__()
                                    .Or(
                                        GraphTraversal.__()
                                            .As("b")
                                            .Out("created")
                                            .Has("name", "ripple"),
                                        GraphTraversal.__().As("b")
                                            .In("knows")
                                            .Count()
                                            .Is(Predicate.not(Predicate.eq(0))))))
                    .Select("a", "b");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var vadasId = ConvertToVertexId(graphCommand, "vadas");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + joshId,
                    "a," + markoId + ";b," + vadasId,
                };
                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_selectXaX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "gremlin-groovy", "g.V.where(out('created').and.out('knows').or.in('knows')).name"
        /// </summary>
        [TestMethod]
        
        public void VerticesWhereOutCreatedAndOutKnowsORInKnowsValueName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V().Where(GraphTraversal.__().Out("created")
                                                                               .And()
                                                                               .Out("knows")
                                                                               .Or()
                                                                               .In("knows"))
                                                    .Values("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(new List<string> { "marko", "vadas", "josh" }, result);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').as('b').where(and(__.as('b').in,__.not(__.as('a').out('created').has('name','ripple')))).select('a','b')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutCreatedAsBWhereAndAsBInNotAsAOutCreatedHasNameRippleSelectAB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().As("a")
                                                    .Out("created").As("b")
                                                    .Where(GraphTraversal.__().And(
                                                                                    GraphTraversal.__().As("b")
                                                                                                        .In(),
                                                                                    GraphTraversal.__().Not(
                                                                                                        GraphTraversal.__().As("a")
                                                                                                                            .Out("created")
                                                                                                                            .Has("name", "ripple"))))
                                                    .Select("a", "b");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var lopId = ConvertToVertexId(graphCommand, "lop");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + lopId,
                    "a," + peterId + ";b," + lopId,
                };
                CheckUnOrderedResults(expected, ans);

            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out('created').as('b').in('created').as('c').both('knows').both('knows').as('d').where('c',P.not(eq('a').or(eq('d')))).select('a','b','c','d')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutCreatedAsBInCreatedAsCBothKnowsBothKnowsAsDWhereCNotEqAOrEqDSelectABCD()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().As("a")
                                                    .Out("created").As("b")
                                                    .In("created").As("c")
                                                    .Both("knows").Both("knows").As("d")
                                                    .Where(
                                                        GraphTraversal.__().Where("c", Predicate.not(Predicate.eq("a")))
                                                                            .And()
                                                                            .Where("c", Predicate.not(Predicate.eq("d"))))
                                                    .Select("a", "b", "c", "d");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(2, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString()
                         + ";b," + temp["b"]["id"].ToString()
                         + ";c," + temp["c"]["id"].ToString()
                         + ";d," + temp["d"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var lopId = ConvertToVertexId(graphCommand, "lop");
                var vadasId = ConvertToVertexId(graphCommand, "vadas");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + lopId + ";c," + joshId + ";d," + vadasId,
                    "a," + peterId + ";b," + lopId + ";c," + joshId + ";d," + vadasId
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').out.as('b').where(__.as('b').in.count.is(eq(3)).or.where(__.as('b').out('created').and.as('b').has(label,'person'))).select('a','b')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutAsBWhereInCountIsEq3OrWhereOutCreatedAndHasLabelPersonSelectAB()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g()
                    .V()
                    .As("a")
                    .Out()
                    .As("b")
                    .Where(
                        GraphTraversal.__()
                            .As("b")
                            .In()
                            .Count()
                            .Is(Predicate.eq(3))
                            .Or()
                            .Where(
                                GraphTraversal.__()
                                    .As("b")
                                    .Out("created")
                                    .And()
                                    .As("b")
                                    .HasLabel("person")))
                    .Select("a", "b");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(4, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"]["id"].ToString() + ";b," + temp["b"]["id"].ToString());
                }
                var markoId = ConvertToVertexId(graphCommand, "marko");
                var joshId = ConvertToVertexId(graphCommand, "josh");
                var lopId = ConvertToVertexId(graphCommand, "lop");
                var peterId = ConvertToVertexId(graphCommand, "peter");

                List<string> expected = new List<string>
                {
                    "a," + markoId + ";b," + lopId,
                    "a," + markoId + ";b," + joshId,
                    "a," + joshId + ";b," + lopId,
                    "a," + peterId + ";b," + lopId
                };
                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().as('a').out('created').in('created').as('b').where('a', gt('b')).by('age').select('a', 'b').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutCreatedInCreatedAsBWhereAGtBByAgeSelectABByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().As("a")
                                                    .Out("created").In("created").As("b")
                                                    .Where("a", Predicate.gt("b"))
                                                    .By("age")
                                                    .Select("a", "b").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(3, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";b," + temp["b"].ToString());
                }

                List<string> expected = new List<string>
                {
                    "a,josh;b,marko",
                    "a,peter;b,marko",
                    "a,peter;b,josh",
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V.as('a').outE('created').as('b').inV().as('c').where('a', gt('b').or(eq('b'))).by('age').by('weight').by('weight').select('a', 'c').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutECreatedAsBInVAsCWhereAGtBOrEqBByAgeByWeightByWeightSelectACByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().As("a").OutE("created").As("b").InV().As("c").Where("a", Predicate.gt("b").Or(Predicate.eq("b"))).By("age").By("weight").By("weight").Select("a", "c").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(4, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";c," + temp["c"].ToString());
                }

                List<string> expected = new List<string>
                {
                    "a,marko;c,lop",
                    "a,josh;c,ripple",
                    "a,josh;c,lop",
                    "a,peter;c,lop",
                };

                CheckUnOrderedResults(expected, ans);
            }
        }

        /// <summary>
        /// Port of the g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX UT from org/apache/tinkerpop/gremlin/process/traversal/step/map/WhereTest.java.
        /// Equivalent gremlin: "g.V().as('a').outE('created').as('b').inV().as('c').in('created').as('d').where('a', lt('b').or(gt('c')).and(neq('d'))).by('age').by('weight').by(__.in('created').values('age').min()).select('a', 'c', 'd').by('name')"
        /// </summary>
        [TestMethod]
        public void VerticesAsAOutECreatedAsBInVAsCInCreatedAsDWhereALtBOrGtCAndNeqDByAgeByWeightByInCreatedValuesAgeMinSelectACDByName()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                this.job.Traversal = graphCommand.g().V().As("a").OutE("created").As("b").InV().As("c").In("created").As("d").Where("a", Predicate.lt("b").Or(Predicate.gt("c")).And(Predicate.neq("d"))).By("age").By("weight").By(GraphTraversal.__().In("created").Values("age").Min()).Select("a", "c", "d").By("name");

                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic dynamicResult = JsonConvert.DeserializeObject<dynamic>(result.FirstOrDefault());

                Assert.AreEqual(4, dynamicResult.Count);
                List<string> ans = new List<string>();
                foreach (var temp in dynamicResult)
                {
                    ans.Add("a," + temp["a"].ToString() + ";c," + temp["c"].ToString() + ";d," + temp["d"].ToString());
                }

                List<string> expected = new List<string>
                {
                    "a,josh;c,lop;d,marko",
                    "a,josh;c,lop;d,peter",
                    "a,peter;c,lop;d,marko",
                    "a,peter;c,lop;d,josh",
                };

                CheckUnOrderedResults(expected, ans);
            }
        }
    }
}
