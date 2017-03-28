using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Filter
{

    // In the analogous gremlin-test, more checks relating to .hasNext() of a traversal iterator are done,
    // but doing so in our implementation isn't possible, hence we've skipped them.
    // WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36513

    // In the analogous gremlin-test, the results are returned backed using the data model, and not values themselves.
    // But since we cannot do so in our implementation, I'm making use of Values(), Label(), etc..., to assert for results.
    // The same applies for a lot of the tests above.
    // chkraw is working on porting the TinkerPop java API's, which might be able address this.

    /// V(id) doesn't seem to work, so I've replaced it with V().HasId(id) and V().Has("id", ...).
    /// Maybe we should look into why V(id) doesn't work.
    /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36517

    /// <summary>
    /// Tests for Has Step.
    /// </summary>
    [TestClass]
    public sealed class HasTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.out('created').has('name',map{it.length()}.is(gt(3))).name"
        /// </summary>
        /// <remarks>
        /// Actual gremlin-test has the following traversal:
        /// g.V.out('created').has('name', map{ it.length()}.is(gt(3))).name
        /// Need to sync with Jeff to understand if we can do an equivalent Map.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36514
        /// </remarks>
        [TestMethod]
        [Ignore]
        public void OutCreatedHasNameLengthGT3()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                Assert.Fail();
                //var traversal = GraphViewCommand.g().V().Out("created").Has("name", Predicate.gt(3));
                //var result = traversal.Values("name").Next();
                //Assert.AreEqual(1, result.Count);
                //Assert.AreEqual("ripple", result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_VX1X_hasXkeyX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).has(k)", "v1Id", v1Id, "k", key
        /// </summary>
        /// <remarks>
        /// A bug fix is required in \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\variables\GremlinVariable.cs Line 368,
        /// Has(GremlinToSqlContext currentContext, string propertyKey) currently throws a throw new NotImplementedException();
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36515
        /// </remarks>
        [TestMethod]
        public void HasVIdHasName()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                var traversal = GraphViewCommand.g().V().HasId(vertexId).Has("name");

                var result = traversal.Values("name").Next();
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("marko", result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_VX1X_hasXname_markoX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).has('name', 'marko')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVIdHasNameMarko()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                var traversal = GraphViewCommand.g().V().HasId(vertexId).Has("name", "marko");

                var result = traversal.Values("name").Next();
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("marko", result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_V_hasXname_markoX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('name', 'marko')"
        /// </summary>
        [TestMethod]
        public void HasNameMarko()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("name", "marko");

                var result = traversal.Values("name").Next();
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("marko", result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_V_hasXname_blahX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('name', 'blah')"
        /// </summary>
        [TestMethod]
        public void HasNameBlah()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("name", "blah");

                var result = traversal.Next().FirstOrDefault();
                Assert.IsNull(result);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('age',gt(30))"
        /// </summary>
        [TestMethod]
        public void HasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("age", Predicate.gt(30));

                var result = traversal.Values("age").Next();
                Assert.AreEqual(2, result.Count);
                foreach (var age in result)
                {
                    Assert.IsTrue(int.Parse(age) > 30);
                }
            }
        }

        /// <summary>
        /// Port of the g_V_hasXage_isXgt_30XX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('age', __.is(gt(30)))"
        /// </summary>
        /// <remarks>
        /// This test fails because Has Property Traversal doesn't seem to be supported yet.
        /// \development\euler\product\microsoft.azure.graph\graphview\gremlintranslation2\filter\gremlinhasop.cs Line 118.
        /// HasOpType.HasKeyTraversal is not implemented
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36516
        /// </remarks>
        [TestMethod]
        public void HasAgeIsGT30()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("age", GraphTraversal2.__().Is(Predicate.gt(30)));

                var result = traversal.Values("age").Next();
                Assert.AreEqual(2, result.Count);
                foreach (var age in result)
                {
                    Assert.IsTrue(int.Parse(age) > 30);
                }
            }
        }

        /// <summary>
        /// Port of the g_VX1X_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).has('age',gt(30))", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVIdHasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "josh");

                var traversal = GraphViewCommand.g().V().HasId(vertexId1).Has("age", Predicate.gt(30));

                var traversal2 = GraphViewCommand.g().V().HasId(vertexId2).Has("age", Predicate.gt(30));

                var result = traversal.Next();
                Assert.AreEqual(0, result.Count);

                var result2 = traversal2.Next();
                Assert.AreEqual(1, result2.Count);
            }
        }

        /// <summary>
        /// Port of the g_VXv1X_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(g.V(v1Id).next()).has('age',gt(30))", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// This test fails because Has Property Traversal doesn't seem to be supported yet.
        /// \development\euler\product\microsoft.azure.graph\graphview\gremlintranslation2\filter\gremlinhasop.cs Line 118.
        /// HasOpType.HasKeyTraversal is not implemented
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36516
        /// </remarks>
        [TestMethod]
        public void HasIdTraversalHasVIdHasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "josh");

                //var traversal = GraphViewCommand.g().V()
                //    .Has("id", GraphTraversal2.__().V().HasId(vertexId1))
                //    .Has("age", Predicate.gt(30));
                var traversal = GraphViewCommand.g().V(vertexId1)
                    .Has("age", Predicate.gt(30));

                var result = traversal.Next();
                Assert.AreEqual(0, result.Count);

                //var traversal2 = GraphViewCommand.g().V()
                //    .Has("id", GraphTraversal2.__().V().HasId(vertexId2))
                //    .Has("age", Predicate.gt(30));

                var traversal2 = GraphViewCommand.g().V(vertexId2)
                    .Has("age", Predicate.gt(30));

                var result2 = traversal2.Next();
                Assert.AreEqual(1, result2.Count);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_out_hasXid_2X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out.hasId(v2Id)", "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        [TestMethod]
        public void HasVIdOutHasVId()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string markoVertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vadasVertexId = this.ConvertToVertexId(GraphViewCommand, "vadas");

                var traversal = GraphViewCommand.g().V().HasId(markoVertexId)
                    .Out().HasId(vadasVertexId);

                this.AssertVadasAsOnlyValueReturned(GraphViewCommand, traversal);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_out_hasXid_2_3X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).out.hasId(v2Id, v3Id)", "v1Id", v1Id, "v2Id", v2Id, "v3Id", v3Id
        /// </summary>
        [TestMethod]
        public void HasVIdOutHasVIds()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string id1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string id2 = this.ConvertToVertexId(GraphViewCommand, "vadas");
                string id3 = this.ConvertToVertexId(GraphViewCommand, "lop");

                var traversal = GraphViewCommand.g().V().HasId(id1).Out().HasId(id2, id3);

                Assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
            }
        }

        /// <summary>
        /// Port of the g_V_hasXblahX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('blah')"
        /// </summary>
        [TestMethod]
        public void HasBlah()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("blah");

                var result = traversal.Next();
                //Assert.IsNull(result);
                Assert.AreEqual(0, result.Count);
            }
        }

        /// <summary>
        /// Port of the g_EX7X_hasXlabelXknowsX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.E(e7Id).hasLabel('knows')", "e7Id", e7Id
        /// </summary>
        [TestMethod]
        public void EdgesHasEIdHasLabelKnows()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string edgeId = this.ConvertToEdgeId(GraphViewCommand, "marko", "knows", "vadas");

                var traversal = GraphViewCommand.g().E().HasId(edgeId).HasLabel("knows");
                //var traversal = GraphViewCommand.g().E().Has("_edgeId", edgeId).HasLabel("knows");

                var result = traversal.Label().Next();
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("knows", result.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_E_hasXlabelXknowsX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.E.hasLabel('knows')"
        /// </summary>
        [TestMethod]
        public void EdgesHasLabelKnows()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().E().HasLabel("knows");

                var result = traversal.Label().Next();
                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.AreEqual("knows", res);
                }
            }
        }

        // 4 UTs from the gremlin-test HasTest.java class made use of the CREW graph data set.
        // But since our implementation does not support properties as documents of their own, we have not added those tests.

        /// <summary>
        /// Port of the g_V_hasXperson_name_markoX_age() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.has('person', 'name', 'marko').age"
        /// </summary>
        [TestMethod]
        public void HasPersonNameMarkoAge()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Has("person", "name", "marko").Values("age");

                var result = traversal.Next();
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(29, int.Parse(result.FirstOrDefault()));
            }
        }

        /// <summary>
        /// Port of the g_VX1X_outE_hasXweight_inside_0_06X_inV() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).outE.has('weight', inside(0.0d, 0.6d)).inV", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// Two problems here:
        /// 1: \Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\Predicate.cs doesn't support double.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36522
        /// 2: \DocDBEnlist\Development\Euler\Product\Microsoft.Azure.Graph\GraphView\GremlinTranslation2\SqlUtil.cs does not implement Predicate.inside.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36523
        /// </remarks>
        [TestMethod]
        public void HasVIdOutEHasWeightInside0dot0d0dot6dInV()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                var traversal = GraphViewCommand.g().V(vertexId).OutE()
                .Has("weight", Predicate.inside(0.0d, 0.6d)).InV();

                var result = traversal.Values("name").Next();
                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.IsTrue(string.Equals(res, "vadas") || string.Equals(res, "lop"));
                }
            }
        }

        /// <summary>
        /// Port of the g_EX11X_outV_outE_hasXid_10X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.E(e11Id).outV.outE.has(T.id, e8Id)", "e11Id", e11Id, "e8Id", e8Id
        /// </summary>
        /// <remarks>
        /// Id() for Edges doesn't exist.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36520
        /// </remarks>
        [TestMethod]
        public void EdgesHasEIdOutVOutEHasEId()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string edgeId1 = this.ConvertToEdgeId(GraphViewCommand, "josh", "created", "lop");
                string edgeId2 = this.ConvertToEdgeId(GraphViewCommand, "josh", "created", "ripple");

                var traversal = GraphViewCommand.g().E().HasId(edgeId1).OutV().OutE().HasId(edgeId2);
                //var traversal = GraphViewCommand.g().E().Has("_edgeId", edgeId1).OutV().OutE().Has("_edgeId", edgeId2);

                var result = traversal.Id().Next();
                //var result = traversal.Values("_edgeId").Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(edgeId2, result.FirstOrDefault());
            }
        }

        // gremlin-test's HasTest.java contains a UT called g_V_hasId_compilationEquality, which basically aggregates and compares the rests of the first few test methods in this class.
        // I'm currently not porting this method, and will return to it later.

        /// <summary>
        /// Port of the g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('person').has('age', P.not(lte(10).and(P.not(between(11,20)))).and(lt(29).or(eq(35)))).name"
        /// </summary>
        /// <remarks>
        /// Three problems here:
        /// 1: Predicate does not support "Not".
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36525
        /// 2: Predicate does not support "Or".
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36526
        /// 3: Predicate does not support "lte", this is particularly odd since lt, gt, and gte are supported.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36527
        /// </remarks>
        [TestMethod]
        public void HasLabelPersonHasAgeNotLTE10AndNotBetween11n20ANDLT29OrEQ35()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().HasLabel("person")
                    .Has("age",
                        Predicate.not(
                            Predicate.lte(10)
                            .And(Predicate.not(Predicate.between(11, 20))))
                        .And(Predicate.lt(29)
                        .Or(Predicate.eq(35))))
                        .Values("name");

                var result = traversal.Next();
                Assert.IsTrue(result.Contains("peter") && result.Contains("vadas"));
            }
        }

        /// <summary>
        /// Port of the g_V_in_hasIdXneqX1XX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V.hasLabel('person').has('age', P.not(lte(10).and(P.not(between(11,20)))).and(lt(29).or(eq(35)))).name"
        /// </summary>
        /// <remarks>
        /// HasId does not support passing a Predicate as param, so we're using Has("id", Predicate). We might need to fix this?
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36528
        /// </remarks>
        [TestMethod]
        public void InHasIdNEQVId()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                var traversal = GraphViewCommand.g().V().In().Has("id", Predicate.neq(vertexId));

                var result = traversal.Values("name").Next();
                Assert.AreEqual(3, result.Count);
                Assert.IsTrue(result.Contains("josh") && result.Contains("peter"));
            }
        }

        private void Assert_g_VX1X_out_hasXid_2_3X(string id2, string id3, GraphTraversal2 traversal)
        {
            var result = traversal.Id().Next();
            Assert.IsTrue(result.Contains(id2) || result.Contains(id3));
        }

        private void AssertVadasAsOnlyValueReturned(GraphViewCommand GraphViewCommand, GraphTraversal2 traversal)
        {
            var results = traversal.Id().Next();
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(this.ConvertToVertexId(GraphViewCommand, "vadas"), results.FirstOrDefault());
        }
    }
}
