using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Filter
{
    [TestClass]
    public sealed class HasTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// Port of the g_VX1X_hasXkeyX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).has(k)", "v1Id", v1Id, "k", key
        /// </summary>
        [TestMethod]
        public void HasVIdHasName()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                this.job.Traversal = GraphViewCommand.g().V().HasId(vertexId).Has("name").Values("name");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual("marko", results.FirstOrDefault());
            }
        }

        /// <summary>
        /// Port of the g_VX1X_hasXname_markoX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        /// Equivalent gremlin: "g.V(v1Id).has('name', 'marko')", "v1Id", v1Id
        /// </summary>
        [TestMethod]
        public void HasVIdHasNameMarko()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                this.job.Traversal = GraphViewCommand.g().V().HasId(vertexId).Has("name", "marko").Values("name");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual("marko", results.FirstOrDefault());
            }
        }

        ///// <summary>
        ///// Port of the g_V_hasXname_markoX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('name', 'marko')"
        ///// </summary>
        [TestMethod]
        public void HasNameMarko()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("name", "marko").Values("name");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(1, results.Count);
                Assert.AreEqual("marko", results.FirstOrDefault());
            }
        }

        ///// <summary>
        ///// Port of the g_V_hasXname_blahX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('name', 'blah')"
        ///// </summary>
        [TestMethod]
        public void HasNameBlah()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("name", "blah");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.IsNull(results);
            }
        }

        ///// <summary>
        ///// Port of the g_V_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('age',gt(30))"
        ///// </summary>
        [TestMethod]
        public void HasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("age", Predicate.gt(30)).Values("age");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(2, results.Count);
                foreach (var age in results)
                {
                    Assert.IsTrue(int.Parse(age) > 30);
                }
            }
        }

        ///// <summary>
        ///// Port of the g_V_hasXage_isXgt_30XX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('age', __.is(gt(30)))"
        ///// </summary>
        [TestMethod]
        public void HasAgeIsGT30()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("age", GraphTraversal.__().Is(Predicate.gt(30))).Values("age");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Console.WriteLine("-------------Test Result-------------");
                foreach (string res in results)
                {
                    Console.WriteLine(res);
                }
                Assert.AreEqual(2, results.Count);
                foreach (var age in results)
                {
                    Assert.IsTrue(int.Parse(age) > 30);
                }
            }
        }

        ///// <summary>
        ///// Port of the g_VX1X_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V(v1Id).has('age',gt(30))", "v1Id", v1Id
        ///// </summary>
        [TestMethod]
        public void HasVIdHasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "josh");

                this.job.Traversal = GraphViewCommand.g().V().HasId(vertexId1).Has("age", Predicate.gt(30));
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(0, results.Count);

                this.job.Traversal = GraphViewCommand.g().V().HasId(vertexId2).Has("age", Predicate.gt(30));
                results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(1, results.Count);
            }
        }

        ///// <summary>
        ///// Port of the g_VXv1X_hasXage_gt_30X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V(g.V(v1Id).next()).has('age',gt(30))", "v1Id", v1Id
        ///// </summary>
        [TestMethod]
        public void HasIdTraversalHasVIdHasAgeGT30()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "josh");

                this.job.Traversal = GraphViewCommand.g().V(vertexId1)
                    .Has("age", Predicate.gt(30));
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(0, results.Count);

                this.job.Traversal = GraphViewCommand.g().V(vertexId2)
                    .Has("age", Predicate.gt(30));
                results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(1, results.Count);
            }
        }

        ///// <summary>
        ///// Port of the g_VX1X_out_hasXid_2X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V(v1Id).out.hasId(v2Id)", "v1Id", v1Id, "v2Id", v2Id
        ///// </summary>
        [TestMethod]
        public void HasVIdOutHasVId()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string markoVertexId = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vadasVertexId = this.ConvertToVertexId(GraphViewCommand, "vadas");

                this.job.Traversal = GraphViewCommand.g().V().HasId(markoVertexId)
                    .Out().HasId(vadasVertexId).Id();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.IsTrue(results.Count == 1);
                Assert.IsTrue(vadasVertexId == results.FirstOrDefault());
            }
        }

        ///// <summary>
        ///// Port of the g_VX1X_out_hasXid_2_3X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V(v1Id).out.hasId(v2Id, v3Id)", "v1Id", v1Id, "v2Id", v2Id, "v3Id", v3Id
        ///// </summary>
        [TestMethod]
        public void HasVIdOutHasVIds()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string id1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string id2 = this.ConvertToVertexId(GraphViewCommand, "vadas");
                string id3 = this.ConvertToVertexId(GraphViewCommand, "lop");

                this.job.Traversal = GraphViewCommand.g().V().HasId(id1).Out().HasId(id2, id3).Id();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.IsTrue(results.Count == 2);
                CheckUnOrderedResults(new[] {id2, id3}, results);
            }
        }

        ///// <summary>
        ///// Port of the g_V_hasXblahX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('blah')"
        ///// </summary>
        [TestMethod]
        public void HasBlah()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("blah");
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(0, results.Count);
            }
        }

        ///// <summary>
        ///// Port of the g_EX7X_hasXlabelXknowsX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.E(e7Id).hasLabel('knows')", "e7Id", e7Id
        ///// </summary>
        [TestMethod]
        public void EdgesHasEIdHasLabelKnows()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string edgeId = this.ConvertToEdgeId(GraphViewCommand, "marko", "knows", "vadas");

                this.job.Traversal = GraphViewCommand.g().E().HasId(edgeId).HasLabel("knows").Label();
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("knows", result.FirstOrDefault());
            }
        }

        ///// <summary>
        ///// Port of the g_E_hasXlabelXknowsX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.E.hasLabel('knows')"
        ///// </summary>
        [TestMethod]
        public void EdgesHasLabelKnows()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().E().HasLabel("knows").Label();
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.AreEqual("knows", res);
                }
            }
        }

        //// 4 UTs from the gremlin-test HasTest.java class made use of the CREW graph data set.
        //// But since our implementation does not support properties as documents of their own, we have not added those tests.

        ///// <summary>
        ///// Port of the g_V_hasXperson_name_markoX_age() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.has('person', 'name', 'marko').age"
        ///// </summary>
        [TestMethod]
        public void HasPersonNameMarkoAge()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Has("person", "name", "marko").Values("age");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(29, int.Parse(result.FirstOrDefault()));
            }
        }

        ///// <summary>
        ///// Port of the g_VX1X_outE_hasXweight_inside_0_06X_inV() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V(v1Id).outE.has('weight', inside(0.0d, 0.6d)).inV", "v1Id", v1Id
        ///// </summary>
        [TestMethod]
        public void HasVIdOutEHasWeightInside0dot0d0dot6dInV()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                this.job.Traversal = GraphViewCommand.g().V(vertexId).OutE()
                .Has("weight", Predicate.inside(0.0d, 0.6d)).InV().Values("name");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(2, result.Count);
                foreach (var res in result)
                {
                    Assert.IsTrue(string.Equals(res, "vadas") || string.Equals(res, "lop"));
                }
            }
        }

        ///// <summary>
        ///// Port of the g_EX11X_outV_outE_hasXid_10X() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.E(e11Id).outV.outE.has(T.id, e8Id)", "e11Id", e11Id, "e8Id", e8Id
        ///// </summary>
        [TestMethod]
        public void EdgesHasEIdOutVOutEHasEId()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string edgeId1 = this.ConvertToEdgeId(GraphViewCommand, "josh", "created", "lop");
                string edgeId2 = this.ConvertToEdgeId(GraphViewCommand, "josh", "created", "ripple");

                this.job.Traversal = GraphViewCommand.g().E().HasId(edgeId1).OutV().OutE().HasId(edgeId2);
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(edgeId2, result.FirstOrDefault());
            }
        }

        //// gremlin-test's HasTest.java contains a UT called g_V_hasId_compilationEquality, which basically aggregates and compares the rests of the first few test methods in this class.
        //// I'm currently not porting this method, and will return to it later.

        ///// <summary>
        ///// Port of the g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.hasLabel('person').has('age', P.not(lte(10).and(P.not(between(11,20)))).and(lt(29).or(eq(35)))).name"
        ///// </summary>
        [TestMethod]
        public void HasLabelPersonHasAgeNotLTE10AndNotBetween11n20ANDLT29OrEQ35()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().HasLabel("person")
                    .Has("age",
                        Predicate.not(
                            Predicate.lte(10)
                            .And(Predicate.not(Predicate.between(11, 20))))
                        .And(Predicate.lt(29)
                        .Or(Predicate.eq(35))))
                        .Values("name");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.IsTrue(result.Contains("peter") && result.Contains("vadas"));
            }
        }

        ///// <summary>
        ///// Port of the g_V_in_hasIdXneqX1XX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/filter/HasTest.java.
        ///// Equivalent gremlin: "g.V.hasLabel('person').has('age', P.not(lte(10).and(P.not(between(11,20)))).and(lt(29).or(eq(35)))).name"
        ///// </summary>
        [TestMethod]
        public void InHasIdNEQVId()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                this.job.Traversal = GraphViewCommand.g().V().In().Has("id", Predicate.neq(vertexId)).Values("name");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(job);
                Assert.AreEqual(3, result.Count);
                Assert.IsTrue(result.Contains("josh") && result.Contains("peter"));
            }
        }
    }
}
