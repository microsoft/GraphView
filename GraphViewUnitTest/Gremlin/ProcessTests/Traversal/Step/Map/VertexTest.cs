using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Newtonsoft.Json;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    /// <summary>
    /// Test for Vertex step, which is g.V().
    /// </summary>
    [TestClass]
    public class VertexTest : AbstractGremlinTest
    {
       /// <summary>
        /// g_VXlistX1_2_3XX_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(Arrays.asList(v1Id, v2Id, v3Id)).values("name");
        /// </summary>
        /// <remarks>
        /// V(id1, id2...) doesn't work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36517
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetVertexByIdList()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V(id1, id2...) doesn't work

                var expectedNames = new[] { "marko", "vadas", "lop" };
                var traversal = graphCommand.g()
                    .V(expectedNames.Select(n => this.ConvertToVertexId(graphCommand, n)).ToArray<object>())
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(expectedNames, result);
            }
        }

        // Gremlin test g_VXlistXv1_v2_v3XX_name() is skipped since we don't have vertex model.

        /// <summary>
        /// g_V()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetAllVertexes()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().V();
                var result = traversal.Next();

                Assert.AreEqual(6, result.Count);
            }
        }

        /// <summary>
        /// g_VX1X_out()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).out();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutVertexes()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .Out()
                    .Values("name");
                var result = traversal.Next();

                AssertMarkoOut(result);
            }
        }

        /// <summary>
        /// get_g_VX2X_in()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v2Id).in();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetInVertexes()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "vadas"))
                    .In()
                    .Values("name");
                var result = traversal.Next();

                AssertVadasIn(result);
            }
        }


        /// <summary>
        /// g_VX4X_both()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v4Id).both();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetBothVertexes()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "josh"))
                    .Both()
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "marko", "ripple", "lop" }, result);
            }
        }

        /// <summary>
        /// g_E()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.E();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetAllEdges()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g().E();
                var result = traversal.Next();

                Assert.AreEqual(6, result.Count);
            }
        }

        /// <summary>
        /// g_EX11X()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.E(e11Id);
        /// </summary>
        /// <remarks>
        /// E(Id), E().HasId() and E().Id() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36520
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetEdgeById()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // E(Id), E().HasId() and E().Id() does not work

                var expectedEdgeId = this.ConvertToEdgeId(graphCommand, "josh", "created", "lop");
                var traversal = graphCommand.g().E(expectedEdgeId).Id();
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(expectedEdgeId, result.First());
            }
        }

        /// <summary>
        /// g_VX1X_outE()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).outE();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutEdges()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .OutE()
                    .Label();
                var result = traversal.Next();

                Assert.AreEqual(3, result.Count);
                CheckUnOrderedResults(new[] { "knows", "knows", "created" }, result);
            }
        }

        /// <summary>
        /// g_VX2X_inE()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v2Id).inE();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetInEdges()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "vadas"))
                    .InE()
                    .Label();
                var result = traversal.Next();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("knows", result.First());
            }
        }

        /// <summary>
        /// g_VX4X_bothEXcreatedX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v4Id).bothE("created");
        /// </summary>
        /// <remarks>
        /// V().BothE() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37888
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetBothEdgesFiltedByLabel()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().BothE() does not work

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var expectedId = this.ConvertToVertexId(graphCommand, "josh");
                var traversal = graphCommand.g()
                    .V(expectedId)
                    .BothE()
                    .Label();
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                foreach (dynamic edge in result)
                {
                    Assert.AreEqual("created", (string)edge.label);
                    Assert.AreEqual(expectedId, (string)edge.outV);
                }

                Assert.AreEqual(2, result.Count);
            }
        }

        /// <summary>
        /// g_VX4X_bothE()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v4Id).bothE();
        /// </summary>
        /// <remarks>
        /// V().BothE() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37888
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetBothEdges()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().BothE() does not work

                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "josh"))
                    .BothE()
                    .Label();
                var result = traversal.Next();

                Assert.AreEqual(3, result.Count);
                CheckUnOrderedResults(new[] { "knows", "created", "created" }, result);
            }
        }

        /// <summary>
        /// g_VX1X_outE_inV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).outE().inV();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutEdgeInVertex()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .OutE()
                    .InV()
                    .Values("name");
                var result = traversal.Next();

                AssertMarkoOut(result);
            }
        }


        /// <summary>
        /// g_VX2X_inE_outV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v2Id).inE().outV();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetInEdgeOutVertex()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "vadas"))
                    .InE()
                    .OutV()
                    .Values("name");
                var result = traversal.Next();

                AssertVadasIn(result);
            }
        }

        /// <summary>
        /// g_V_outE_hasXweight_1X_outV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V().outE().has("weight", 1.0d).outV();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutEdgeOutVertexFilteredByProperty()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V()
                    .OutE()
                    .Has("weight", 1.0d)
                    .OutV()
                    .Values("name");
                var result = traversal.Next();

                Assert.AreEqual(2, result.Count);
                CheckUnOrderedResults(new[] { "marko", "josh" }, result);
            }
        }

        /// <summary>
        /// g_V_out_outE_inV_inE_inV_both_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V().out().outE().inV().inE().inV().both().values("name");
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void CombinationOfInOutBoth()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var expected = Enumerable.Repeat("josh", 4).Concat(
                    Enumerable.Repeat("marko", 3)).Concat(
                    Enumerable.Repeat("peter", 3));

                var traversal = graphCommand.g()
                    .V()
                    .Out()
                    .OutE()
                    .InV()
                    .InE()
                    .InV()
                    .Both()
                    .Values("name");
                var result = traversal.Next();

                Assert.AreEqual(10, result.Count);
                CheckUnOrderedResults(expected, result);
            }
        }

        /// <summary>
        /// g_VX1X_outEXknowsX_bothV_name()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).outE("knows").bothV().values("name");
        /// </summary>
        /// <remarks>
        /// V().BothV() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37909
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutEdgeBothVertex()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().BothV() does not work

                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .OutE("knows")
                    .BothV()
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "marko", "marko", "josh", "vadas" }, result);
            }
        }

        /// <summary>
        /// g_VX1X_outE_otherV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).outE().otherV();
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutEdgeOtherVertex()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .OutE()
                    .OtherV()
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "josh", "vadas", "lop" }, result);
            }
        }

        /// <summary>
        /// g_VX4X_bothE_otherV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v4Id).bothE().otherV();
        /// </summary>
        /// <remarks>
        /// V().BothE() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37888
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetBothEdgeOtherVertex()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().BothE() does not work
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "josh"))
                    .BothE()
                    .OtherV()
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "marko", "ripple", "lop" }, result);
            }
        }

        /// <summary>
        /// g_VX4X_bothE_hasXweight_lt_1X_otherV()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v4Id).bothE().has("weight", P.lt(1d)).otherV();
        /// </summary>
        /// <remarks>
        /// V().BothE() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37888
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void GetBothEdgeOtherVertexFilteredByProperty()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().BothE() does not work

                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "josh"))
                    .BothE()
                    .Has("weight", Predicate.lt(1d))
                    .OtherV()
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "lop" }, result);
            }
        }

        // Gremlin test g_VX1X_outXknowsX() is skipped because in our implementation vertex Id cannot be parsed as int.

        /// <summary>
        /// g_VX1X_outXknowsAsStringIdX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(v1Id).out("knows");
        /// </summary>
        [Owner("xunsun")]
        [TestMethod]
        public void GetOutVertexFilteredByEdgeLabel()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = graphCommand.g()
                    .V(this.ConvertToVertexId(graphCommand, "marko"))
                    .Out("knows")
                    .Values("name");
                var result = traversal.Next();

                var expected = new[] { "vadas", "josh" };
                CheckUnOrderedResults(expected, result);
            }
        }

        /// <summary>
        /// Original test
        /// Gremlin: g.V().hasId(Arrays.asList(v1Id, v2Id, v3Id)).values("name");
        /// </summary>
        /// <remarks>
        /// V().HasId(id1, id2) does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37913
        /// </remarks>
        [Owner("xunsun")]
        [TestMethod]
        public void VertexHasIdByIdList()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: V().HasId(id1, id2) does not work

                var expectedNames = new[] { "marko", "vadas", "lop" };
                var traversal = graphCommand.g()
                    .V()
                    .HasId(expectedNames.Select(n => this.ConvertToVertexId(graphCommand, n)).ToArray<object>())
                    .Values("name");
                var result = traversal.Next();

                CheckUnOrderedResults(expectedNames, result);
            }
        }

        /// <summary>
        /// Original test
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V(new Object());
        /// </summary>
        /// <remarks>
        /// V(id1, id2...) doesn't work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36517
        /// </remarks>
        [ExpectedException(typeof(ArgumentException))]
        [Owner("xunsun")]
        [TestMethod]
        public void GetVertexByInvalidId()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var result = graphCommand.g().V(new object()).Next();
            }
        }


        /// <summary>
        /// Original test
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/VertexTest.java
        /// Gremlin: g.V().hasId(new Object());
        /// </summary>
        /// <remarks>
        /// V().HasId(id1, id2) does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37913
        /// </remarks>
        [ExpectedException(typeof(ArgumentException))]
        [Owner("xunsun")]
        [TestMethod]
        public void GetVertexHasIdByInvalidId()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                var result = graphCommand.g().V().HasId(new object()).Next();
            }
        }

        private static void AssertMarkoOut(List<string> result)
        {
            var expected = new[] { "vadas", "josh", "lop" };
            CheckUnOrderedResults(expected, result);
        }


        private static void AssertVadasIn(List<string> result)
        {
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("marko", result.First());
        }
    }
}