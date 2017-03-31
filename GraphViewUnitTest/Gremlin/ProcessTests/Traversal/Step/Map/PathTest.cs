using System.Collections.Generic;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class PathTest : AbstractGremlinTest
    {
        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V(v1Id).values("name").path();
        /// </summary>
        [TestMethod]
        public void get_g_VX1X_name_path()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.Regular;
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                GraphTraversal2 traversal = graphCommand.g().V(vertexId1).Values("name").Path();
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault())[0];

                Assert.AreEqual(vertexId1, (string)result["objects"][0].id);
                Assert.AreEqual("marko", (string)result["objects"][1]);
            }
        }

        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V(v1Id).out().path().by("age").by("name");
        /// </summary>
        [TestMethod]
        public void get_g_VX1X_out_path_byXageX_byXnameX()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.Regular;
                string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

                graphCommand.OutputFormat = OutputFormat.GraphSON;
                GraphTraversal2 traversal = graphCommand.g().V(vertexId1).Out().Path().By("age").By("name");
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(3, results.Count);
                List<string> expected = new List<string>();
                foreach (var result in results)
                {
                    expected.Add((string)result["objects"][1]);
                }
                CheckUnOrderedResults(new List<string>() {"lop", "vadas", "josh"}, expected);
            }
        }

        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V().repeat(__.out()).times(2).path().by().by("name").by("lang");
        /// </summary>
        [TestMethod]
        public void get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var vertex = this.getVertexString(graphCommand, "marko");
                
                GraphTraversal2 traversal =
                    graphCommand.g().V().Repeat(GraphTraversal2.__().Out()).Times(2).Path().By().By("name").By("lang");
                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(2, results.Count);
                List<string> actualList = new List<string>();
                foreach (var result in results[0]["objects"])
                {
                    actualList.Add(result.ToString());
                } 
                CheckPathResults(new List<string> { vertex, "josh", "java"}, actualList);

                actualList.Clear();
                foreach (var result in results[1]["objects"])
                {
                    actualList.Add(result.ToString());
                }
                CheckPathResults(new List<string> { vertex, "josh", "java" }, actualList);
            }
        }

        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V().out().out().path().by("name").by("age")
        /// </summary>
        [TestMethod]
        public void get_g_V_out_out_path_byXnameX_byXageX()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;

                GraphTraversal2 traversal =
                    graphCommand.g().V().Out().Out().Path().By("name").By("age");

                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(2, results.Count);

                int counter = 0;
                foreach (dynamic result in results)
                {
                    List<object> actualList = new List<object>();
                    actualList.Add((string)result["objects"][0]);
                    actualList.Add((int)result["objects"][1]);
                    actualList.Add((string)result["objects"][2]);

                    if (actualList.Last().Equals("ripple")) {
                        CheckPathResults(new List<object> { "marko", 32, "ripple" }, actualList);
                        counter++;
                    }
                    else {
                        CheckPathResults(new List<object> { "marko", 32, "lop" }, actualList);
                        counter++;
                    }
                }

                Assert.AreEqual(2, counter);
            }
        }

        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path()
        /// </summary>
        [TestMethod]
        public void get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                var vertex = this.getVertexString(graphCommand, "marko");

                GraphTraversal2 traversal =
                    graphCommand.g().V().As("a").Has("name", "marko").As("b").Has("age", 29).As("c").Path();

                dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(1, results[0]["objects"].Count);
                Assert.AreEqual(vertex, results[0]["objects"][0].ToString());
            }
        }

        /// <summary>
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/PathTest.java
        /// Gremlin: g.V(v1Id).outE("created").inV().inE().outV().path()
        /// </summary>
        //[TestMethod]
        //public void get_g_VX1X_outEXcreatedX_inV_inE_outV_path()
        //{
        //    using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
        //    {
        //        string vertexId1 = this.ConvertToVertexId(graphCommand, "marko");

        //        GraphTraversal2 traversal = graphCommand.g().V(vertexId1).OutE("created").InV().InE().OutV().Path();

        //        List<string> temp = graphCommand.g().V(vertexId1).OutE("created").InV().InE().OutV().Path().Next();

        //        dynamic results = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
        //    }
        //}
    }
}
