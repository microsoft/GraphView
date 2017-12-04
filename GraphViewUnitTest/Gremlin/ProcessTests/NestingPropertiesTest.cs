using System;
using System.Collections.Generic;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests
{
    [TestClass]
    public class MultiPropertiesAndMetaPropertiesTest : AbstractGremlinTest
    {
        [TestMethod]
        [TestModernCompatible]
        public void AddVWithMultiPropertiesAndMetaProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.OutputFormat = OutputFormat.GraphSON;
                command.g()
                    .AddV()
                    .Property(TEST_PARTITION_BY_KEY, "====")
                    .Property("name233", "marko", "meta1", "meta1Value")
                    .Property("name233", "mike", "meta2", "meta2Value")
                    .Next();
                var traversal = command.g().V().Properties("name233").Properties();
                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(2, (int)result.Count);
                Assert.AreEqual("meta1", (string)result[0].key);
                Assert.AreEqual("meta1Value", (string)result[0].value);
                Assert.AreEqual("meta2", (string)result[1].key);
                Assert.AreEqual("meta2Value", (string)result[1].value);

                traversal = command.g().V().Properties("name233").Has("meta1", "meta1Value");
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual("name233", (string)result[0].label);
                Assert.AreEqual("marko", (string)result[0].value);

                if (graphConnection.GraphType == GraphType.GraphAPIOnly)
                {
                    command.g().V().Properties().Properties().Drop().Next();
                    traversal = command.g().V().Properties("name233").Properties();
                    result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                    Assert.AreEqual(0, result.Count);
                }
            }
        }

        [TestMethod]
        [TestModernCompatible]
        public void DropMixPropertiesTest()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                string id = ConvertToVertexId(command, "marko");

                command.OutputFormat = OutputFormat.GraphSON;
                var traversal = command.g().V(id).Union(GraphTraversal.__().Out(), GraphTraversal.__().OutE()).Properties();
                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                if (graphConnection.GraphType != GraphType.GraphAPIOnly) {
                    Assert.AreEqual(6, (int)result.Count);
                }
                else {
                    Assert.AreEqual(9, (int)result.Count);
                }

                if (graphConnection.GraphType == GraphType.GraphAPIOnly)
                {
                    command.g().V(id).Union(GraphTraversal.__().Out(), GraphTraversal.__().OutE()).Properties().Drop().Next();

                    traversal = command.g().V(id).Union(GraphTraversal.__().Out(), GraphTraversal.__().OutE()).Properties();
                    result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                    Assert.AreEqual(0, (int)result.Count);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// Add id of properties in ToGraphSon format
        /// </remarks>
#if TEST_PARTITION_BY_NAME
        [ExpectedException(typeof(GraphViewException), "Partition value must not have meta properties")]
#endif
        [TestMethod]
        public void DropMultiPropertiesAndMetaPropertiesTest()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.OutputFormat = OutputFormat.GraphSON;

                command.g().V().Drop().Next();

                command.g()
                    .AddV("name", "marko")
                    .Property("name", "marko2", "meta1", 1, "meta2", false)
                    .Next();

                var traversal = command.g().V().Properties();
                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(1, (int)result.Count);
                Assert.AreEqual("marko2", (string)result[0].value);
                Assert.AreEqual("name", (string)result[0].label);
                Assert.AreEqual(1, ((JObject)result[0].properties)["meta1"].ToObject<int>());
                Assert.AreEqual(false, ((JObject)result[0].properties)["meta2"].ToObject<bool>());

                traversal = command.g().V().Properties().Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(2, (int)result.Count);

                CheckUnOrderedResults(
                    new []
                    {
                        JObject.Parse(@"{""key"": ""meta1"", ""value"": 1}").ToString(),
                        JObject.Parse(@"{""key"": ""meta2"", ""value"": false}").ToString()
                    }, 
                    new []
                    {
                        ((JObject)result[0]).ToString(),
                        ((JObject)result[1]).ToString()
                    });

                command.g().V().Property(GremlinKeyword.PropertyCardinality.List, "name", "marko3").Next();
                traversal = command.g().V().Property(GremlinKeyword.PropertyCardinality.List, "name", "marko4", "meta1", "metaStr1").Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(3, (int)result.Count);

                command.g()
                    .V()
                    .Properties()
                    .Has("meta1", 1)
                    .Property("meta2", true).Next();

                traversal = command.g()
                    .V()
                    .Properties()
                    .Has("meta1", 1)
                    .Property("meta3", "metaStr3")
                    .Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(3, (int)result.Count);
                CheckUnOrderedResults(
                    new[]
                    {
                        JObject.Parse(@"{""key"": ""meta1"", ""value"": 1}").ToString(),
                        JObject.Parse(@"{""key"": ""meta2"", ""value"": true}").ToString(),
                        JObject.Parse(@"{""key"": ""meta3"", ""value"": ""metaStr3""}").ToString()
                    },
                    new[]
                    {
                        ((JObject) result[0]).ToString(),
                        ((JObject) result[1]).ToString(),
                        ((JObject) result[2]).ToString()
                    });

                command.g().V().Properties().Has("meta1", 1).Properties().As("a").Value().Is(true).Select("a").Drop().Next();
                command.g().V().Properties().As("a").Value().Is("marko3").Select("a").Drop().Next();

                traversal = command.g().V().Properties("name");
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(2, result.Count);
                CheckUnOrderedResults(
                    new []
                    {
                        JObject.Parse(@"{""id"": """ + ConvertToPropertyId(command, "marko4", "name", "marko4") + @""", ""value"": ""marko4"",""label"": ""name"",""properties"": {""meta1"": ""metaStr1""}}").ToString(),
                        JObject.Parse(@"{""id"": """ + ConvertToPropertyId(command, "marko2", "name", "marko2") + @""", ""value"": ""marko2"",""label"": ""name"",""properties"": {""meta1"": 1,""meta3"": ""metaStr3""}}").ToString(), 
                    },
                    new []
                    {
                        ((JObject)result[0]).ToString(),
                        ((JObject)result[1]).ToString(),
                    });

                traversal = command.g().V().Property("name", "override").Properties("name");
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(1, result.Count);

                command.g().V().Properties().Drop().Next();

                traversal = command.g().V().Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(0, result.Count);
            }
        }

        [TestMethod]
        public void Test_AddV_With_MultipleProperty()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().Inject(1).AddV().Property("name", "testNode")
                    .Property("grade", "100")
                    .Property("grade", "60")
                    .Values("grade");

                var result = traversal.Next();
                Assert.IsTrue(result.Count == 2);
                Assert.IsTrue(result[0] == "100");
                Assert.IsTrue(result[1] == "60");
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }


        [TestMethod]
        public void Test_Vertex_Property_With_Subtraversal()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Has("name", "marko").Property("boolproperty", false);
                traversal.Next();

                traversal = command.g().V().Has("name", "marko")
                    .Property("p1", "v1")
                    .Property("outDegree", GraphTraversal.__().Out().Count())
                    .Property("p2", "v2", "meta1", GraphTraversal.__().V().Count(), "meta2", "meta2value")
                    .Property("p3", GraphTraversal.__().Values("label"), "meta3", GraphTraversal.__().V().OutE("knows").Count())
                    .Property("p4", GraphTraversal.__().Values("boolproperty"))
                    .Property("p5", GraphTraversal.__().OutE("created").Values("weight"))
                    .Union(
                        GraphTraversal.__().Properties("boolproperty").Drop(),
                        GraphTraversal.__().Properties().Value(),
                        GraphTraversal.__().Properties().HasKey("p2").Properties().Value(),
                        GraphTraversal.__().Properties().HasKey("p3").Properties().Value());

                var result = traversal.Next();
                Assert.IsTrue(result.Count == 11);
                Assert.IsTrue(result.Contains("3"));//outDegree
                Assert.IsTrue(result.Contains("6"));//meta1
                Assert.IsTrue(result.Contains("person"));//p3
                Assert.IsTrue(result.Contains("2"));//meta3
                Assert.IsTrue(result.Contains("False"));//p4
                Assert.IsTrue(result.Contains("0.4"));//p5

                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void Test_Edge_Property_With_Subtraversal()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g().V().Has("name", "marko").Property("bool", false);
                traversal.Next();

                traversal = command.g().V().Has("name", "marko").OutE("created")
                    .Property("p1", "v1")
                    .Property("inName", GraphTraversal.__().InV().Values("name"))
                    .Property("maxWeight", GraphTraversal.__().V().OutE().Values("weight").Max())
                    .Property("p2", GraphTraversal.__().OutV().Values("bool"))
                    .Properties().Value();

                var result = traversal.Next();
                Assert.IsTrue(result.Count == 5);
                Assert.IsTrue(result.Contains("v1"));//p1
                Assert.IsTrue(result.Contains("lop"));//inName
                Assert.IsTrue(result.Contains("1"));//maxWeight
                Assert.IsTrue(result.Contains("False"));//p2
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

    }
}
