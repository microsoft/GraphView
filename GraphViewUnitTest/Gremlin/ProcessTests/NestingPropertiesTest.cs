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
        public void AddVWithMultiPropertiesAndMetaProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                command.OutputFormat = OutputFormat.GraphSON;
                command.g()
                    .AddV()
                    .Property("name", "marko", "meta1", "meta1Value")
                    .Property("name", "mike", "meta2", "meta2Value")
                    .Next();
                var traversal = command.g().V().Properties("name").Properties();
                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(2, (int)result.Count);
                Assert.AreEqual("meta1", (string)result[0].key);
                Assert.AreEqual("meta1Value", (string)result[0].value);
                Assert.AreEqual("meta2", (string)result[1].key);
                Assert.AreEqual("meta2Value", (string)result[1].value);

                traversal = command.g().V().Properties("name").Has("meta1", "meta1Value");
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual("name", (string)result[0].label);
                Assert.AreEqual("marko", (string)result[0].value);

                command.g().V().Properties().Properties().Drop().Next();
                traversal = command.g().V().Properties("name").Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                Assert.AreEqual(0, result.Count);
            }
        }

        [TestMethod]
        public void DropMixPropertiesTest()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                string id = ConvertToVertexId(command, "marko");

                command.OutputFormat = OutputFormat.GraphSON;
                var traversal = command.g().V(id).Union(GraphTraversal2.__().Out(), GraphTraversal2.__().OutE()).Properties();
                var result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(9, (int)result.Count);

                command.g().V(id).Union(GraphTraversal2.__().Out(), GraphTraversal2.__().OutE()).Properties().Drop().Next();
                
                traversal = command.g().V(id).Union(GraphTraversal2.__().Out(), GraphTraversal2.__().OutE()).Properties();
                result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(0, (int)result.Count);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// Add id of properties in ToGraphSon format
        /// </remarks>
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
    }
}
