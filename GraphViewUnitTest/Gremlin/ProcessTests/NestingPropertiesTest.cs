using System;
using System.Collections.Generic;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin.ProcessTests
{
    [TestClass]
    public class NestingPropertiesTest : AbstractGremlinTest
    {
        [TestMethod]
        public void AddVWithNestingProperties()
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
    }
}
