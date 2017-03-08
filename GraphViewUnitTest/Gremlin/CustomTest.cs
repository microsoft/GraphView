using System;
using System.Collections.Generic;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace GraphViewUnitTest.Gremlin.ProcessTests
{
    [TestClass]
    public class CustomTest : AbstractGremlinTest
    {
        [TestMethod]
        public void AddVWithNestingProperties()
        {
            using (GraphViewCommand command = new GraphViewCommand(graphConnection))
            {
                var traversal = command.g()
                    .V().As("a").Select("a").As("b").Select("b").Values("name");
                var result = traversal.Next();
            }
        }
    }
}
