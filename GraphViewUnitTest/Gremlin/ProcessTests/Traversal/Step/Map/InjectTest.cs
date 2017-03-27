using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class InjectTest : AbstractGremlinTest
    {
        /// <summary>
        /// Original test
        /// Gremlin: g.V().has("name","marko").values("name").inject("daniel");
        /// </summary>
        /// <remarks>
        /// Inject() does not work
        /// https://msdata.visualstudio.com/DocumentDB/_workitems/edit/37567
        /// </remarks>
        [TestMethod]
        public void BasicInject()
        {
            using (GraphViewCommand graphCommand = new GraphViewCommand(graphConnection))
            {
                // TODO: Inject() does not work

                var traversal = graphCommand.g().V()
                    .Has("name", "marko")
                    .Values("name")
                    .Inject("daniel");
                var result = traversal.Next();

                CheckUnOrderedResults(new[] { "marko", "daniel" }, result);
            }
        }
    }
}
