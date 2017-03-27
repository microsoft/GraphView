using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Map
{
    [TestClass]
    public class GraphSONTest : AbstractGremlinTest
    {
        /// <summary>
        /// Creates a vertex with properties
        /// </summary>
        [TestMethod]
        public async Task AddVertexTest()
        {
            GraphViewCommand cmd = new GraphViewCommand(graphConnection);
            cmd.OutputFormat = OutputFormat.GraphSON;
            cmd.CommandText = "g.AddV('character').Property('name', 'VENUS II').Property('weapon', 'shield')";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }

            graphConnection.ResetCollection();
        }
    }
}
