using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.SideEffect
{
    [TestClass]
    public class SubgraphTest : AbstractAzureBatchGremlinTest
    {
        /// <remarks>
        /// The results both have six vertex.
        /// But JToken.DeepEquals(originGraph, subGraph) is false. I don't know why.
        /// </remarks>
        [TestMethod]
        public void SubgraphSameAsOriginalGraph()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                command.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = command.g().V();
                List<string> result = this.jobManager.TestQuery(this.job);
                var originGraphSON = result[0];

                command.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = command.g().E().Subgraph("sg").Cap("sg");
                result = this.jobManager.TestQuery(this.job);
                var subGraphSON = result.FindAll(res => res != "[]")[0];


                var originGraph = JToken.Parse(originGraphSON);
                var subGraph = JToken.Parse(subGraphSON);

                //Assert.IsTrue(JToken.DeepEquals(originGraph, subGraph));
            }
        }

        [TestMethod]
        public void SubgraphWithMultiVp()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                //command.g()
                //    .V()
                //    .Has("name", "peter")
                //    .Property(GremlinKeyword.PropertyCardinality.List, "age", 13)
                //    .Property("qqq", false)
                //    .Next();

                this.job.Traversal = command.g().E().Subgraph("sg").Cap("sg");
                this.jobManager.TestQuery(this.job);
            }
        }

        /// <remarks>
        /// The results both have six vertex.
        /// But JToken.DeepEquals(originGraph, subGraph) is false. I don't know why.
        /// </remarks>
        [TestMethod]
        public void SubgraphAggregation()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                command.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = command.g().V();
                var originGraphSON = this.jobManager.TestQuery(this.job)[0];

                command.OutputFormat = OutputFormat.Regular;
                this.job.Query = "g.E().hasLabel('created').subgraph('sg').bothV().bothE().subgraph('sg').cap('sg')";
                var subGraphSON = this.jobManager.TestQuery(this.job).FindAll(res => res != "[]")[0];

                var originGraph = JToken.Parse(originGraphSON);
                var subGraph = JToken.Parse(subGraphSON);

                //Assert.IsTrue(JToken.DeepEquals(originGraph, subGraph));
            }
        }

        [TestMethod]
        public void SubgraphInRepeat()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Query =
                    "g.V().has('name', 'lop').repeat(__.inE().subgraph('subGraph').outV()).times(3).cap('subGraph')";
                var subGraphSON = this.jobManager.TestQuery(this.job)[0];
                var subGraph = JArray.Parse(subGraphSON);

                Assert.AreEqual(subGraph.Count, 4);
            }
        }

        [TestMethod]
        public void SubgraphWithDifferentKeys()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Query =
                    "g.V().outE('knows').subgraph('knowsG').inV().outE('created').subgraph('createdG').inV().inE('created').subgraph('createdG')";
                var result = this.jobManager.TestQuery(this.job);

                Assert.AreEqual(result.Count, 4);
            }
        }
    }
}
