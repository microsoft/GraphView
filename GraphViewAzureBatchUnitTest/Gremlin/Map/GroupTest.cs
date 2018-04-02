using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class GroupTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void g_V_Group()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().Group();
                List<string> results = this.jobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        [TestMethod]
        public void g_V_Group_by()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().Group().By(GraphTraversal.__().Values("name")).By();
                List<string> results = this.jobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// bug: 
        /// In Out() step, records will be sent and received. 
        /// But in the process of rawrecord deserialization in receiveOp, VertexField.GetHashCode() will be invoked.(Because the deserialization of MapField (Dictionary) )
        /// However, the vertexField has nothing but searchInfo.
        /// </remarks>
        [TestMethod]
        public void g_V_Group_by_select()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().As("a").In().Select("a").GroupCount().Unfold().Select(GremlinKeyword.Column.Keys).Out().ValueMap();
                List<string> results = this.jobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        [TestMethod]
        public void g_V_GroupCount()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V().GroupCount().Order(GremlinKeyword.Scope.Local).By(GremlinKeyword.Column.Values, GremlinKeyword.Order.Decr);
                List<string> result = this.jobManager.TestQuery(this.job);
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }

        [TestMethod]
        public void g_V_FlatMap_Out_Group()
        {
            using (GraphViewCommand GraphViewCommand = this.job.Command)
            {
                this.job.Traversal = GraphViewCommand.g().V().FlatMap(GraphTraversal.__().Out().Group());
                List<string> result = this.jobManager.TestQuery(this.job);
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }
    }
}
