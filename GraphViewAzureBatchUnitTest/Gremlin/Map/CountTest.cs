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
    public class CountTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void G_V_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Count();
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] {"6"}, result);
            }
        }

        [TestMethod]
        public void G_V_Union_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Union(GraphTraversal.__().Count());
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "6" }, result);
            }
        }

        [TestMethod]
        public void G_V_Map_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Map(GraphTraversal.__().Count());
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "1", "1", "1", "1", "1", "1" }, result);
            }
        }

        [TestMethod]
        public void G_V_Map_Out_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Map(GraphTraversal.__().Out().Count());
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "3", "0", "0", "2", "0", "1" }, result);
            }
        }

        [TestMethod]
        public void G_V_Map_Union_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Map(GraphTraversal.__().Union(GraphTraversal.__().Count()));
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "1", "1", "1", "1", "1", "1" }, result);
            }
        }

        [TestMethod]
        public void G_V_Map_Union_Out_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Map(GraphTraversal.__().Union(GraphTraversal.__().Out().Count()));
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "3", "0", "0", "2", "0", "1" }, result);
            }
        }

        [TestMethod]
        public void G_V_Map_Out_Union_Count()
        {
            using (GraphViewCommand command = this.job.Command)
            {
                this.job.Traversal = command.g().V().Map(GraphTraversal.__().Out().Union(GraphTraversal.__().Count()));
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "3", "2", "1" }, result);
            }
        }
    }
}
