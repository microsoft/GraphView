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
    public class InjectTest : AbstractAzureBatchGremlinTest
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
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                this.job.Traversal = graphCommand.g().V()
                    .Has("name", "marko")
                    .Values("name")
                    .Inject("daniel");
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "marko", "daniel" }, result);
            }
        }
    }
}
