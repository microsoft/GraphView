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
        [TestMethod]
        public void BasicInject()
        {
            using (GraphViewCommand graphCommand = this.job.Command)
            {
                this.job.Traversal = graphCommand.g().V()
                    .Has("name", "marko")
                    .Values("name")
                    .Inject("daniel");
                List<string> result = this.jobManager.TestQuery(this.job);

                CheckUnOrderedResults(new[] { "marko", "daniel" }, result);
            }
        }
    }
}
