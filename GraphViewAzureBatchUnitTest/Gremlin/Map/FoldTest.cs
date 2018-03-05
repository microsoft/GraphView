using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class FoldTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// g_V_fold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/FoldTest.java
        /// Gremlin: g.V().fold();
        /// </summary>
        [TestMethod]
        public void BasicFold()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Fold();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic result = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());
                graphCommand.OutputFormat = OutputFormat.Regular;

                Assert.AreEqual(1, ((JArray)result).Count);
                Assert.AreEqual(6, ((JArray)result[0]).Count);
            }
        }

        /// <summary>
        /// g_V_fold_unfold()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/FoldTest.java
        /// Gremlin: g.V().fold().unfold();
        /// </summary>
        [TestMethod]
        public void FoldThenUnfold()
        {
            using (GraphViewCommand graphCommand = this.job.GetCommand())
            {
                graphCommand.OutputFormat = OutputFormat.GraphSON;
                this.job.Traversal = graphCommand.g().V().Fold().Unfold();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                dynamic result = JsonConvert.DeserializeObject<dynamic>(results.FirstOrDefault());
                graphCommand.OutputFormat = OutputFormat.Regular;

                Assert.AreEqual(6, ((JArray)result).Count);
            }
        }
    }
}
