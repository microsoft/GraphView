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
    public class ConstantTest : AbstractAzureBatchGremlinTest
    {
        /// <summary>
        /// g_V_constantX123X()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/ConstantTest.java
        /// Gremlin: g.V().constant(123);
        /// </summary>
        [TestMethod]
        public void ConstantWithVertex()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().Constant(123);
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                CheckUnOrderedResults(Enumerable.Repeat("123", 6), result);
            }
        }

        /// <summary>
        /// g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX()
        /// from org/apache/tinkerpop/gremlin/process/traversal/step/map/ConstantTest.java
        /// Gremlin: g.V().choose(hasLabel("person"), values("name"), constant("inhuman"));
        /// </summary>
        [TestMethod]
        public void ConstantWithChoose()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                 this.job.Traversal = GraphViewCommand.g().V().Choose(
                    GraphTraversal.__().HasLabel("person"),
                    GraphTraversal.__().Values("name"),
                    GraphTraversal.__().Constant("inhuman"));
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                CheckUnOrderedResults(new string[] { "marko", "vadas", "inhuman", "josh", "inhuman", "peter" }, result);
            }
        }
    }
}
