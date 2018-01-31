using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Branch
{
    [TestClass]
    public class ChooseTest : AbstractAzureBatchGremlinTest
    {

        [TestMethod]
        public void get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX()
        {
            string query = "g.V().choose(__.out().count()).option(2, __.values('name')).option(3, __.valueMap())";
            // todo
        }

        [TestMethod]
        public void get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name()
        {
            string query = "g.V().choose(__.hasLabel('person').and().out('created'), __.out('knows'), __.identity()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            CheckUnOrderedResults(new[] { "lop", "ripple", "josh", "vadas", "vadas" }, results);
        }

        [TestMethod]
        public void get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name()
        {
            string query = "g.V().choose(__.label()).option('blah', __.out('knows')).option('bleep', __.out('created')).option(GremlinKeyword.Pick.None, __.identity()).values('name')";
            List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(query);
            Console.WriteLine("-------------Test Result-------------");
            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
            CheckUnOrderedResults(new[] { "marko", "vadas", "peter", "josh", "lop", "ripple" }, results);
        }

        [TestMethod]
        public void get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name()
        {
            string query = "g.V().choose(__.out('knows').count().is(gt(0)), __.out('knows')).values('name')";
            // todo
        }
    }
}
