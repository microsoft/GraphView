using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;
using GraphView.GremlinTranslationOps;

namespace GremlinTranslationOperator.Tests
{
    [TestClass()]
    public class GremlinTranslationOperator
    {
        [TestMethod]
        public void test()
        {
            const string q2 = @"with p_0 as (select * from n_1), p_1 as (select * from n_2) select * from n_0, n_3";

            var sr = new StringReader(q2);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;

            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
        }

        [TestMethod]
        public void TestStep()
        {
            //GraphTraversal2.g().V().local(GraphTraversal2.__().Out()).outE().next();

            GraphTraversal2.g().V().outE().local(GraphTraversal2.__().inV()).Out().next();
        }
    }
}