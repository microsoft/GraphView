using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;

namespace GremlinTranslationOperator.Tests
{
    [TestClass()]
    public class GremlinTranslationOperator
    {
        [TestMethod()]
        public void nextTest()
        {
            GraphTraversal2 g = new GraphTraversal2();
            g.V().next();
        }

        [TestMethod]
        public void test()
        {
            const string q2 = @"
                    select  count(*)
                    from (select * from node n_0) as a";
            var sr = new StringReader(q2);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
        }

    }
}