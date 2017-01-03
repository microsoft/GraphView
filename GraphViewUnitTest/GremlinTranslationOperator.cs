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
        [TestMethod]
        public void test()
        {
            const string q2 = @"select TVF_1.id from (select n_0.id as [n_0.id] from  node n_0) as D_0 where D_0.n_0.id = 1";

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
            //GraphTraversal2.g().addV("test").property("name", "jinjin").addE("edge").to(GraphTraversal2.g().addV("test2").property("age", "22")).property("label", "123").next();


            GraphTraversal2.g().inject(0)
                .union(GraphTraversal2.__().not(GraphTraversal2.g().V()
                            .has("_app", "test-app")
                            .hasLabel("application")
                            .has("_deleted", false))
                        .constant("~0"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .hasLabel("application")
                        .has("_provisioningState", 0)
                        .constant("~1"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .hasLabel("application")
                        .has("_provisioningState", 2)
                        .constant("~2"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .has("_id", "product:soda-machine")
                        .hasLabel("product-model")
                        .constant("~3"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .has("_id", "uber-product:soda-machine")
                        .hasLabel("product-model")
                        .constant("~4"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .has("_id", "device:ice-machine")
                        .hasLabel("device-model")
                        .constant("~5"),
                    GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .has("_id", "device:soda-mixer")
                        .hasLabel("device-model")
                        .constant("~6")
                        ).fold()
                        //.Is(Predicate.neq(new List<object>()))
                        .next();
        }
    }
}