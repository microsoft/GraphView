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

            GraphTraversal2.g().V()
                .has("_app", "test-app")
                .has("_id", "product:soda-machine:shop-2")
                .hasLabel("product")
                .flatMap(GraphTraversal2.__().As("src")
                    .flatMap(GraphTraversal2.g().V()
                        .has("_app", "test-app")
                        .has("_id", "device:soda-mixer:shop-1")
                        .hasLabel("device"))
                    .As("tgt")
                    .select("src")
                    .coalesce(GraphTraversal2.__().inE("device-product"),
                                 GraphTraversal2.__().inE("device-product")
                        //GraphTraversal2.__().addE("device-product").from("tgt")
                        )
                )
                .count()
                .next();


            //GraphTraversal2.g().V()
            //    .has("_app", "test-app")
            //    .has("_id", "product:soda-machine:shop-2")
            //    .hasLabel("product")
            //    .flatMap(GraphTraversal2.__().As("src")
            //        .flatMap(GraphTraversal2.g().V()
            //            .has("_app", "test-app")
            //            .has("_id", "device:soda-mixer:shop-1")
            //            .hasLabel("device"))
            //        .As("tgt")
            //        .select("src")
            //        .coalesce(GraphTraversal2.__().inE("device-product")
            //                .where(GraphTraversal2.__().otherV().where(Predicate.eq("tgt"))),
            //            GraphTraversal2.__().addE("device-product").from("tgt")))
            //    .count().next();
        }
    }
}