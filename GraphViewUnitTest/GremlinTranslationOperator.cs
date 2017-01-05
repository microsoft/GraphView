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
            const string q2 = @"select null as number from n_0";

            var sr = new StringReader(q2);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;

            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
        }

        [TestMethod]
        public void TestModernGraph()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            GraphTraversal2.g().addV("person").property("age", "27").property("name", "vadas").next();
            GraphTraversal2.g().addV("person").property("age", "29").property("name", "marko").next();
            GraphTraversal2.g().addV("person").property("age", "35").property("name", "peter").next();
            GraphTraversal2.g().addV("person").property("age", "32").property("name", "josh").next();
            GraphTraversal2.g().addV("software").property("lang", "java").property("name", "lop").next();
            GraphTraversal2.g().addV("software").property("lang", "java").property("name", "ripple").next();

            GraphTraversal2.g().V().has("name", "marko").addE("knows").to(GraphTraversal2.g().V().has("name", "vadas")).next();
            GraphTraversal2.g().V().has("name", "marko").addE("knows").to(GraphTraversal2.g().V().has("name", "josh")).next();
            GraphTraversal2.g().V().has("name", "marko").addE("knows").to(GraphTraversal2.g().V().has("name", "lop")).next();
            GraphTraversal2.g().V().has("name", "peter").addE("created").to(GraphTraversal2.g().V().has("name", "lop")).next();
            GraphTraversal2.g().V().has("name", "josh").addE("created").to(GraphTraversal2.g().V().has("name", "lop")).next();
            GraphTraversal2.g().V().has("name", "josh").addE("created").to(GraphTraversal2.g().V().has("name", "ripple")).next();

            // v("lop")
            // v("lop")
            // v("lop")
            // v("vadas")
            // v("josh")
            // v("ripple")
            GraphTraversal2.g().V().Out().next();

        }

        [TestMethod]
        public void TestStep()
        {
            GraphTraversal2.g()
                .V().Out("jinjin").optional(GraphTraversal2.__().Out("mdl").outE().inV()).next();

            GraphTraversal2.g().V().local(GraphTraversal2.__().outE()).properties("name", "age").key().next();

            GraphTraversal2.g().V()
                .project("vertex", "parents", "references", "model")
                .by(GraphTraversal2.__().emit().repeat(GraphTraversal2.__().outE("_val").As("_").inV()).tree())
                .by(GraphTraversal2.__().outE().label().dedup().fold())
                .by(GraphTraversal2.__().As("@v")
                    .flatMap(GraphTraversal2.__().Out("mdl").outE("ref"))
                    .repeat(GraphTraversal2.__().As("@e")
                        .flatMap(GraphTraversal2.__().inV()
                            .As("mdl")
                            .select(GremlinKeyword.Pop.last, "@v")
                            .both()
                            .where(GraphTraversal2.__().Out("mdl")
                                .where(Predicate.eq("mdl"))))
                        .As("@v")
                        .optional(GraphTraversal2.__().flatMap(
                            GraphTraversal2.__().select(GremlinKeyword.Pop.last, "@e")
                                .values("_ref")
                                .As("key")
                                .select(GremlinKeyword.Pop.last, "@v")
                                .Out("mdl")
                                .outE("ref")
                                .where(GraphTraversal2.__().values("_key")
                                    .where(Predicate.eq("key"))))))
                    .until(GraphTraversal2.__().flatMap(
                        GraphTraversal2.__().As("res").select(GremlinKeyword.Pop.last, "@v").where(Predicate.eq("res"))))
                    .union(GraphTraversal2.__().dedup()
                            .emit()
                            .repeat(GraphTraversal2.__().outE("_val").As("_").inV())
                            .tree(),
                        GraphTraversal2.__().project("id", "key", "ref")
                            .by(GraphTraversal2.__().id())
                            .by(GraphTraversal2.__().select(GremlinKeyword.Pop.first, "@e").values("_key"))
                            .by(GraphTraversal2.__().select(GremlinKeyword.Pop.last, "@e").values("_ref"))
                            .fold())
                    .fold())
                .by(GraphTraversal2.__().Out("mdl").project("vertex").by(GraphTraversal2.__().tree())).next();


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
            //        .coalesce(GraphTraversal2.__().inE("device-product"),
            //                     GraphTraversal2.__().inE("device-product")
            //            //GraphTraversal2.__().addE("device-product").from("tgt")
            //            )
            //    )
            //    .count()
            //    .next();


            
        }
    }
}