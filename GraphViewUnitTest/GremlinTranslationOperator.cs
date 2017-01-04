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

            GraphTraversal2.g().V().union(GraphTraversal2.__().dedup()
                        .emit()
                        .repeat(GraphTraversal2.__().outE("_val").As("_").inV())
                        .tree(),
                    GraphTraversal2.__().project("id", "key", "ref")
                        .by(GraphTraversal2.__().id())
                        .by(GraphTraversal2.__().select(GremlinKeyword.Pop.first, "@e").values("_key"))
                        .by(GraphTraversal2.__().select(GremlinKeyword.Pop.last, "@e").values("_ref"))
                        .fold()).next();

            GraphTraversal2.g().V().As("@v")
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
                                .where(Predicate.eq("key")))))
                   )
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
                .fold().next();


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