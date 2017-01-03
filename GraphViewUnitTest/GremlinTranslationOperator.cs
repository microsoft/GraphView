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


            GraphTraversal2.g().V().As("@v")  //n_0 as @v
                .Out("mdl") //n_0-[edge as e_0]->n_1   n_1
                .outE("ref") //n_1->[edge as e_1] e_1
                .repeat(GraphTraversal2.__().As("@e") //e_1 as @e
                         .inV() //n_2
                         .As("mdl") //n_2.id as mdl
                         .select(GremlinKeyword.Pop.last, "@v")  //n_0
                         .both()  //n_0-[edge as e_2]-n_3
                         .where(GraphTraversal2.__().Out("mdl").where(Predicate.eq("mdl"))) //n_3-[edge as e_3]->n_4 && n_4.id = n_2.id
                         .As("@v") // n_3 as @v
                         .optional(GraphTraversal2.__().select(GremlinKeyword.Pop.last, "@e")  //e_1
                                .values("_ref")  //e_1._ref
                                .As("key")  //e_1._ref as key
                                .select(GremlinKeyword.Pop.last, "@v") //n_3
                                .Out("mdl") //n_3-[edge as e_4]->n_5
                                .outE("ref") //n_5->[edge as e_5]
                                .where(GraphTraversal2.__().values("_key")  // e_5._key
                                           .where(Predicate.eq("key")))))  //e_1._ref = e_5._key
                .until(GraphTraversal2.__().As("res").select(GremlinKeyword.Pop.last, "@v").where(Predicate.eq("res"))).next();
        }
    }
}