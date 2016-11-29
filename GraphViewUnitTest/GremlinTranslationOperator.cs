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
        [TestMethod()]
        public void nextTest()
        {
            GraphTraversal2 g = new GraphTraversal2();
            //g.V().As("a").Out().As("b").addE("123").@from("a").next(); //pass
            //g.V().As("a").Out().As("b").addE("123").@from("a").to("b").next(); //pass
            //g.V().addV().next(); //pass
            //g.V().addV().property("name", "jinjin").next(); //pass

            // TODO: add property 
            //g.V().property("name", "jinjin").next();

            //g.constant("123").next(); //pass
            //g.V().drop().next(); //pass
            //g.E().drop().next(); //pass
            //g.V().has("name", "jinjin").next(); //pass
            //g.V().group().@by("name").next(); //pass
            //g.V().order().@by("name").next(); //pass
            //g.V().order().@by("name").@by("age").next(); //pass
            //g.V().Out().next(); //pass
            //g.V().Out("create").next(); //pass
            //g.V().outE().next(); //pass
            //g.V().outE("create").next(); //pass
            //g.V().In().next(); //pass
            //g.V().In("create").next(); //pass
            //g.V().inE().next(); //pass
            //g.V().inE("create").next(); //pass
            //g.E().bothV().next(); //pass

            //g.V().bothE().next(); //pass

            //g.V().bothE("create").next(); //pass
            //g.V().count().next();  //pass
            //g.V().values("age").next(); //pass
            //g.V().values("name", "age").next(); //pass
            //g.V().values("age").max().next(); //pass
            //g.V().values("age").min().next(); //pass
            //g.V().values("age").mean().next(); //pass
            //g.V().values("age").sum().next(); //pass
            //g.V().fold().next();
            //g.V().fold().unfold().next();

            //g.V().sample(5).next(); // pass
            //g.V().coin(0.5).next(); //pass
            //g.V().limit(5).next(); //pass
            //g.V().range(1, 5).next(); //pass
            //g.E().range(1, 5).next(); //pass
            //g.V().tail(5).next(); //pass
            //g.V().@where("name", Predicate.eq("jinjin")).next(); //pass
            //g.V().@where("age", Predicate.neq(1)).next(); //pass
            //g.V().@where("name", Predicate.lt(1)).next(); //pass
            //g.V().@where("name", Predicate.gt(1)).next(); //pass
            //g.V().@where("name", Predicate.gte(1)).next(); //pass
            //g.V().next();

            //g.V().choose(GraphTraversal2.__().values("age"))
            //    .option(1, GraphTraversal2.__().values("first_name"))
            //    .option(2, GraphTraversal2.__().values("lase_name")).next(); //pass

            //g.V().choose(GraphTraversal2.__().values("isMale"),
            //    GraphTraversal2.__().values("first_name"),
            //    GraphTraversal2.__().values("last_name")).next(); //pass

            //g.V().coalesce(GraphTraversal2.__().outE("'knows'"),
            //                GraphTraversal2.__().outE("'created'")).next(); //pass

            //g.V().and(GraphTraversal2.__().outE("knows"),
            //            GraphTraversal2.__().@where("name", Predicate.eq("jinjin"))).values("name").next(); //pass
            //g.V().Or(GraphTraversal2.__().outE("knows"),
            //            GraphTraversal2.__().@where("name", Predicate.eq("jinjin"))).values("name").next(); //pass
            //g.V().and(GraphTraversal2.__().outE("knows"),
            //            GraphTraversal2.__().values("age").Is(Predicate.lt(30))).values("name").next();

            //g.V().match(
            //    GraphTraversal2.__().As("a").Out("created").As("b"),
            //    GraphTraversal2.__().As("b").has("name", "lop"),
            //    GraphTraversal2.__().As("b").In("created").As("c"),
            //    GraphTraversal2.__().As("c").has("age", 29)).next();  //pass

            //g.V().match(
            //    GraphTraversal2.__().As("a").Out("created").has("name", "lop").As("b"),
            //    GraphTraversal2.__().As("b").In("created").has("age", 29).As("c")).next(); //pass

            //g.V().match(
            //     GraphTraversal2.__().As("a").has("name", "Garcia"),
            //     GraphTraversal2.__().As("a").In("writtenBy").As("b"),
            //     GraphTraversal2.__().As("a").In("sungBy").As("b")).next(); //pass

            g.V().match(
                GraphTraversal2.__().As("a").Out().count().As("b"),
                GraphTraversal2.__().As("a").In().count().As("b")
            ).next();

            //g.E().next();
            /*
                sqlFragment: WSqlFragment
                    FirstTokenIndex: int
                    FromClause: WFromClause
                    GroupByClause: WGroupByClause
                    HavingClause: WHavingClause
                    Into: WSchemaObjectName
                    LastTokenIndex: int
                    LimitClause: WLimitClause
                    MatchClause: WMatchClause
                    OrderByClause: WOrderClause
                    OutputPath: bool
                    SelectElements: List<WSelectElement>
                    TopRowFilter: WTopRowFilter
                    UniqueRowFilter: UniqueRowFilter
                    WhereClause: WWhereClause
                    WithPathClause: WWithPathClause
             */

            //g.V().addE("123").next();
            /*
                 sqlFragment: WInsertEdgeSpecification
                    Columns: List<WColumnReferenceExpression>
                    EdgeColumn: WColumnReferenceExpression
                    FirstTokenIndex: int
                    InsertOption: InsertOption
                    InsertSource: WInsertSource
                    LastTokenIndex: int
                    SelectInsertSource: WSelectInsertSource
                    Target: WNamedTableReference
                    TopRowFilter: WTopRowFilter
             
             */
        }

        [TestMethod]
        public void test()
        {
            const string q2 = @"select * from inject(1) as n_1";

            var sr = new StringReader(q2);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
        }

        [TestMethod]
        public void GremlinFuntionalTestSuite()
        {
            GraphTraversal2.g().V().Out().next();
        }
    }
}