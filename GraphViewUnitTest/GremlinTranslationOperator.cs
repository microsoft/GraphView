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
            GraphTraversal2.g()
                .V()
                .union(GraphTraversal2.__().Out(), GraphTraversal2.__().outE(), GraphTraversal2.__().In())
                .next();

            GraphTraversal2.g().addV("test").property("name", "jinjin").addE("edge").to(GraphTraversal2.g().addV("test2").property("age", "22")).property("label", "123").next();

            GraphTraversal2.g().inject(0).sideEffect(GraphTraversal2.__().union(
                GraphTraversal2.g().addV("product-model") //addV_0
                    .property("_app", "test-app") //addV_0
                    .property("_id", "uber-product:soda-machine") //addV_0
                    .sideEffect(GraphTraversal2.__().union( //addV_0
                        GraphTraversal2.__().property("_name", "Uber Soda Machine"), //addV_0
                        GraphTraversal2.__().sideEffect( //addV_0
                            GraphTraversal2.__().addE("_val") //addV_0 -> addE_0
                                .to(GraphTraversal2.__().addV("_val").property("_app", "test-app")) //addV_1
                                .property("_key", "_properties") //addE_0
                                .property("_ary", true) //addE_0
                                .inV() //addV1
                                .sideEffect(GraphTraversal2.__().addE("_val") //addV_1 -> addE_1
                                    .to(GraphTraversal2.__().addV("_val") //addV_2
                                        .property("_app", "test-app")) //addV_2
                                    .property("_key", "0") //addE_1
                                    .property("_ary", false) //addE_1
                                    .inV() //addV_2
                                    .property("id", "location") //addV_2
                                    .property("name", "Soda machine location") //addV_2
                                    .property("kind", "property") //addV_2
                                    .property("type", "string")) //addV_2
                        ))))).next();

            GraphTraversal2.g().inject(0).sideEffect(GraphTraversal2.__().union(
                    GraphTraversal2.g().addV("product-model") //addV_0
                        .property("_app", "test-app") //addV_0
                        .property("_id", "uber-product:soda-machine") //addV_0
                        .sideEffect(GraphTraversal2.__().union( //addV_0
                            GraphTraversal2.__().property("_name", "Uber Soda Machine"), //addV_0
                            GraphTraversal2.__().sideEffect( //addV_0
                                GraphTraversal2.__().addE("_val") //addV_0 -> addE_0
                                    .to(GraphTraversal2.__().addV("_val").property("_app", "test-app")) //addV_1
                                    .property("_key", "_properties") //addE_0
                                    .property("_ary", true) //addE_0
                                    .inV() //addV1
                                    .sideEffect(GraphTraversal2.__().addE("_val") //addV_1 -> addE_1
                                        .to(GraphTraversal2.__().addV("_val") //addV_2
                                            .property("_app", "test-app")) //addV_2
                                        .property("_key", "0") //addE_1
                                        .property("_ary", false) //addE_1
                                        .inV() //addV_2
                                        .property("id", "location") //addV_2
                                        .property("name", "Soda machine location") //addV_2
                                        .property("kind", "property") //addV_2
                                        .property("type", "string")) //addV_2
                                    .sideEffect(GraphTraversal2.__().addE("_val") //addV_1 ->addE_2
                                        .to(GraphTraversal2.__().addV("_val") //addV_3
                                            .property("_app", "test-app")) //addV_3
                                        .property("_key", "1") //addE_2
                                        .property("_ary", false) //addE_2
                                        .inV() //addV_3
                                        .property("id", "installer") //addV_3
                                        .property("name", "Soda machine installer") //addV_3
                                        .property("kind", "property") //addV_3
                                        .property("type", "string")) //addV_3
                                    .sideEffect(
                                        GraphTraversal2.__().addE("_val") //addV_1 ->addE_3
                                            .to(GraphTraversal2.__().addV("_val").property("_app", "test-app")) //addV_4
                                            .property("_key", "2") //addE_3
                                            .property("_ary", false) //addE_3
                                            .inV() //addV_4
                                            .property("id", "syrup_level") //addV_4
                                            .property("name", "Syrup Level") //addV_4
                                            .property("kind", "reference") //addV_4
                                            .sideEffect(
                                                GraphTraversal2.__().addE("_val") //addV_4->addE_4
                                                    .to(GraphTraversal2.__().addV("_val") //addV_5
                                                        .property("_app", "test-app")) //addV_5
                                                    .property("_key", "target") //addE_4
                                                    .property("_ary", false) //addE_4
                                                    .inV() //addV_5
                                                    .property("id", "product:soda-machine")//addV_5
                                                    .property("type", "product")))  //addV_5
                                ),
                            GraphTraversal2.__().addE("product-product") //addV_0 -> addE_5
                                .from( //
                                    GraphTraversal2.g().addV("product-model") //addV_6
                                        .property("_app", "test-app") //addV_6
                                        .property("_id", "product:soda-machine") //addV_6
                                        .sideEffect(GraphTraversal2.__().union( //addV_6
                                            GraphTraversal2.__().property("_name", "Soda Machine"), //addV_6
                                            GraphTraversal2.__().sideEffect( //addV_6
                                                GraphTraversal2.__().addE("_val") //addV_6->addE_6
                                                    .to(GraphTraversal2.__().addV("_val") //addV_7
                                                        .property("_app", "test-app")) //addV_7
                                                    .property("_key", "_properties") //addE_6
                                                    .property("_ary", true) //addE_6
                                                    .inV() //addV_7
                                                    .sideEffect( //addV_7
                                                        GraphTraversal2.__().addE("_val") //addV_7->addE_7
                                                            .to(GraphTraversal2.__().addV("_val").property( //addV_8
                                                                "_app", "test-app")) //addV_8
                                                            .property("_key", "0") //addE_7
                                                            .property("_ary", false) //addE_7
                                                            .inV() //addV_8
                                                            .property("id", "location") //addV_8
                                                            .property("name", "Soda machine location") //addV_8
                                                            .property("kind", "property") //addV_8
                                                            .property("type", "string")) //addV_8
                                                    .sideEffect( //addV_7
                                                        GraphTraversal2.__().addE("_val") //addV_7 -> addE_8
                                                            .to(GraphTraversal2.__().addV("_val").property( //addV_9
                                                                "_app", "test-app")) //addV_9
                                                            .property("_key", "1") //addE_8
                                                            .property("_ary", false) //addE_8
                                                            .inV() //addV_9
                                                            .property("id", "installer") //addV_9
                                                            .property("name", "Soda machine installer") //addV_9
                                                            .property("kind", "property") //addV_9
                                                            .property("type", "string")) //addV_9
                                                    .sideEffect( //addV_7
                                                        GraphTraversal2.__().addE("_val") //addV_7 ->addE_9
                                                            .to(GraphTraversal2.__().addV("_val").property( //addV_10
                                                                "_app", "test-app")) //addV_10
                                                            .property("_key", "2") //addE_9
                                                            .property("_ary", false) //addE_9
                                                            .inV() //addV_10
                                                            .property("id", "syrup_level") //addV_10
                                                            .property("name", "Syrup level") //addV_10
                                                            .property("kind", "reference") //addV_10
                                                            .sideEffect(//addV_10
                                                                GraphTraversal2.__().addE("_val") //addV_10->addE_10
                                                                    .to(GraphTraversal2.__() //addE_10
                                                                            .addV("_val") //addV_11
                                                                            .property("_app", "test-app")) //addV_11
                                                                    .property("_key", "target") //addE_10
                                                                    .property("_ary", false) //addE_10
                                                                    .inV() //addV_11
                                                                    .property("id", "device:soda-mixer") //addV_11
                                                                    .property("type", "device"))) //addV_11
                                                    .sideEffect( //addV_7
                                                        GraphTraversal2.__().addE("_val") //addV_7->addE11
                                                            .to(GraphTraversal2.__().addV("_val").property( //addV_12
                                                                "_app", "test-app")) //addV_12
                                                            .property("_key", "3") //addE_11
                                                            .property("_ary", false) //addE_11
                                                            .inV() //addV_12
                                                            .property("id", "ice_level") //addV_12
                                                            .property("name", "Ice level") //addV_12
                                                            .property("kind", "reference") //addV_12
                                                            .sideEffect( //addV_12
                                                                GraphTraversal2.__().addE("_val") //addV_12->addE_12
                                                                    .to(GraphTraversal2.__().addV("_val") //addV_13
                                                                            .property("_app", "test-app")) //addV_13
                                                                    .property("_key", "target") //addE_12
                                                                    .property("_ary", false)//addE_12
                                                                    .inV() //addV_13
                                                                    .property("id", "device:ice-machine") //addV_13
                                                                    .property("type", "device"))) //addV_13
                                             ),
                                            GraphTraversal2.__().addE("device-product") //addV_6->addE_13->addV_14
                                                .from(
                                                    GraphTraversal2.g().addV("device-model") //addV_14
                                                        .property("_app", "test-app") //addV_14
                                                        .property("_id", "device:ice-machine") //addV_14
                                                        .sideEffect(GraphTraversal2.__().union( //addV_14
                                                            GraphTraversal2.__().property("_name", "Ice Machine"), //addV_14
                                                            GraphTraversal2.__().sideEffect( //addV_14
                                                                GraphTraversal2.__().addE("_val") //addV_14->addE_14
                                                                    .to(GraphTraversal2.__().addV("_val") //addV_15
                                                                            .property("_app", "test-app")) //addV_15
                                                                    .property("_key", "_properties") //addE_14
                                                                    .property("_ary", true)  //addE_14
                                                                    .inV() //addV_15
                                                                    .sideEffect(GraphTraversal2.__().addE("_val") //addV_15->addE_15
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_16
                                                                            .property("_app", "test-app")) //addV_16
                                                                        .property("_key", "0") //addE_15
                                                                        .property("_ary", false) //addE_15
                                                                        .inV() //addV_16
                                                                        .property("id", "firmware_version") //addV_16
                                                                        .property("name", "Firmware Version") //addV_16
                                                                        .property("kind", "desired") //addV_16
                                                                        .property("type", "string") //addV_16
                                                                        .property("path", "/firmware_version")) //addV_16
                                                                    .sideEffect(GraphTraversal2.__().addE("_val") //addV_15->addE_16
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_17
                                                                            .property("_app", "test-app")) //addV_17
                                                                        .property("_key", "1") //addE_16
                                                                        .property("_ary", false) //addE_16
                                                                        .inV() //addV_17
                                                                        .property("id", "serial_number") //addV_17
                                                                        .property("name", "Serial Number") //addV_17
                                                                        .property("kind", "desired") //addV_17
                                                                        .property("type", "string") //addV_17
                                                                        .property("path", "/serial_number")) //addV_17
                                                                    .sideEffect(GraphTraversal2.__().addE("_val") //addV_15->addE_17
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_18
                                                                            .property("_app", "test-app")) //addV_18
                                                                        .property("_key", "2") //addE_17
                                                                        .property("_ary", false) //addE_17
                                                                        .inV() //addV_18
                                                                        .property("id", "ice_level") //addV_18
                                                                        .property("name", "Ice Level") //addV_18
                                                                        .property("kind", "reported") //addV_18
                                                                        .property("type", "number") //addV_18
                                                                        .property("path", "/ice_level")) //addV_18
                                                                        ))))
                                            ,
                                            GraphTraversal2.__().addE("device-product") //addV_6 -> addE_18
                                                .from(GraphTraversal2.g().addV("device-model") //addV_19
                                                    .property("_app", "test-app") //addV_19
                                                    .property("_id", "device:soda-mixer") //addV_19
                                                    .sideEffect(GraphTraversal2.__().union( //addV_19
                                                        GraphTraversal2.__().property("_name", "Soda Mixer"), //addV_19
                                                        GraphTraversal2.__().sideEffect( //addV_19
                                                            GraphTraversal2.__().addE("_val") //addV_19->addE_19
                                                                .to(GraphTraversal2.__() //addV_20
                                                                    .addV("_val") //addV_20
                                                                    .property("_app", "test-app")) //addV_20
                                                                .property("_key", "_properties") //addE_19
                                                                .property("_ary", true) //addE_19
                                                                .inV() //addV_20
                                                                .sideEffect( //addV_20
                                                                    GraphTraversal2.__().addE("_val") //addV_20->addE_20
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_21
                                                                            .property("_app", "test-app")) //addV_21
                                                                        .property("_key", "0") //addE_20
                                                                        .property("_ary", false) //addE_20
                                                                        .inV() //addV_21
                                                                        .property("id", "firmware_version") //addV_21
                                                                        .property("name", "Firmware Version") //addV_21
                                                                        .property("kind", "desired") //addV_21
                                                                        .property("type", "string") //addV_21
                                                                        .property("path", "/firmware_version")) //addV_21
                                                                .sideEffect( //addV_20
                                                                    GraphTraversal2.__().addE("_val") //addV_20->addE_21
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_22
                                                                            .property("_app", "test-app")) //addV_22
                                                                        .property("_key", "1") //addE_21
                                                                        .property("_ary", false) //addE_21
                                                                        .inV() //addV_22
                                                                        .property("id", "serial_number") //addV_22
                                                                        .property("name", "Serial Number") //addV_22
                                                                        .property("kind", "desired") //addV_22
                                                                        .property("type", "string") //addV_22
                                                                        .property("path", "/serial_number")) //addV_22
                                                                .sideEffect(GraphTraversal2.__().addE("_val") //addV_20->addE_22
                                                                    .to(GraphTraversal2.__().addV("_val").property( //addV_23
                                                                        "_app", "test-app"))  //addV_23
                                                                    .property("_key", "2") //addE_22
                                                                    .property("_ary", false) //addE_22
                                                                    .inV() //addV23
                                                                    .property("id", "co2_level") //addV23
                                                                    .property("name", "CO2 Level") //addV23
                                                                    .property("kind", "reported") //addV23
                                                                    .property("type", "number") //addV23
                                                                    .property("path", "/co2_level")) //addV23
                                                                .sideEffect( //addV20
                                                                    GraphTraversal2.__().addE("_val") //addV20->addE_23
                                                                        .to(GraphTraversal2.__().addV("_val") //addV_24
                                                                            .property("_app", "test-app"))  //addV_24
                                                                        .property("_key", "3") //addE_23
                                                                        .property("_ary", false) //addE_23
                                                                        .inV()  //addV_24
                                                                        .property("id", "syrup_level")  //addV_24
                                                                        .property("name", "Syrup Level")  //addV_24
                                                                        .property("kind", "reported")  //addV_24
                                                                        .property("type", "number")  //addV_24
                                                                        .property("path", "/syrup_level"))  //addV_24
                                                                        ))))
                                        )))
                                        )
                   )
                )
                )//inject_0
                .sideEffect(GraphTraversal2.__().union(
                    GraphTraversal2.g().V() //n_0
                        .has("_app", "test-app") //n_0
                        .has("_id", "product:soda-machine") //n_0
                        .hasLabel("product-model") //n_0
                        .addE("ref") //n_0 -> addE_24
                        .to(GraphTraversal2.g().V() // n_1
                            .has("_app", "test-app") // n_1
                            .has("_id", "device:soda-mixer") // n_1
                            .hasLabel("device-model")) // n_1
                        .property("_key", "syrup_level") //addE_24
                        .property("_ref", "syrup_level"), //addE_24
                    GraphTraversal2.g().V() //n_2
                        .has("_app", "test-app")  //n_2
                        .has("_id", "product:soda-machine")  //n_2
                        .hasLabel("product-model")  //n_2
                        .addE("ref")  //n_2->addE_25
                        .to(GraphTraversal2.g().V() //n_3
                            .has("_app", "test-app") //n_3
                            .has("_id", "device:ice-machine") //n_3
                            .hasLabel("device-model")) //n_3
                        .property("_key", "ice_level") //addE_25
                        .property("_ref", "ice_level"),//addE_25
                    GraphTraversal2.g().V() //n_4
                        .has("_app", "test-app") //n_4
                        .has("_id", "uber-product:soda-machine")  //n_4
                        .hasLabel("product-model") //n_4
                        .addE("ref") //n_4->addE_26
                        .to(GraphTraversal2.g().V() //n_5
                            .has("_app", "test-app") //n_5
                            .has("_id", "product:soda-machine") //n_5
                            .hasLabel("product-model")) //n_5
                        .property("_key", "syrup_level") //addE_26
                        .property("_ref", "syrup_level"))) //addE_26
                                                           //.count()
                .next();

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
            .until(GraphTraversal2.__().As("res").select(GremlinKeyword.Pop.last, "@v").where(Predicate.eq("res"))).
            next();
        }
    }
}