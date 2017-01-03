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


            GraphTraversal2.g().inject(0).union(
                    GraphTraversal2.__().not(GraphTraversal2.g().V() //n_0
                               .has("_app", "test-app") //n_0
                               .hasLabel("application") //n_0
                               .has("_deleted", false)) //n_0
                        .constant("~0"), //tvf_0
                    GraphTraversal2.g().V() //n_1 
                        .has("_app", "test-app") //n_1
                        .hasLabel("application") //n_1
                        .has("_provisioningState", 0) //n_1
                        .constant("~1"), //tvf_1
                    GraphTraversal2.g().V() //n_2
                        .has("_app", "test-app") //n_2
                        .hasLabel("application") //n_2
                        .has("_provisioningState", 2) //n_2
                        .constant("~2"), //tvf_2
                    GraphTraversal2.g().V() //n_3
                        .has("_app", "test-app") //n_3
                        .has("_id", "product:soda-machine:shop-1") //n_3
                        .hasLabel("product") //n_3
                        .constant("~3"),//tvf_3
                    GraphTraversal2.__().not(GraphTraversal2.g().V() //n_4
                               .has("_app", "test-app") //n_4
                               .has("_id", "product:soda-machine") //n_4
                               .hasLabel("product-model")) //n_4
                        .constant("~4"), //tvf_4
                    GraphTraversal2.g().V() //n_5
                        .has("_app", "test-app")  //n_5
                        .has("_id", "product:soda-machine")  //n_5
                        .hasLabel("product-model")  //n_5
                        .outE("_val") //n_5 -[edge as e_0] -> n_6
                        .has("_key", "_properties") // e_0
                        .inV() //n_6
                        .Out("_val") //n_6 -[edge as e_1] -> n_7
                        .has("kind", "property") //n_7
                        .values("id") //tvf_5
                        .Is(Predicate.without("name", "location", "installer")) //tvf_5
                        .constant("~5"), //tvf_6
                    GraphTraversal2.g().V() //n_8
                        .has("_app", "test-app") //n_8
                        .has("_id", "product:soda-machine:shop-2") //n_8
                        .hasLabel("product") //n_8
                        .constant("~6"),  //tvf_7
                    GraphTraversal2.g().V() //n_9
                        .has("_app", "test-app") //n_9
                        .has("_id", "product:soda-machine:shop-3.1") //n_9
                        .hasLabel("product") //n_9
                        .constant("~7"), //tvf_8
                    GraphTraversal2.g().V() //n_10
                        .has("_app", "test-app") //n_10
                        .has("_id", "product:soda-machine:shop-3.2") //n_10
                        .hasLabel("product") //n_10
                        .constant("~8"), //tvf_9
                    GraphTraversal2.g().V() //n_11
                        .has("_app", "test-app") //n_11
                        .has("_id", "uber-product:soda-machine:shop-3") //n_11
                        .hasLabel("product") //n_11
                        .constant("~9"), //tvf_10
                    GraphTraversal2.__().not(GraphTraversal2.g().V() //n_12
                               .has("_app", "test-app") //n_12
                               .has("_id", "uber-product:soda-machine") //n_12
                               .hasLabel("product-model")) //n_12
                        .constant("~10"), //tvf_11
                    GraphTraversal2.g().V() //n_13
                        .has("_app", "test-app") //n_13
                        .has("_id", "uber-product:soda-machine") //n_13
                        .hasLabel("product-model") //n_13
                        .outE("_val") //n_13 -[edge as e_2] -> n_14
                        .has("_key", "_properties") //e_2
                        .inV() //n_14
                        .Out("_val") //n_14->[edge as e_3] -> n_15
                        .has("kind", "property") //n_15
                        .values("id") //tvf_12 
                        .Is(Predicate.without("name", "location", "installer")) //tvf_12
                        .constant("~11"), //tvf_13
                    GraphTraversal2.g().V() //n_16
                        .has("_app", "test-app") //n_16
                        .has("_id", "device:ice-machine:shop-1") //n_16
                        .hasLabel("device") //n_16
                        .constant("~12"), //tvf_14
                    GraphTraversal2.__().not(GraphTraversal2.g().V() //n_17
                               .has("_app", "test-app") //n_17
                               .has("_id", "device:ice-machine") //n_17
                               .hasLabel("device-model")) //n_17
                        .constant("~13"), //tvf_15
                    GraphTraversal2.g().V() //n_18
                        .has("_app", "test-app") //n_18
                        .has("_id", "device:ice-machine") //n_18
                        .hasLabel("device-model") //n_18
                        .outE("_val") //n_18 -[edge as e_4] -> n_19
                        .has("_key", "_properties") //e_4
                        .inV() //n_19
                        .Out("_val") // n_19 -[edge as e_5] ->n_20
                        .has("kind", "property") //n_20
                        .values("id") //tvf_16
                        .Is(Predicate.without("name", "serial_number", "firmware_version", "ice_level")) //tvf_16
                        .constant("~14"), //tvf_17
                    GraphTraversal2.g().V() //n_21
                        .has("_app", "test-app") //n_21
                        .has("_id", "device:soda-mixer:shop-1") //n_21
                        .hasLabel("device") //n_21
                        .constant("~15"), //tvf_18
                    GraphTraversal2.__().not(
                          GraphTraversal2.g().V() //n_22
                              .has("_app", "test-app") //n_22
                              .has("_id", "device:soda-mixer") //n_22
                              .hasLabel("device-model")) //n_22
                        .constant("~16"), //tvf_19
                    GraphTraversal2.g().V() //n_23
                        .has("_app", "test-app") //n_23
                        .has("_id", "device:soda-mixer") //n_23
                        .hasLabel("device-model") //n_23
                        .outE("_val") //n_23 -[edge as e_6]-> n_24
                        .has("_key", "_properties") //e_6
                        .inV() //n_24
                        .Out("_val") //n_24 -[edge as e_7]-> n_25
                        .has("kind", "property") //n_25
                        .values("id") //tvf_20
                        .Is(Predicate.without("name", "serial_number", "firmware_version", "co2_level", "syrup_level")) //tvf_20
                        .constant("~17"), //tvf_21
                    GraphTraversal2.g().V() //n_26 
                        .has("_app", "test-app") //n_26
                        .has("_id", "device:ice-machine:shop-2") //n_26
                        .hasLabel("device") //n_26
                        .constant("~18"), //tvf_22
                    GraphTraversal2.g().V() //n_27
                        .has("_app", "test-app")
                        .has("_id", "device:cola-mixer:shop-2") //n_27
                        .hasLabel("device") //n_27
                        .constant("~19"), //tvf_23
                    GraphTraversal2.g().V() //n_28
                        .has("_app", "test-app") //n_28
                        .has("_id", "device:root-beer-mixer:shop-2") //n_28
                        .hasLabel("device") //n_28
                        .constant("~20"), //tvf_24
                    GraphTraversal2.g().V() //n_29
                        .has("_app", "test-app") //n_29
                        .has("_id", "device:lemonade-mixer:shop-2") //n_29
                        .hasLabel("device") //n_29
                        .constant("~21"), //tvf_25
                    GraphTraversal2.g().V() //n_30
                        .has("_app", "test-app") //n_30
                        .has("_id", "device:ice-machine:shop-3.1") //n_30
                        .hasLabel("device") //n_30
                        .constant("~22"), //tvf_26
                    GraphTraversal2.g().V() //n_31
                        .has("_app", "test-app") //n_31
                        .has("_id", "device:soda-mixer:shop-3.1") //n_31
                        .hasLabel("device") //n_31
                        .constant("~23"), //tvf_27
                    GraphTraversal2.g().V() //n_32
                        .has("_app", "test-app") //n_32
                        .has("_id", "device:ice-machine:shop-3.2") //n_32
                        .hasLabel("device") //n_32
                        .constant("~24"), //tvf_28
                    GraphTraversal2.g().V() //n_33
                        .has("_app", "test-app") //n_33
                        .has("_id", "device:cola-mixer:shop-3.2") //n_33
                        .hasLabel("device") //n_33
                        .constant("~25"), //tvf_29
                    GraphTraversal2.g().V() //n_34
                        .has("_app", "test-app") //n_34
                        .has("_id", "device:kool-aid-mixer:shop-3.2") //n_34
                        .hasLabel("device") //n_34
                        .constant("~26"), //tvf_30
                    GraphTraversal2.__().not(
                          GraphTraversal2.g().V() //n_35
                              .has("_app", "test-app")//n_35
                              .has("_id", "product:soda-machine")//n_35
                              .hasLabel("product-model")//n_35
                              .both("device-product") //n_35 -[edge as e8] - n_36
                              .has("_app", "test-app") //n_36
                              .has("_id", "device:ice-machine") //n_36
                              .hasLabel("device-model")) //n_36
                        .constant("~27"), //tvf_31
                    GraphTraversal2.__().not(
                          GraphTraversal2.g().V() //n_37
                              .has("_app", "test-app") //n_37
                              .has("_id", "product:soda-machine") //n_37
                              .hasLabel("product-model") //n_37
                              .both("device-product") //n_37 -[edge e_9]- n_38
                              .has("_app", "test-app") //n_38
                              .has("_id", "device:soda-mixer") //n_38
                              .hasLabel("device-model")) //n_38
                        .constant("~28"), //tvf_32
                    GraphTraversal2.__().not(
                          GraphTraversal2.g().V() //n_39
                              .has("_app", "test-app") //n_39
                              .has("_id", "uber-product:soda-machine") //n_39
                              .hasLabel("product-model") //n_39
                              .both("product-product") //n_39 -[edge as e_10]- n_40
                              .has("_app", "test-app") //n_40
                              .has("_id", "product:soda-machine") //n_40
                              .hasLabel("product-model")) //n_40
                        .constant("~29")) //tvf_33
                .fold()
                //.Is(Predicate.neq("[]"))
                .next();


        }
    }
}