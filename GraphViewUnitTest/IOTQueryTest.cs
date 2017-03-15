using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using GraphViewUnitTest.Gremlin;

namespace GraphViewUnitTest
{
    [TestClass]
    public class IOTQueryTest
    {
        /// <summary>
        /// test and fix the query 44 id() operator, invalid argumentsError
        /// </summary>
        /// <remarks>
        /// The reason is scalar variable let Id() operator throws exception, just let it return the default column name to avoid the error.
        /// </remarks>
        [TestMethod]
        public void queryTest44()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            //connection.ResetCollection();
            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.OutputFormat = OutputFormat.GraphSON;
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).union(__.select('vertices').unfold().sideEffect(__.id().store('^ids')),__.select('edges').unfold().union(__.inV(),__.outV())).dedup().union(__.identity(),__.as('@v').flatMap(__.optional(__.out('mdl')).outE('ref')).repeat(__.as('@e').flatMap(__.inV().as('mdl').select(last,'@v').both().dedup().and(__.optional(__.out('mdl')).where(eq('mdl')))).as('@v').optional(__.flatMap(__.select(last,'@e').values('|ref:propertyId').as('key').select(last,'@v').optional(__.out('mdl')).outE('ref').and(__.values('/_id').where(eq('key')))))).until(__.flatMap(__.as('res').select(last,'@v').where(eq('res')))).sideEffect(__.project('segments','targets').by(__.select('@e').unfold().fold()).by(__.select('@v').unfold().project('label','/_id','|v0','|v1').by(__.label()).by(__.values('/_id')).by(__.values('|v0')).by(__.values('|v1')).fold()).store('^refs'))).dedup().union(__.identity().sideEffect(__.group('^mdls').by(__.id()).by(__.coalesce(__.out('mdl').id(),__.constant('')))),__.out('mdl')).dedup()).union(__.emit().repeat(__.outE('_val').as('_').inV()).tree(),__.cap('^ids'),__.cap('^mdls'),__.cap('^refs')).fold().union(__.identity(),__.cap('^edges')))";   
            // (1) extract the top error sub query
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).union(__.select('vertices').unfold().sideEffect(__.id().store('^ids')),__.select('edges').unfold().union(__.inV(),__.outV()))))";
            // (2) Remove the union out of it
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).select('vertices').unfold().sideEffect(__.id().store('^ids'))))";
            // (3) Remove the first id op 
            // remove unfold, also throw, when debug into unfold the pivot variable is also the scalar variable
            cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end'))))).id()))";
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).id()))";
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges'))))";
            // The reason is that the unfold operator make the data to the Scalar type
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()).store('^edges')).select('vertices').sideEffect(__.id().store('^ids'))))";
            // (4) Remove the store op
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge','source','target').by().by(__.outV().id()).by(__.inV().id()))))";
            // (5) Make it to single column project
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))).project('edge').by(__.outV().id()))))";
            // (6)
            //cmd.CommandText = "g.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').coalesce(__.union(__.not(__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application')).constant('~0'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',0).constant('~1'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|provisioning',2).constant('~2'),__.V().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').hasLabel('application').has('|deleted',true).constant('~3')),__.flatMap(__.project('vertices','edges').by(__.constant()).by(__.union(g.E().has('|app','ed011feb-0db1-40de-b633-9ec16b758259').has('/_id','product-to-device-1').has('|v0',1).has('|v1',0).hasLabel('instance')).fold()).sideEffect(__.select('edges').unfold().union(__.identity(),__.hasLabel('model').as('eM').inV().as('end').select('eM').outV().BothE('ref').and(__.otherV().where(eq('end')))))))";
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
        /// <summary>
        /// test and fix the query 44 id() operator, invalid argumentsError
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]
        public void IdTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
             "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
             "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            //var results = graph.g().V().Project("c").By("name").Where("c", Predicate.eq("josh"));
            // (0) check the has 
            //var results = graph.g().V().Has("name", Predicate.eq("josh")).Values("name");
            // (1) first step, ref the origin 
            var results = graph.g().V().Id().Store("store");
            // (2) second step, ref the new alias
            // var results = graph.g().V().Project("c").By("name").Where(GraphTraversal2.__().Values("c").Is("josh"));

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        /// <summary>
        /// test and fix the query 36 project.by.by.or(where(key, predicate))
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]
        public void ProjectTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.CommandText = "g.V().has('weapon', 'lasso').as('character').out('appeared').as('comicbook').select('comicbook').next()";
            //graph.CommandText = "g.V().project('c', 'u').by('|provisioning').by('|provisioning').where('c', gt('u'))";
            //graph.CommandText = "g.V().Where(GraphTraversal2.__().As('a').Values('name').Is('josh'))";
            graph.CommandText = "g.V().where('c')";
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.Execute();

            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
        }

        /// <summary>
        /// test and fix the query 36 project.by.by.or(where(key, predicate))
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]

        public void ProjectTest2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            //var results = graph.g().V().Project("c").By("name").Where("c", Predicate.eq("josh"));
            // (0) check the has 
            //var results = graph.g().V().Has("name", Predicate.eq("josh")).Values("name");
            // (1) first step, ref the origin 
            var results = graph.g().V().Project("c").By("name").Where(GraphTraversal2.__().V().Has("c", Predicate.eq("josh")));
            // (2) second step, ref the new alias
            // var results = graph.g().V().Project("c").By("name").Where(GraphTraversal2.__().Values("c").Is("josh"));
  
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        /// <summary>
        /// test and fix the query 36 project.by.by.or(where(key, predicate))
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]
        public void WhereStep()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Where("name", Predicate.eq("josh"));
            // (0) check the has 
            //var results = graph.g().V().Has("name", Predicate.eq("josh")).Values("name");
            // (1) first step, ref the origin 
            //var results = graph.g().V().Where(GraphTraversal2.__().V().Has("name", Predicate.eq("josh")));
            // (2) second step, ref the new alias
            // var results = graph.g().V().Where(GraphTraversal2.__().Values("c").Is("josh"));

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
        public void runQuery(int queryNum)
        {
            string line;
            // Read the file and display it line by line.
            System.IO.StreamReader file =
               new System.IO.StreamReader("D:\\project\\GraphView_11_29\\DocDB-merge2\\query_processed\\" + queryNum + ".txt");
            line = file.ReadLine();
            file.Close();

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
       "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
       "GroupMatch", "MarvelTest");

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText = line;
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }
        [TestMethod]
        public void testSingleQuery()
        {
            runQuery(44);
        }
        [TestMethod]
        public void testAllQueries()
        {
            int count = 0;
            try
            {
                for (; count < 64; count++)
                {
                    runQuery(count);
                    count++;
                }
            } catch(Exception e)
            {
                Console.WriteLine("query" + count + " throw Exception");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                throw e;
            }

        }

        [TestMethod]
        public void preProcessTheQueries()
        {
            int counter = 0;
            string line;
            // Read the file and display it line by line.
            System.IO.StreamReader inFile =
               new System.IO.StreamReader("D:\\project\\GraphView_11_29\\DocDB-merge2\\gremlin.tsv");
            while ((line = inFile.ReadLine()) != null)
            {
                Console.WriteLine(line);
                var array = line.Split('\t');
                var processedQuery = formatQueryStr(array[0]);
                System.IO.StreamWriter outFile = new System.IO.StreamWriter(@"D:\\project\\GraphView_11_29\\DocDB-merge2\\query_processed\\" + counter + ".txt", false);
                outFile.WriteLine(processedQuery);
                outFile.Close();
                counter++;
            }
            inFile.Close();
            
        }

        public String formatQueryStr(String query)
        {
            String result = "";
            query = query.Replace("\"", "\'");
            query = query.Replace("[", "");
            query = query.Replace("]", "");
            query = query.Replace("bothE", "BothE");
            result = query;
            return result;
        }
    }
}
