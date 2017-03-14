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
        [TestMethod]
        public void ProjectTest()
        {
            //          GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //"GroupMatch", "MarvelTest");
            //          GraphViewCommand cmd = new GraphViewCommand(connection);
            //          //cmd.CommandText = "g.V().project('c', 'u').by('|provisioning').by('|provisioning').where('c', gt('u'))";
            //          //cmd.CommandText = "g.V().project('c').by('|provisioning')";
            //          cmd.CommandText = "g.V().has('weapon', 'lasso').as('character').out('appeared').as('comicbook').select('comicbook').next()";
            //          cmd.OutputFormat = OutputFormat.GraphSON;
            //          var results = cmd.Execute();
            //          foreach (var result in results)
            //          {
            //              Console.WriteLine(result);
            //          }

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
            runQuery(36);
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
            //query = query.Replace("gt(", "Predicate.gt(");
            result = query;
            return result;
        }
    }
}
