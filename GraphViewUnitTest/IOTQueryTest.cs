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
            runQuery(8);
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
