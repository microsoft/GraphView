using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using GraphView;

namespace GraphViewUnitTest
{
    [TestClass]
    public class DocDbInsertTest
    {
        [TestMethod]
        public void InsertNode()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"INSERT INTO Node (uid, firstName, lastName, age) VALUES (101, 'Jane', 'Doe', 22);";
            gcmd.ExecuteNonQuery();

            gcmd.CommandText = @"INSERT INTO Node (uid, firstName, lastName, age) VALUES (102, 'John', 'Doe', 25);";
            gcmd.ExecuteNonQuery();

            gcmd.CommandText = @"INSERT INTO Node (uid, name) VALUES (103, 'jeffchen');";
            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteNode()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"DELETE FROM Node WHERE Node.firstName = 'Jane' AND Node.lastName = 'Doe';";
            gcmd.ExecuteNonQuery();

            gcmd.CommandText = @"DELETE FROM Node WHERE Node.firstName = 'John' AND Node.lastName = 'Doe';";
            gcmd.ExecuteNonQuery();

            gcmd.CommandText = @"DELETE FROM Node WHERE Node.name =  'jeffchen';";
            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void InsertEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"INSERT INTO Edge (eid)
                              SELECT n1, n2, 999
                              FROM Node n1, Node n2
                              where n1.firstName = 'Jane' AND n2.firstName = 'John'";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                DELETE EDGE [A]-[Edge as e]->[C]
                FROM Node A, Node C
                WHERE A.firstName = 'Jane' AND C.firstName = 'John' ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void Selectall()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSeven");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                Select 
            ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DocDBLinearSelectTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSeven");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT grandfather.name, father.name, son.name FROM node AS father, node AS son, node AS grandfather MATCH son-[Edge AS e1]->father-[Edge AS e2]->grandfather 
 WHERE grandfather.name = 'saturn' AND e1.type = 'father' AND e2.type = 'father'";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DocDBBranchSelectTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSeven");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT father.name, mother.name FROM node AS hercules, node AS father, node AS mother 
 MATCH hercules-[Edge AS e]->father, hercules-[Edge AS f]->mother 
 WHERE hercules.name = 'hercules' AND e.type = 'father' AND f.type ='mother' ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DocDBEdgeSelectTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSeven");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n1.name, e.reason, n2.name FROM node AS n1, node AS n2
 MATCH n1-[Edge AS e]->n2";

            gcmd.ExecuteNonQuery();
        }
    }
}
