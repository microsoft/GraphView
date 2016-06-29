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
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                Select *
            ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void Selecttest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSix");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT e1.type FROM node AS father, node AS son MATCH son-[Edge AS e1]->father WHERE e1.type = 'father'
            ";

            gcmd.ExecuteNonQuery();
        }
    }
}
