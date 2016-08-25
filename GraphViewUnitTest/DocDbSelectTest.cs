using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewSelectTest
    {
        [TestMethod]
        public void Selectall()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GremlinTest");
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                Select A, B, C
                From Node A, Node B, Node C
                Match A-[edge as e]->B, B-[edge as f]->C
                Where A.id = C.id
            ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
                var rc = reader;
            }


            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBLinearSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT grandfather.name, father.name, son.name FROM node AS father, node AS son, node AS grandfather MATCH son-[Edge AS e1]->father-[Edge AS e2]->grandfather 
                WHERE grandfather.name = 'saturn' AND e1.type = 'father' AND e2.type = 'father'";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
            }

            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBBranchSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GroupbyTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                 SELECT father.name, mother.name FROM node AS hercules, node AS father, node AS mother 
                 MATCH hercules-[Edge AS e]->father, hercules-[Edge AS f]->mother 
                 WHERE hercules.name = 'hercules' AND e.type = 'father' AND f.type ='mother' ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
                var x = reader;
            }
        }

        [TestMethod]
        public void DocDBEdgeSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n1.name, e.reason, n2.name FROM node AS n1, node AS n2
                MATCH n1-[Edge AS e]->n2";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
            }

            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBTriangleSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n1.name, n2.name
                FROM node n1, node n2,node n3
                MATCH n1-[Edge AS e1]->n2,
                      n3-[Edge AS e2]->n2,
                      n1-[Edge AS e3]->n3
            ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
            }

            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBTwoTriangleSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n1.name, n2.name, n3.name, n4.name
                FROM node n1, node n2,node n3, node n4,node n5, node n6
                MATCH n1-[Edge AS e1]->n2,
                      n3-[Edge AS e2]->n2,
                      n1-[Edge AS e3]->n3,
                      n4-[Edge AS e4]->n5,
                      n5-[Edge AS e5]->n6,
                      n6-[Edge AS e6]->n4
            ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
            }

            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBSquareSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n1.name, n2.name, n3.name, n4.name
                FROM node n1, node n2,node n3, node n4
                MATCH n1-[Edge AS e1]->n2,
                      n2-[Edge AS e2]->n3,
                      n3-[Edge AS e3]->n4,
                      n4-[Edge AS e4]->n1
            ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
                var rc = reader;
            }

           connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBNoResonTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphSeven");

            connection.SetupClient();
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n2.name, n3.name
                FROM node n1, node n2,node n3
                MATCH n1-[Edge AS e1]->n2,
                      n2-[Edge AS e2]->n3
                WHERE n1.name = 'pluto' AND e1.type = 'brother' AND e2.type = 'lives'        
            ";

            var reader = gcmd.ExecuteReader();
            while (reader.Read())
            {
                var rc = reader;
            }

            connection.ResetCollection();
        }
        [TestMethod]
        public void DocDBSelectDocTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GremlinTest");

            connection.SetupClient();
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                SELECT n2.name, n3.name, n1.doc
                FROM node n1, node n2,node n3
                MATCH n1-[Edge AS e1]->n2,
                      n2-[Edge AS e2]->n3
                WHERE n1.name = 'pluto' AND e1.type = 'brother' AND e2.type = 'lives'        
            ";

            var reader = gcmd.ExecuteReader();

            while (reader.Read())
            {
                var rc = reader;
            }
        }

        [TestMethod]
        public void InsertBigGraphWithoutDeleteCollection(bool flag = true)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GremlinTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            connection.SetupClient();

            gcmd.CommandText = @"
                INSERT INTO Node (name, age, type) VALUES ('saturn', 10000, 'titan');
                INSERT INTO Node (name, type) VALUES ('sky', 'location');
                INSERT INTO Node (name, type) VALUES ('sea', 'location');
                INSERT INTO Node (name, age, type) VALUES ('jupiter', 5000, 'god');
                INSERT INTO Node (name, age, type) VALUES ('neptune', 4500, 'god');
                INSERT INTO Node (name, age, type) VALUES ('hercules', 30, 'demigod');
                INSERT INTO Node (name, age, type) VALUES ('alcmene', 45, 'human');
                INSERT INTO Node (name, age, type) VALUES ('pluto', 4000, 'god');
                INSERT INTO Node (name, type) VALUES ('nemean', 'monster');
                INSERT INTO Node (name, type) VALUES ('hydra', 'monster');
                INSERT INTO Node (name, type) VALUES ('cerberus', 'monster');
                INSERT INTO Node (name, type) VALUES ('tartarus', 'location');

                INSERT INTO Edge (type)
                SELECT A,B,'father'
                FROM Node A, Node B
                WHERE A.name = 'jupiter' AND B.name = 'saturn'

                INSERT INTO Edge (type,reason)
                SELECT A,B,'lives','loves fresh breezes'
                FROM Node A, Node B
                WHERE A.name = 'jupiter' AND B.name = 'sky'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'jupiter' AND B.name = 'neptune'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'jupiter' AND B.name = 'pluto'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'pluto' AND B.name = 'neptune'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'pluto' AND B.name = 'jupiter'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'neptune' AND B.name = 'jupiter'

                INSERT INTO Edge (type)
                SELECT A,B,'brother'
                FROM Node A, Node B
                WHERE A.name = 'neptune' AND B.name = 'pluto'

                INSERT INTO Edge (type,reason)
                SELECT A,B,'lives','loves waves'
                FROM Node A, Node B
                WHERE A.name = 'neptune' AND B.name = 'sea'

                INSERT INTO Edge (type)
                SELECT A,B,'father'
                FROM Node A, Node B
                WHERE A.name = 'hercules' AND B.name = 'jupiter'

                INSERT INTO Edge (type)
                SELECT A,B,'mother'
                FROM Node A, Node B
                WHERE A.name = 'hercules' AND B.name = 'alcmene'

                INSERT INTO Edge (type,reason)
                SELECT A,B,'lives','no fear of death'
                FROM Node A, Node B
                WHERE A.name = 'pluto' AND B.name = 'tartarus'

                INSERT INTO Edge (type)
                SELECT A,B,'lives'
                FROM Node A, Node B
                WHERE A.name = 'cerberus' AND B.name = 'tartarus'

                INSERT INTO Edge (type)
                SELECT A,B,'pet'
                FROM Node A, Node B
                WHERE A.name = 'pluto' AND B.name = 'cerberus'

                INSERT INTO Edge (type,time,place_x,place_y)
                SELECT A,B,'battled',1,38.1,23.7
                FROM Node A, Node B
                WHERE A.name = 'hercules' AND B.name = 'nemean'

                INSERT INTO Edge (type,time,place_x,place_y)
                SELECT A,B,'battled',2,37.7,23.9
                FROM Node A, Node B
                WHERE A.name = 'hercules' AND B.name = 'hydra'

                INSERT INTO Edge (type,time,place_x,place_y)
                SELECT A,B,'battled',12,39,22
                FROM Node A, Node B
                WHERE A.name = 'hercules' AND B.name = 'cerberus'
";

            gcmd.ExecuteNonQuery();

        }
    }
}
