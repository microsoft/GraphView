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
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                INSERT INTO Node (name, age, type) VALUES ('saturn', 10000, 'titan');
                INSERT INTO Node (name, age, type) VALUES ('jupiter', 5000, 'god');
                INSERT INTO Node (name, type) VALUES ('sky', 'location');
            ";
            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
        }

        [TestMethod]
        public void InsertNodeWithoutDeleteCollection(bool flag = true)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                INSERT INTO Node (name, age, type) VALUES ('saturn', 10000, 'titan');
                INSERT INTO Node (name, age, type) VALUES ('jupiter', 5000, 'god');
                INSERT INTO Node (name, type) VALUES ('sky', 'location');
            ";
            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteNode()
        {
            InsertNodeWithoutDeleteCollection();

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                    DELETE FROM Node
                    WHERE  Node.name = 'saturn'
            ";
            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
        }

        [TestMethod]
        public void InsertEdge()
        {
            InsertNodeWithoutDeleteCollection();

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                INSERT INTO Edge (type, reason)
                SELECT A, B, 'lives', 'loves fresh breezes'
                FROM   Node A, Node B
                WHERE  A.name = 'jupiter' AND B.name = 'sky'
            ";

            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
        }

        [TestMethod]
        public void InsertEdgeWithDeleteCollection(bool flag = true)
        {
            InsertNodeWithoutDeleteCollection();

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                INSERT INTO Edge (type, reason)
                SELECT A, B, 'lives', 'loves fresh breezes'
                FROM   Node A, Node B
                WHERE  A.name = 'jupiter' AND B.name = 'sky'
            ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteEdge()
        {
            InsertEdgeWithDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                DELETE EDGE [D]-[Edge as e]->[A]
                FROM   Node D, Node A
                WHERE  D.name = 'jupiter' AND A.name = 'sky'
";

            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
        }

        [TestMethod]
        public void InsertBigGraph()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

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

            connection.ResetCollection();
        }

        [TestMethod]
        public void InsertBigGraphWithoutDeleteCollection(bool flag = true)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

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

        [TestMethod]
        public void Selectall()
        {
            InsertBigGraphWithoutDeleteCollection();   
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                Select A
                From Node A
            ";

            gcmd.ExecuteNonQuery();

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

            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
        }

        [TestMethod]
        public void DocDBBranchSelectTest()
        {
            InsertBigGraphWithoutDeleteCollection();
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            gcmd.CommandText = @"
                 SELECT father.name, mother.name FROM node AS hercules, node AS father, node AS mother 
                 MATCH hercules-[Edge AS e]->father, hercules-[Edge AS f]->mother 
                 WHERE hercules.name = 'hercules' AND e.type = 'father' AND f.type ='mother' ";

            gcmd.ExecuteNonQuery();

            connection.ResetCollection();
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

            gcmd.ExecuteNonQuery();
        }
    }
}
