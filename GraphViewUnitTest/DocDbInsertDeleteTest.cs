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

            connection.SetupClient();

            gcmd.CommandText = @"
                INSERT INTO Node (name, age, type) VALUES ('saturn', 10000, 'titan');
                INSERT INTO Node (name, age, type) VALUES ('jupiter', 5000, 'god');
                INSERT INTO Node (name, type) VALUES ('sky', 'location');
";
            gcmd.ExecuteNonQuery();

            //connection.ResetCollection();
        }

        [TestMethod]
        public void InsertNodeWithoutDeleteCollection(bool flag = true)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            connection.SetupClient();

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

            connection.SetupClient();
            
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

            connection.SetupClient();

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

            connection.SetupClient();

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

            connection.SetupClient();

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

            //connection.ResetCollection();
        }

        [TestMethod]
        public void InsertBigGraphWithoutDeleteCollection(bool flag = true)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

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

        [TestMethod]
        public void ResetCollection()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");
            connection.SetupClient();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);

            connection.ResetCollection();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }
    }
}
