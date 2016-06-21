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
            GraphViewCommand gcmd = new GraphViewCommand();
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
            GraphViewCommand gcmd = new GraphViewCommand();
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
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.CommandText = @"INSERT INTO Edge (eid)
                              SELECT n1, n2, 999
                              FROM Node n1, Node n2
                              where n1.firstName = 'Jane' AND n2.firstName = 'John'";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void DeleteEdge()
        {
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.CommandText = @"
                DELETE EDGE [A]-[Edge as e]->[C]
                FROM Node A, Node C
                WHERE A.firstName = 'Jane' AND C.firstName = 'John' ";

            gcmd.ExecuteNonQuery();
        }

        [TestMethod]
        public void Selectall()
        {
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.CommandText = @"
                Select *
            ";

            gcmd.ExecuteNonQuery();
        }
    }
}
