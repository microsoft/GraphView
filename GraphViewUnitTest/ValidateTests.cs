using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    /// <summary>
    /// ValidateTest tests the correctness of graphview using specific data and check the result is expected or not
    /// </summary>
    [TestClass]
    public class ValidateTests
    {
        /// <summary>
        /// Initializes with data generator
        /// </summary>
        public ValidateTests()
        {
            TestInitialization.InitValidateData();
        }
        [TestMethod]
        public void pathValidityTest()
        {
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                #region simple path test on [Node]
                var res = con.ExecuteReader(@"SELECT path.*
                    FROM Node as N1,Node as N2
                    MATCH [N1]-[Edge* AS path]->[N2]
                    WHERE N1.id = 0");
                try
                {
                    int cnt = 0;
                    while (res.Read())
                        ++cnt;
                    int ans = 9;
                    if (cnt != ans)
                        Assert.Fail("There should be {0} matched path, but graphview found {1} path", ans, cnt);
                }
                finally
                {
                    res.Close();
                }
                #endregion

            }
        }
        [TestMethod]
        public void EdgeViewValidityTest()
        {
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                #region edge view test on [Device] and [Link]
                con.CreateEdgeView(@"
                    CREATE EDGE VIEW Device.Links AS
                    SELECT *
                    FROM Device.STARTDEVICE
                    UNION ALL
                    SELECT *
                    FROM Device.ENDDEVICE
                    ");
                System.Data.SqlClient.SqlDataReader res;
                for (int i = 0; i < ValidataData.DeviceNum; ++i)
                {
                    res = con.ExecuteReader(string.Format(@"SELECT Link.id from Link, Device
                                            MATCH Device-[Links]->Link WHERE Device.id = {0}", i));
                    try
                    {
                        int cnt = 0;
                        while (res.Read())
                        {
                            ++cnt;
                            int x = Convert.ToInt32(res["id"]);
                            if (2 * x % ValidataData.DeviceNum != i && 3 * x % ValidataData.DeviceNum != i)   //  Any Link #x could be linked to #2*x or #3*x device
                                Assert.Fail("The Link {0} have wrong device Linked", i);
                        }
                        if (cnt != 2)
                            Assert.Fail("The Link {0} doesn't have 2 device Linked!", i);
                    }
                    finally
                    {
                        res.Close();
                    }
                }
                #endregion
            }
        }
        [TestMethod]
        public void globalNodeViewValidityTest()
        {
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                #region simple path test on [globalNodeView]
                var res = con.ExecuteReader(@"SELECT path.*
                    FROM GlobalNodeView as N1,GlobalNodeView as N2
                    MATCH [N1]-[Edge*4..4 AS path]->[N2]
                    WHERE N1.id = 0");
                try
                {
                    int cnt = 0;
                    while (res.Read())
                    {
                        ++cnt;
                    }
                    int ans = 2;
                    if (cnt != ans)
                        Assert.Fail("There should be {0} matched path, but graphview found {1} path", ans, cnt);
                }
                finally
                {
                    res.Close();
                }

                #endregion

            }
        }
        [TestMethod]
        public void propertyOfNodeViewValidityTest()
        {
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                #region create node view and test correctness
                con.CreateNodeView(@"
                    CREATE NODE VIEW PeopleAndBook AS
                    SELECT *
                    FROM People
                    UNION ALL
                    SELECT *
                    FROM Book
                    ");
                var res = con.ExecuteReader("SELECT * FROM PeopleAndBook");
                try
                {
                    int cnt = 0;
                    while (res.Read())
                    {
                        ++cnt;
                        int[] ids = new int[] { 0, 1, 2, 1000, 1001, 1002 };
                        string[] names = new string[] { "Alice", "Bob", "Cindy", "Alice reads this book", "Bob reads this book", "Cindy reads this book" };
                        bool find = false;

                        for (int i = 0; i < ids.Length ; ++i)
                        {
                            if (res["id"].Equals(ids[i]))
                            {
                                if (!res["name"].Equals(names[i]))
                                    Assert.Fail("Id({0}) should have name({1}), but graphview outputs name({2})", ids[i], names[i], res["name"]);
                                find = true;
                            }
                        }
                        if(!find)
                            Assert.Fail("graphview outputs unknow id({0})", res["id"]);
                    }
                    int ans = 6;
                    if (cnt != ans)
                        Assert.Fail("There should be {0} matched path, but graphview found {1} path", ans, cnt);
                }
                finally
                {
                    res.Close();
                }
                #endregion
            }
        }

        [TestMethod]
        public void attributeOfEdgeViewValidityTest()
        {
            using (var con = new GraphView.GraphViewConnection(TestInitialization.ConnectionString))
            {
                con.Open();
                #region create edge view and test correctness
                con.CreateEdgeView(@"
                    CREATE EDGE VIEW Writer.RW AS
                    SELECT *
                    FROM Writer.reads
                    UNION ALL
                    SELECT *
                    FROM Writer.writes
                    ");
                var res = con.ExecuteReader("SELECT path.* FROM Writer, Book MATCH Writer-[RW* as path]->Book");
                try
                {
                    int cnt = 0;
                    string[] resList = new string[] { @"[{""NodeType"":""writer"", ""Id"":""0""}, {""EdgeType"":""reads"", ""Attribute"":{""comments"":""good"",""times_string"":""Never"",""times_int"":null}}, {""NodeType"":""Book"", ""Id"":""1000""}]",
@"[{""NodeType"":""writer"", ""Id"":""0""}, {""EdgeType"":""writes"", ""Attribute"":{""comments"":""not good"",""times_string"":null,""times_int"":4}}, {""NodeType"":""Book"", ""Id"":""1000""}]",
@"[{""NodeType"":""writer"", ""Id"":""1""}, {""EdgeType"":""reads"", ""Attribute"":{""comments"":""bad"",""times_string"":""Twice"",""times_int"":null}}, {""NodeType"":""Book"", ""Id"":""1001""}]",
@"[{""NodeType"":""writer"", ""Id"":""1""}, {""EdgeType"":""writes"", ""Attribute"":{""comments"":""not bad"",""times_string"":null,""times_int"":5}}, {""NodeType"":""Book"", ""Id"":""1001""}]",
@"[{""NodeType"":""writer"", ""Id"":""2""}, {""EdgeType"":""reads"", ""Attribute"":{""comments"":""soso"",""times_string"":""Once"",""times_int"":null}}, {""NodeType"":""Book"", ""Id"":""1002""}]",
@"[{""NodeType"":""writer"", ""Id"":""2""}, {""EdgeType"":""writes"", ""Attribute"":{""comments"":""hehe"",""times_string"":null,""times_int"":6}}, {""NodeType"":""Book"", ""Id"":""1002""}]"
 };
                    while (res.Read())
                    {
                        ++cnt;
                        bool ok = false;
                        for (int i = 0; i < resList.Length; ++i)
                            if (res[0].ToString().Equals(resList[i]))
                                ok = true;
                        if (!ok)
                            Assert.Fail("There is an unknow result: " + res[0]);
                    }
                    int ans = 6;
                    if (cnt != ans)
                        Assert.Fail("There should be {0} matched path, but graphview found {1} path", ans, cnt);
                }
                finally
                {
                    res.Close();
                }
                #endregion
            }
        }
    }
}
