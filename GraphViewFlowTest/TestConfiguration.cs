using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    public static class TestConfiguration
    {
        #region You should set up connection settings before running testcases
        public static bool localTest = true;
        //  Parameters for local connection
        private static string yourSQLServerName = @"(local)\MS15";
        private static string yourDBName = "graphViewTest";
        //  Parameters for Azure connection
        private static string yourAzureSQLAddress = "xxx";
        private static string yourAzureSQLDBName = "xxx";
        private static string yourAzureUserId = "xxx";
        private static string yourAzurePassword = "xxx";
        #endregion

        #region You should set up data file locations before running testcases
        public static string NodeDataFileLocation = @"D:\filesForGraphview\data\apat63_99_new.txt";
        public static string EdgeDataFileLocation = @"D:\filesForGraphview\data\cite75_99_new.txt";
        #endregion

        public static string TestCaseNodeTableName { get { return "Patent_NT"; } }
        public static string TestCaseNodeIdName { get { return "patentid"; } }
        public static string TestCaseEdgeName { get { return "adjacencyList"; } }

        /// <summary>
        /// Returns the string for graphviewConnection
        /// </summary>
        public static string getConnectionString() 
        {
            if (localTest)
                return "Data Source=" + yourSQLServerName
                    + ";Initial Catalog=" + yourDBName
                    + ";Integrated Security=True;Connect Timeout=3000;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False;Max Pool Size=300";
            else
                return "Server=tcp:" + yourAzureSQLAddress
                    + ";Database=" + yourAzureSQLDBName
                    + ";User ID=" + yourAzureUserId
                    + ";Password= " + yourAzurePassword
                    + ";Trusted_Connection=False;Encrypt=True;Max Pool Size=3000;Connect Timeout=0";
        }
        /// <summary>
        /// Returns the Create table statement for GraphViewFlowTest
        /// </summary>
        public static string getCreateTableString()
        {
            return string.Format(@"
                CREATE TABLE [{0}] (
                    [ColumnRole: ""NodeId""]
                    {1} INT ,
                    [ColumnRole: ""Property""]
                    gyear INT,
                    [ColumnRole: ""Property""]
                    gdate INT,
                    [ColumnRole: ""Property""]
                    ayear INT,
                    [ColumnRole: ""Property""]
                    country VARCHAR(10),
                    [ColumnRole: ""Property""]
                    postate VARCHAR(10),
                    [ColumnRole: ""Property""]
                    assignee INT,
                    [ColumnRole: ""Property""]
                    asscode INT,
                    [ColumnRole: ""Property""]
                    claims INT,
                    [ColumnRole: ""Property""]
                    nclass INT,
                    [ColumnRole: ""Property""]
                    cat INT,
                    [ColumnRole: ""Property""]
                    subcat INT,
                    [ColumnRole: ""Property""]
                    cmade INT,
                    [ColumnRole: ""Property""]
                    creceive INT,
                    [ColumnRole: ""Property""]
                    ratiocit DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    general DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    original DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    fwdaplag DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    bckgtlag DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    selfctub DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    selfctlb DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    secdupbd DECIMAL(12,5),
                    [ColumnRole: ""Property""]
                    secdlwbd DECIMAL(12,5),
                    [ColumnRole: ""Edge"", Reference: ""{0}""]
                    {2} varbinary(8000)
                )" , TestCaseNodeTableName, TestCaseNodeIdName,TestCaseEdgeName);
        }
    }
}
