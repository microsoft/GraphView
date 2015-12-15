using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewTutorial
{
    public static class TutorialConfiguration
    {
        #region You should set up connection settings before running tutorial
        public static bool localTest = true;
        //  Parameters for local connection
        private static string yourSQLServerName = @"(local)\MS15";
        private static string yourDBName = "graphViewTest";
        //  Parameters for Azure connection
        private static string yourAzureSQLAddress = "graphview.database.windows.net";
        private static string yourAzureSQLDBName = "graphViewTest";
        private static string yourAzureUserId = "xxx";
        private static string yourAzurePassword = "xxx";
        #endregion
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
    }
}
