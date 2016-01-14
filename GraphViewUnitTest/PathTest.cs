using System;
using System.Collections.Generic;
using System.IO;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewUnitTest
{
    [TestClass]
    public class PathTest
    {
        [TestMethod]
        public void ParsePathTest()
        {
            string query = @"
                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues* as a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues*0..5 AS a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues*0 .. 5 as a]->[E2];
                    
                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues * 0 .. 5 as a]->[E2];

                    SELECT *
                    FROM EmployeeNode as E1,EmployeeNode as E2
                    MATCH [E1]-[Colleagues * 0 .. 5]->[E2]";

            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var stat = parser.Parse(new StringReader(query), out errors);
        }
    }
}
