// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class TableSizeEstimator
    {
        public SqlTransaction Tx { get; private set; }

        public TableSizeEstimator(SqlTransaction tx)
        {
            this.Tx = tx;
        }

        /// <summary>
        /// get estimated execution plan of Sql query
        /// </summary>
        private string GetEstimatedPlanXml(string sqlStr)
        {
            string xml = "";
            //Set showplan
            try
            {
                using (var cmdSetShowPlanXml = Tx.Connection.CreateCommand())
                {
                    cmdSetShowPlanXml.Transaction = Tx;
                    cmdSetShowPlanXml.CommandText = "SET SHOWPLAN_XML ON";
                    cmdSetShowPlanXml.ExecuteNonQuery();
                }

                //Run input SQL
                using (var cmdInput = Tx.Connection.CreateCommand())
                {
                    cmdInput.CommandText = sqlStr;
                    cmdInput.Transaction = Tx;

                    var adapter = new SqlDataAdapter();
                    var dataSet = new DataSet { Locale = CultureInfo.CurrentCulture };

                    adapter.SelectCommand = cmdInput;
                    dataSet.Tables.Add(new DataTable("Results") { Locale = CultureInfo.CurrentCulture });
                    adapter.Fill(dataSet, "Results");
                    dataSet.Tables[0].BeginLoadData();
                    dataSet.Tables[0].EndLoadData();

                    //XML is in 1st Col of 1st Row of 1st Table
                    xml = dataSet.Tables[0].Rows[0][0].ToString();
                    xml = Regex.Replace(xml, "<ShowPlanXML.*?>", "<ShowPlanXML>");
                }

                using (var cmdSetShowPlanXml = Tx.Connection.CreateCommand())
                {
                    cmdSetShowPlanXml.Transaction = Tx;
                    cmdSetShowPlanXml.CommandText = "SET SHOWPLAN_XML OFF";
                    cmdSetShowPlanXml.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Tx.Rollback();
                throw new QueryCompilationException("Cannot obtain estimated execution plan from the SQL database",e);
            }

            return xml;
        }


        /// <summary>
        /// DEPRECATED. Returns number of estimated rows for given query
        /// </summary>
        /// <param name="sqlStr">the query to be estimated.</param>
        /// <returns>
        /// Number of estimated rows
        /// </returns>
        internal double GetStatementEstimatedRows(string sqlStr)
        {
            var xml = GetEstimatedPlanXml(sqlStr);

            var root = XElement.Parse(xml);
            return Convert.ToDouble(root.Descendants("StmtSimple").First().Attribute("StatementEstRows").Value,CultureInfo.CurrentCulture);
        }

        internal Dictionary<string, double> GetQueryTableEstimatedRows(string sqlStr)
        {
            var xml = GetEstimatedPlanXml(sqlStr);
            var root = XElement.Parse(xml);

            var tables =
                from e in root.Descendants("RelOp")
                where e.Elements().Any(e2 => e2.Name.LocalName == "TableScan" || e2.Name.LocalName == "IndexScan")
                select e;

            var ret = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in tables)
            {
                QuoteType quote;
                var obj = t.Descendants("Object").First();
                var exposedName = obj.Attribute("Alias") ?? obj.Attribute("Table");

                ret.Add(Identifier.DecodeIdentifier(exposedName.Value, out quote),
                    Convert.ToDouble(t.Attribute("EstimateRows").Value, CultureInfo.CurrentCulture));
            }

            return ret;
        }

        internal List<double> GetUnionQueryTableEstimatedRows(string sqlStr)
        {
            var xml = GetEstimatedPlanXml(sqlStr);
            var root = XElement.Parse(xml);
            var res = new List<double>();
            foreach (var element in root.Descendants("Concat").First().Elements("RelOp"))
            {
                if (element.Attribute("PhysicalOp").Value == "Concatenation")
                {
                    var xElement = element.Element("Concat");
                    if (xElement != null)
                        res.AddRange(from sElement in xElement.Elements("RelOp")
                            select Convert.ToDouble(sElement.Attribute("EstimateRows").Value));
                }
                else
                    res.Add(Convert.ToDouble(element.Attribute("EstimateRows").Value));
            }
            return res;
        }

        internal int GetTableRowCount(string tableSchema, string tableName)
        {
            var tableRowCount = 0;
            const string sqlStr = @"
            SELECT
                p.[Rows]
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN sys.schemas s
                ON t.schema_id = s.schema_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN 
                sys.allocation_units a ON p.partition_id = a.container_id
            WHERE 
                i.OBJECT_ID > 255 AND   
                i.index_id <= 1 AND
                s.name = @tableSchema AND
                t.name = @tableName
            GROUP BY 
                s.NAME, t.NAME, i.object_id, i.index_id, i.name, p.[Rows]
            ";
            using (var command = Tx.Connection.CreateCommand())
            {
                command.Transaction = Tx;
                command.CommandText = sqlStr;
                command.Parameters.AddWithValue("@tableSchema", tableSchema);
                command.Parameters.AddWithValue("@tableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                        tableRowCount = Convert.ToInt32(reader["Rows"], CultureInfo.CurrentCulture);
                }
            }
            return tableRowCount;
        }
    }
}
