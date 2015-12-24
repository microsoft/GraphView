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
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Xml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualBasic.FileIO;

namespace GraphView
{
    public partial class GraphViewConnection : IDisposable
    {
        private static int _rep = 0;

        private static string RandomString(int strLength = 10)
        {
#if DEBUG
            //return "DEBUG";
#endif
            var rndStr = "";
            var tick = (int)DateTime.Now.Ticks + _rep++;
            var rd = new Random(tick);

            for (var i = 0; i < strLength; ++i)
            {
                var rndChr = (char)rd.Next(65, 90);
                rndStr = rndStr + rndChr;
            }
            return rndStr;
        }

        private class IndexOrConstraintInformationRecord
        {
            public string name;
            public string cluseterType;
            public bool isUnique;
            public bool isPrimaryKey;
            public int isConstraint; //0 index(default), 1 unique constraint
            public IList<string> indexColumns;
            public bool isExecuteSuccess;
        }

        private static object ConvertStringIntoSqlType(string obj, string sqlDataTypeName)
        {
            switch (sqlDataTypeName.ToLower())
            {
                case "char":
                case "nchar":
                case "text":
                case "ntext":
                case "nvarchar":
                case "varchar":
                    return new SqlString(obj);
            }
            if (string.IsNullOrWhiteSpace(obj))
            {
                return null;
            }
            switch (sqlDataTypeName.ToLower())
            {
                case "int":
                    return SqlInt32.Parse(obj);

                case "numeric":
                case "decimal":
                    return SqlDecimal.Parse(obj);

                case "float":
                    return SqlDouble.Parse(obj);

                case "binary":
                case "image":
                case "timestamp":
                case "varbinary":
                    var encoding = new ASCIIEncoding();
                    return new SqlBytes(encoding.GetBytes(obj));

                case "bit":
                    return SqlBoolean.Parse(obj);

                case "tinyint":
                    return SqlByte.Parse(obj);

                case "datetime":
                case "smalldatetime":
                    return SqlDateTime.Parse(obj);

                case "uniqueidentifier":
                    return SqlGuid.Parse(obj);

                case "smallint":
                    return SqlInt16.Parse(obj);

                case "bigint":
                    return SqlInt64.Parse(obj);

                case "money":
                case "smallmoney":
                    return SqlMoney.Parse(obj);

                case "real":
                    return SqlSingle.Parse(obj);

                case "xml":
                    var objData =
                        new SqlXml(new System.Xml.XmlTextReader(obj, System.Xml.XmlNodeType.Document, null));
                    return objData;

                default:
                    throw new Exception();
            }
        }

        private sealed class BulkInsertFileDataReader : IDataReader
        {
            private int _countRow;
            private string[] _value;
            private readonly Dictionary<string, int> _columnIndex;
            private readonly IList<string> _dataColumn;
            private readonly IList<string> _dataType;
            private readonly string _rowTerminator;
            private readonly string _fieldTerminator;
            private readonly StreamReader _streamReader;
            private readonly IList<int> _kmp;
            private readonly bool _skipHeader;
            private readonly int _labelColumn;
            private readonly string _labelName;

            private void PreKmp(string pattern, IList<int> kmp)
            {
                int m = pattern.Length;
                kmp.Add(-1);
                for (int i = 1; i < m; i++)
                {
                    int k = kmp[i - 1];
                    while (k >= 0)
                    {
                        if (pattern[k] == pattern[i - 1])
                        {
                            break;
                        }
                        else
                        {
                            k = kmp[k];
                        }

                    }
                    kmp.Add(k + 1);
                }
            }

            public BulkInsertFileDataReader(string fileName, string fieldTerminator,
                string rowTerminator, IList<string> datacolumn, IList<string> dataType, bool skipHeader = false,
                int labelColumn = -1, string labelName = null)
            {
                _streamReader = new StreamReader(fileName);
                _rowTerminator = rowTerminator;
                _fieldTerminator = fieldTerminator;
                _columnIndex = new Dictionary<string, int>();
                _dataType = dataType;
                _dataColumn = datacolumn;
                _countRow = 1;
                _skipHeader = skipHeader;
                _kmp = new List<int>();
                _labelColumn = labelColumn;
                _labelName = labelName;
                PreKmp(rowTerminator, _kmp);

                var count = 0;
                foreach (var it in datacolumn)
                {
                    _columnIndex.Add(it, count);
                    count++;
                }
            }

            public void Dispose()
            {
                _streamReader.Dispose();
            }

            public string GetName(int i)
            {
                return _dataColumn[i];
            }

            public string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public Type GetFieldType(int i)
            {
                throw new NotImplementedException();
            }

            public object GetValue(int i)
            {
                try
                {
                    return ConvertStringIntoSqlType(_value[i], _dataType[i]);
                }
                catch
                {
                    throw new BulkInsertNodeException(
                        string.Format("The data in row {0}, column {1} can't be converted into Type\"{2}\".",
                            _countRow - 1,
                            i + 1, _dataType[i]));
                }
            }

            public int GetValues(object[] values)
            {
                throw new NotImplementedException();
            }

            public int GetOrdinal(string name)
            {
                return _columnIndex[name];
            }

            public bool GetBoolean(int i)
            {
                throw new NotImplementedException();
            }

            public byte GetByte(int i)
            {
                throw new NotImplementedException();
            }

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public char GetChar(int i)
            {
                throw new NotImplementedException();
            }

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public Guid GetGuid(int i)
            {
                throw new NotImplementedException();
            }

            public short GetInt16(int i)
            {
                throw new NotImplementedException();
            }

            public int GetInt32(int i)
            {
                throw new NotImplementedException();
            }

            public long GetInt64(int i)
            {
                throw new NotImplementedException();
            }

            public float GetFloat(int i)
            {
                throw new NotImplementedException();
            }

            public double GetDouble(int i)
            {
                throw new NotImplementedException();
            }

            public string GetString(int i)
            {
                throw new NotImplementedException();
            }

            public decimal GetDecimal(int i)
            {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i)
            {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i)
            {
                throw new NotImplementedException();
            }

            public bool IsDBNull(int i)
            {
                throw new NotImplementedException();
            }

            public int FieldCount
            {
                get { return _dataType.Count; }
            }

            object IDataRecord.this[int i]
            {
                get { throw new NotImplementedException(); }
            }

            object IDataRecord.this[string name]
            {
                get { throw new NotImplementedException(); }
            }

            public void Close()
            {
                _streamReader.Close();
            }

            public DataTable GetSchemaTable()
            {
                throw new NotImplementedException();
            }

            public bool NextResult()
            {
                return false;
            }

            private bool getNextRow()
            {
                bool getRowTerminator = false;
                var builder = new StringBuilder();
                var ichar = _streamReader.Read();
                int rowTerminatorPosition = 0;

                while (ichar != -1)
                {
                    if (rowTerminatorPosition == -1)
                    {
                        builder.Append((char)ichar);
                        ichar = _streamReader.Read();
                        rowTerminatorPosition = 0;
                    }
                    else if (_rowTerminator[rowTerminatorPosition] == (char)ichar)
                    {
                        builder.Append((char)ichar);
                        rowTerminatorPosition++;
                        if (rowTerminatorPosition == _rowTerminator.Length)
                        {
                            getRowTerminator = true;
                            break;
                        }
                        ichar = _streamReader.Read();
                    }
                    else
                    {
                        rowTerminatorPosition = _kmp[rowTerminatorPosition];
                    }
                }
                if (getRowTerminator == false)
                {
                    if (builder.Length == 0)
                    {
                        return false;
                    }
                    else
                    {
                        throw new BulkInsertNodeException("Data file should be ended by a rowterminator.");
                    }
                }

                builder.Length -= _rowTerminator.Length;
                var rowstring = builder.ToString();
                var words = new string[1];
                words[0] = _fieldTerminator;
                _value = rowstring.Split(words, StringSplitOptions.None);
                if (_value == null || _value.Count() != FieldCount)
                {
                    if (_streamReader.Peek() == -1)
                    {
                        return false;
                    }
                    const string error = @"It should be {0} field(s) in the {1} row of data file";
                    throw new BulkInsertNodeException(string.Format(error, FieldCount, _countRow));
                }
                return true;
            }

            public bool Read()
            {
                if (!getNextRow())
                {
                    return false;
                }

                if ((_countRow == 1) && _skipHeader)
                {
                    if (!getNextRow())
                    {
                        return false;
                    }
                }
                if (_labelColumn != -1)
                {
                    while (GetValue(_labelColumn).ToString() != _labelName)
                    {
                        if (!getNextRow())
                        {
                            return false;
                        }
                    }
                }
                _countRow++;
                return true;
            }

            public int Depth
            {
                get { return 0; }
            }

            public bool IsClosed
            {
                get { return _streamReader.EndOfStream; }
            }

            public int RecordsAffected
            {
                get { return -1; }
            }

            public object this[int i]
            {
                get { return ConvertStringIntoSqlType(_value[i], _dataType[i]); }
            }
        }

        /// <summary>
        /// Bulk insert node from data file into node table. 
        /// </summary>
        /// <param name="dataFileName"> The name of data file.</param>
        /// <param name="tableName"> The table name of node table.</param>
        /// <param name="tableSchema"> The Schema name of node table. Default by "dbo".</param>
        /// <param name="dataColumnName"> User-supplied column(s) in data file in order.
        /// By default(null or empty), data file should exactly contain all the columns of property and nodeid in creating order.</param>
        /// <param name="fieldTerminator"> The field terminator of data file. Default by "\t".</param>
        /// <param name="rowTerminator"> The row terminator of data file. Default by "\r\n".</param>
        public void BulkInsertNode(string dataFileName, string tableName, string tableSchema = "dbo",
            IList<string> dataColumnName = null, string fieldTerminator = "\t", string rowTerminator = "\r\n", bool skipHeader = false)
        {
            var command = Conn.CreateCommand();
            command.CommandTimeout = 0;

            var transaction = Conn.BeginTransaction();
            command.Transaction = transaction;
            try
            {
                if (tableName == null)
                {
                    throw new BulkInsertNodeException("The string of table name is null.");
                }
                if (dataFileName == null)
                {
                    throw new BulkInsertNodeException("The string of data file name is null.");
                }
                if (!File.Exists(dataFileName))
                {
                    throw new BulkInsertNodeException(string.Format("Data file {0} doesn't exist.", dataFileName));
                }
                if (string.IsNullOrEmpty(tableSchema))
                {
                    tableSchema = "dbo";
                }
                if (string.IsNullOrEmpty(fieldTerminator))
                {
                    fieldTerminator = "\t";
                }
                if (string.IsNullOrEmpty(rowTerminator))
                {
                    rowTerminator = "\r\n";
                }

                command.Parameters.Clear();
                command.Parameters.Add("name", SqlDbType.NVarChar, 128);
                command.Parameters["name"].Value = tableName;
                command.Parameters.Add("schema", SqlDbType.NVarChar, 128);
                command.Parameters["schema"].Value = tableSchema;
                const string checkTableExist = @"
                select *
                from {0}
                where TableName  = @name and TableSchema = @schema";
                command.CommandText = string.Format(checkTableExist, MetadataTables[0]); //_NodeTableCollection
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new BulkInsertNodeException(
                            string.Format(
                                "There doesn't exist the table with tableSchema(\"{0}\") and tableName(\"{1}\")",
                                tableSchema, tableName));
                    }
                }

                //Get the column name and datatype in the node table for bulk insert.
                var columnDataType = new List<string>();
                const string findColumn = @"
                select GC.columnName, ISC.DATA_TYPE
                from {0} GC 
                join sys.columns C
                on GC.TableName = @name and GC.TableSchema = @schema and c.name = GC.ColumnName 
                join sys.tables T
                on C.object_id = T.object_id and GC.ColumnRole != 1 and T.name = GC.TableName
                join sys.schemas SC
                on SC.name = GC.TableSchema and SC.schema_id = T.schema_id
                join INFORMATION_SCHEMA.COLUMNS ISC
                on ISC.TABLE_SCHEMA = GC.TableSchema and ISC.TABLE_NAME = GC.TableName and ISC.COLUMN_NAME = GC.ColumnName
                order by C.column_id";
                command.CommandText = string.Format(findColumn, MetadataTables[1]); //_NodeTableColumnCollection

                if (dataColumnName == null || !dataColumnName.Any())
                {
                    if (dataColumnName == null)
                    {
                        dataColumnName = new List<string>();
                    }
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            dataColumnName.Add(reader["columnName"].ToString());
                            columnDataType.Add(reader["DATA_TYPE"].ToString());
                        }
                    }
                }
                else
                {
                    var columnNameMapping = new Dictionary<string, Tuple<string, string>>();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            columnNameMapping[reader["columnName"].ToString().ToLower()] =
                                Tuple.Create(reader["columnName"].ToString(), reader["DATA_TYPE"].ToString());
                        }
                    }
                    for (int i = 0; i < dataColumnName.Count; i++)
                    {
                        var it = dataColumnName[i];
                        if (columnNameMapping.ContainsKey(it.ToLower()))
                        {
                            columnDataType.Add(columnNameMapping[it.ToLower()].Item2);
                            dataColumnName[i] = columnNameMapping[it.ToLower()].Item1;
                        }
                        else
                        {
                            throw new BulkInsertNodeException(
                                string.Format("There doesn't exist a legal column \"{0}\" in the table \"{1}\".", it,
                                    tableName));
                        }
                    }
                }

                //Record information of indexes.
                var indexInfo = new List<IndexOrConstraintInformationRecord>();
                command.CommandText = @"
                select K.name indexname, K.index_id, K.type_desc, K.is_unique, K.is_primary_key, C.name referencename
                from sys.tables T
				join sys.schemas SC
				on SC.name = @schema and SC.schema_id = T.schema_id
				join sys.indexes K
                on T.object_id = K.object_id and T.name = @name 
				join sys.index_columns I
				on I.object_id = K.object_id and I.index_id = K.index_id
				join sys.columns C
				on I.object_id = C.object_id and I.column_id = C.column_id
				order by K.index_id, I.index_id";

                using (var reader = command.ExecuteReader())
                {
                    int indexId = -1;
                    while (reader.Read())
                    {
                        if (reader["indexname"].ToString() != "")
                        {
                            var columnIndexId = Convert.ToInt32(reader["index_id"].ToString());
                            if (indexId != columnIndexId)
                            {
                                indexInfo.Add(
                                    new IndexOrConstraintInformationRecord
                                    {
                                        name = reader["indexname"].ToString(),
                                        cluseterType = reader["type_desc"].ToString(),
                                        isUnique = (reader["is_unique"].ToString() == "True"),
                                        isPrimaryKey = (reader["is_primary_key"].ToString() == "True"),
                                        isConstraint = 0,
                                        indexColumns = new List<string>()
                                    });
                                indexId = columnIndexId;
                            }
                            indexInfo.Last().indexColumns.Add(reader["referencename"].ToString());
                        }
                    }
                    reader.Close();
                }

                command.CommandText = @"
                select KC.name 
                from sys.key_constraints KC
				join sys.schemas SC
				on SC.schema_id  = KC.schema_id and SC.name = @schema 
			    join sys.tables T
				on T.schema_id = SC.schema_id and T.name = @name and T.object_id  = KC.parent_object_id";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        foreach (var it in indexInfo)
                        {
                            var str = reader["name"].ToString();
                            if (str.ToLower() == it.name.ToLower())
                            {
                                it.isConstraint = 1;
                            }
                        }
                    }
                    reader.Close();
                }

                var tableNameWithSchema = string.Format("{0}.{1}", tableSchema, tableName);

                //Drop indexes and constraint.
                command.Parameters.Clear();
                const string dropIndexCommand = @"
                drop index {0} on {1} ";
                const string dropConstraintCommand = @"
                ALTER TABLE {0}
                DROP CONSTRAINT {1}";
                foreach (var it in indexInfo)
                {
                    if (it.isPrimaryKey || it.isConstraint != 0) //drop constraint
                    {
                        command.CommandText = string.Format(dropConstraintCommand, tableNameWithSchema, it.name);
                    }
                    else //drop index
                    {
                        command.CommandText = string.Format(dropIndexCommand, it.name, tableNameWithSchema);
                    }
                    try
                    {
                        command.ExecuteNonQuery();
                        it.isExecuteSuccess = true;
                    }
                    catch
                    {
                        it.isExecuteSuccess = false;
                    }
                }

                //Bulk insert
                using (var sqlBulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.TableLock, transaction))
                {
                    sqlBulkCopy.BulkCopyTimeout = 0;
                    using (var reader = new BulkInsertFileDataReader(dataFileName, fieldTerminator, rowTerminator, dataColumnName,
                        columnDataType, skipHeader))
                    {
                        foreach (var it in dataColumnName)
                        {
                          sqlBulkCopy.ColumnMappings.Add(it, it);
                        }
                        sqlBulkCopy.DestinationTableName = tableNameWithSchema;
                        sqlBulkCopy.WriteToServer(reader);
                    }
                }

                //Rebuild indexes or constraint.
                command.Parameters.Clear();
                const string buildUnique = @"
                ALTER TABLE {0}
                ADD constraint {1}
                {2}({3})";
                const string buildIndex = @"
                Create {0} {1} index {2}
                on {3}({4})";

                foreach (var it in indexInfo)
                {
                    if (it.isExecuteSuccess)
                    {
                        if (it.isPrimaryKey || it.isConstraint == 1) //primary key or unique constraint
                        {
                            var type = it.isPrimaryKey ? "Primary key" : "Unique";
                            command.CommandText = string.Format(buildUnique, tableNameWithSchema,
                                it.name, type, string.Join(",", it.indexColumns));
                        }
                        else if (it.isConstraint == 0) //index
                        {
                            var unique = it.isUnique ? "unique" : "";
                            command.CommandText = string.Format(buildIndex, unique, it.cluseterType,
                                it.name, tableNameWithSchema, string.Join(",", it.indexColumns));
                        }
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
            catch (Exception error)
            {
                transaction.Rollback();
                throw new BulkInsertNodeException(error.Message);
            }
        }

        /// <summary>
        /// Bulk insert edge from data file
        /// Data file should contain source node id and Sink node id on the first and second columns,
        /// then followed by the columns of user-supplied edge attribute.
        /// </summary>
        /// <param name="dataFileName"> The name of data file.</param>
        /// <param name="tableSchema"> The Schema name of node table. Default by "dbo".</param>
        /// <param name="sourceTableName"> The source node table name of node table.</param>
        /// <param name="sourceNodeIdName"> The node id name of source node table.</param>
        /// <param name="sinkTableName"> The Sink node table name of node table.</param>
        /// <param name="sinkNodeIdName"> The node id name of Sink node table.</param>
        /// <param name="edgeColumnName"> The edge column name in source node table.</param>
        /// <param name="dataEdgeAttributeName"> User-supplied edge attribute name in data file in order.
        /// By default(null), data file should exactly contain all the Edge Attribute in creating order.
        /// Empty stands for that data file doesn't provide edge attribute.</param>
        /// <param name="fieldTerminator"> The field terminator of data file. Default by "\t".</param>
        /// <param name="rowTerminator"> The row terminator of data file. Default by "\r\n".</param>
        /// <param name="updateMethod"> Choose update method or rebuild table method to implement bulk insert edge.
        /// <param name="Header"> True, the data file contains Header</param>
        public void BulkInsertEdge(string dataFileName, string tableSchema, string sourceTableName,
            string sourceNodeIdName, string sinkTableName, string sinkNodeIdName, string edgeColumnName,
            IList<string> dataEdgeAttributeName = null, string fieldTerminator = "\t", string rowTerminator = "\r\n",
            bool updateMethod = true, bool Header = false)
        {
            //Data types mapping from C# into sql and .Net.
            var typeDictionary = new Dictionary<string, Tuple<string, string>>
            {
                {"int", new Tuple<string, string>("int", "int")},
                {"long", new Tuple<string, string>("bigint", "bigint")},
                {"double", new Tuple<string, string>("float", "float")},
                {"string", new Tuple<string, string>("nvarchar(4000)", "nvarchar")}
            };

            //Check validity of input
            if (string.IsNullOrEmpty(sourceTableName))
            {
                throw new BulkInsertEdgeException("The string of source table name is null or empty.");
            }
            if (string.IsNullOrEmpty(sinkTableName))
            {
                throw new BulkInsertEdgeException("The string of Sink table name is null or empty.");
            }
            if (string.IsNullOrEmpty(dataFileName))
            {
                throw new BulkInsertEdgeException("The string of data file name is null or empty.");
            }
            if (string.IsNullOrEmpty(sourceNodeIdName))
            {
                throw new BulkInsertEdgeException("The string of source node Id name is null or empty.");
            }
            if (string.IsNullOrEmpty(sinkNodeIdName))
            {
                throw new BulkInsertEdgeException("The string of Sink node Id name is null or empty.");
            }
            if (!File.Exists(dataFileName))
            {
                throw new BulkInsertEdgeException(string.Format("Data file {0} doesn't exist.", dataFileName));
            }
            if (string.IsNullOrEmpty(tableSchema))
            {
                tableSchema = "dbo";
            }
            if (string.IsNullOrEmpty(fieldTerminator))
            {
                fieldTerminator = "\t";
            }
            if (string.IsNullOrEmpty(rowTerminator))
            {
                rowTerminator = "\r\n";
            }

            var transaction = Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.CommandTimeout = 0;
            command.Transaction = transaction;
            try
            {
                //Check validity of table name in GraphView(source and Sink node table)
                command.Parameters.Clear();
                const string checkNodeTableCollection = @"
                Select *
                From {0}
                Where (TableName = @tablename and TableSchema = @tableschema)";
                command.CommandText = string.Format(checkNodeTableCollection, MetadataTables[0]); //_NodeTableCollection
                command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters.Add("tablename", SqlDbType.NVarChar, 128);
                command.Parameters["tablename"].Value = sourceTableName;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new BulkInsertNodeException(
                            string.Format(
                                "There doesn't exist the table with tableSchema(\"{0}\") and tableName(\"{1}\")",
                                tableSchema, sourceTableName));
                    }
                }
                command.Parameters["tablename"].Value = sinkTableName;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new BulkInsertNodeException(
                            string.Format(
                                "There doesn't exist the table with tableSchema(\"{0}\") and tableName(\"{1}\")",
                                tableSchema, sinkTableName));
                    }
                }

                //Check validity of edge column name in GraphView
                command.Parameters.Clear();
                const string checkEdgeColumn = @"
                select *
                from {0}
                where (TableName = @tablename and TableSchema = @tableschema and ColumnName = @columnname and Reference = @reference)";
                command.CommandText = string.Format(checkEdgeColumn, MetadataTables[1]); //_NodeTableColumnCollection
                command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters.Add("tablename", SqlDbType.NVarChar, 128);
                command.Parameters["tablename"].Value = sourceTableName;
                command.Parameters.Add("columnname", SqlDbType.NVarChar, 128);
                command.Parameters["columnname"].Value = edgeColumnName;
                command.Parameters.Add("reference", SqlDbType.NVarChar, 128);
                command.Parameters["reference"].Value = sinkTableName;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new BulkInsertEdgeException(
                            string.Format("There doesn't exist edge column \"{0}\" or the edge is not from {1} to {2}",
                                edgeColumnName, sourceTableName, sinkTableName));
                    }
                }

                //Record edges' name and datatype in sql
                var userSuppliedEdgeAttributeInfo = new List<Tuple<string, string>>();
                var allEdgeAttributeNameInOrder = new List<string>(); //For UDF's variable reference

                //Get edges' name and datatype in sql
                command.Parameters.Clear();
                const string getEdgeAttribute = @"
                select AttributeName, AttributeType
                from {0} EGA
                where EGA.TableSchema = @tableschema and EGA.TableName = @tablename and EGA.ColumnName = @columnname
                order by EGA.AttributeEdgeId";

                command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters.Add("tablename", SqlDbType.NVarChar, 128);
                command.Parameters["tablename"].Value = sourceTableName;
                command.Parameters.Add("columnname", SqlDbType.NVarChar, 128);
                command.Parameters["columnname"].Value = edgeColumnName;
                command.CommandText = String.Format(getEdgeAttribute, MetadataTables[2]); //_EdgeAttributeCollection 
                if (dataEdgeAttributeName == null)
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var attributeName = reader["AttributeName"].ToString();
                            var attributeDatatype = reader["AttributeType"].ToString();
                            userSuppliedEdgeAttributeInfo.Add(Tuple.Create(attributeName, attributeDatatype.ToLower()));
                            allEdgeAttributeNameInOrder.Add(attributeName);
                        }
                    }
                }
                else
                {
                    var attributeMapping = new Dictionary<string, string>();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var attributeName = reader["AttributeName"].ToString().ToLower();
                            var propetyDatatype = reader["AttributeType"].ToString();
                            attributeMapping[attributeName] = propetyDatatype;
                            allEdgeAttributeNameInOrder.Add(attributeName);
                        }
                    }
                    foreach (var it in dataEdgeAttributeName)
                    {
                        if (attributeMapping.ContainsKey(it.ToLower()))
                        {
                            userSuppliedEdgeAttributeInfo.Add(Tuple.Create(it, attributeMapping[it.ToLower()].ToLower()));
                        }
                        else
                        {
                            throw new BulkInsertEdgeException(
                                string.Format("There doesn't exist edge attribute name \"{0}\"", it));
                        }
                    }
                }

                //Get the name and datatype of user-defined node id of source and Sink table
                command.Parameters.Clear();
                const string getNodeIdDataType = @"
                Select GC.ColumnName, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                From {0} GC
                Join sys.columns C
                On C.name = GC.ColumnName
                Join sys.tables T
                On C.object_id = T.object_id and T.name = GC.TableName
                Join sys.schemas SC
                On SC.name = GC.TableSchema and SC.schema_id = T.schema_id
                Join INFORMATION_SCHEMA.COLUMNS ISC
                On ISC.TABLE_SCHEMA = GC.TableSchema and ISC.TABLE_NAME = GC.TableName and ISC.COLUMN_NAME = GC.ColumnName
                Where GC.TableSchema = @tableschema and GC.TableName = @tablename and GC.ColumnRole = 2";

                Tuple<string, string, string, string> sourceNodeId; //Name,DataType,NameInTempTable,DataTypeLength
                command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters.Add("tablename", SqlDbType.NVarChar, 128);
                command.Parameters["tablename"].Value = sourceTableName;
                command.CommandText = String.Format(getNodeIdDataType, MetadataTables[1]);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        if (reader["ColumnName"].ToString().ToLower() != sourceNodeIdName.ToLower())
                        {
                            throw new BulkInsertEdgeException(
                                string.Format("\"{0}\" is not a node id name of source node table\"{1}\"",
                                    sourceNodeIdName, sourceTableName));
                        }
                        sourceNodeId = Tuple.Create(reader["ColumnName"].ToString(), reader["DATA_TYPE"].ToString(),
                            reader["ColumnName"] + "_src_" + RandomString(), reader["CHARACTER_MAXIMUM_LENGTH"].ToString());
                    }
                    else
                    {
                        throw new BulkInsertEdgeException(
                            string.Format("There doesn't exist a node id in source note table \"{0}\".", sourceTableName));
                    }
                }

                Tuple<string, string, string, string> sinkNodeId; //Name,DataType,NameInTempTable,DataTypeLength
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters["tablename"].Value = sinkTableName;
                command.CommandText = String.Format(getNodeIdDataType, MetadataTables[1]);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        if (reader["ColumnName"].ToString().ToLower() != sinkNodeIdName.ToLower())
                        {
                            throw new BulkInsertEdgeException(
                                string.Format("\"{0}\" is not a node id name of Sink node table\"{1}\"", sinkNodeIdName,
                                    sinkTableName));
                        }
                        sinkNodeId = Tuple.Create(reader["ColumnName"].ToString(), reader["DATA_TYPE"].ToString(),
                            reader["ColumnName"] + "_sink_" + RandomString(), reader["CHARACTER_MAXIMUM_LENGTH"].ToString());
                    }
                    else
                    {
                        throw new BulkInsertEdgeException(
                            string.Format("There doesn't exist a node id in Sink note table \"{0}\".", sinkTableName));
                    }
                }

                //Create temp table for bulk inserting edge data
                var randomTempTableName = tableSchema + "." + sourceTableName + edgeColumnName + sinkTableName + "_" +
                                          RandomString();
                var attributeNameArray =
                    userSuppliedEdgeAttributeInfo.Select(x => x.Item1.ToString() + " " + typeDictionary[x.Item2].Item1);
                const string createTempTable = @"
                Create table {0}
                (
                    {1},
                    {2},
                    {3}
                )";
                var maxLength = string.IsNullOrEmpty(sourceNodeId.Item4)
                    ? ""
                    : ("(" + (sourceNodeId.Item4 == "-1" ? "max" : sourceNodeId.Item4) + ")");
                var sourceNodeIdColumnInfo = sourceNodeId.Item3 + ' ' + sourceNodeId.Item2 + maxLength;
                maxLength = string.IsNullOrEmpty(sinkNodeId.Item4)
                    ? ""
                    : ("(" + (sinkNodeId.Item4 == "-1" ? "max" : sinkNodeId.Item4) + ")");
                var sinkNodeIdColumnInfo = sinkNodeId.Item3 + ' ' + sinkNodeId.Item2 + maxLength;
                command.CommandText = string.Format(createTempTable, randomTempTableName, sourceNodeIdColumnInfo,
                    sinkNodeIdColumnInfo, string.Join(",\n", attributeNameArray));
                command.ExecuteNonQuery();

                //Bulk insert
                var dataColumnName = new List<string>();
                var columnDataType = new List<string>();
                dataColumnName.Add(sourceNodeId.Item3);
                columnDataType.Add(sourceNodeId.Item2);

                dataColumnName.Add(sinkNodeId.Item3);
                columnDataType.Add(sinkNodeId.Item2);

                foreach (var it in userSuppliedEdgeAttributeInfo)
                {
                    dataColumnName.Add(it.Item1);
                    columnDataType.Add(typeDictionary[it.Item2.ToLower()].Item2);
                }


                using (var sqlBulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.TableLock, transaction))
                {
                    sqlBulkCopy.BulkCopyTimeout = 0;
                    using (var reader = new BulkInsertFileDataReader(dataFileName, fieldTerminator, rowTerminator,
                            dataColumnName,
                            columnDataType, Header))
                    {
                        foreach (var it in dataColumnName)
                        {
                            sqlBulkCopy.ColumnMappings.Add(it, it);
                        }
                        sqlBulkCopy.DestinationTableName = randomTempTableName;
                        sqlBulkCopy.WriteToServer(reader);
                    }
                }

                //Create clustered index on Sink node in temp table
                string clusteredIndexName = "sinkIndex_" + RandomString();
                const string createClusteredIndex = @"
                create clustered index [{0}] on {1}([{2}])";
                command.Parameters.Clear();
                command.CommandText = string.Format(createClusteredIndex, clusteredIndexName, randomTempTableName, sinkNodeId.Item3);
                command.ExecuteNonQuery();

                if (updateMethod)
                {
                    //Update database
                    var hashSetOfUserSuppliedEdgeAttribute =
                        new HashSet<string>(userSuppliedEdgeAttributeInfo.Select(x => x.Item1));
                    string aggregeteFunctionName = tableSchema + '_' + sourceTableName + '_' + edgeColumnName + '_' + "Encoder";
                    var tempTableForVariable =
                        allEdgeAttributeNameInOrder.Select(
                            x =>
                                string.Format(", {0}",
                                    (hashSetOfUserSuppliedEdgeAttribute.Contains(x) ? ("tempTable.[" + x + "]") : "null")));
                    var tempStringForVariable = string.Join("", tempTableForVariable);
                    string aggregateFunction = aggregeteFunctionName + "([sinkTable].[GlobalNodeId]" +
                                               tempStringForVariable +
                                               ")";
                    const string updateEdgeData = @"
                    Select [{0}].globalnodeid, [GraphView_InsertEdgeInternalTable].binary, [GraphView_InsertEdgeInternalTable].sinkCount into #ParallelOptimalTempTable
					From 
					(
					Select tempTable.[{2}] source, [{3}].{4} as binary, count([sinkTable].[GlobalNodeId]) as sinkCount
                    From {5} tempTable
                    Join [{3}].[{6}] sinkTable
                    On sinkTable.[{7}] = tempTable.[{8}]
                    Group by tempTable.[{2}]
					)
                    as [GraphView_InsertEdgeInternalTable],
					[{3}].[{0}]
                    Where [{0}].[{9}] = [GraphView_InsertEdgeInternalTable].source;
                    UPDATE [{3}].[{0}] SET {1} .WRITE(temp.[binary], null, null), {1}OutDegree += sinkCount
					from #ParallelOptimalTempTable temp
					where temp.globalnodeid = [{0}].globalnodeid;
                    drop table #ParallelOptimalTempTable;";
                    command.Parameters.Clear();
                    command.CommandText = string.Format(updateEdgeData, sourceTableName, edgeColumnName,
                        sourceNodeId.Item3,
                        tableSchema, aggregateFunction, randomTempTableName, sinkTableName, sinkNodeId.Item1,
                        sinkNodeId.Item3, sourceNodeId.Item1);
                    command.ExecuteNonQuery();

                    const string updateReversedEdgeData = @"
                        UPDATE [{3}].[{0}] SET [InDegree] += sourceCount
                        From (
                            Select tempTable.[{1}] as Sink, count(*) as sourceCount
                            From {2} tempTable
                            Join [{5}]
                            On [{5}].[{6}] = tempTable.[{7}]
                            Group by tempTable.[{1}]
                        ) as [GraphView_InsertEdgeInternalTable]
                        Where [GraphView_InsertEdgeInternalTable].Sink = [{0}].[{4}]";
                    command.CommandText = string.Format(updateReversedEdgeData, sinkTableName, sinkNodeId.Item3,
                        randomTempTableName, tableSchema, sinkNodeId.Item1, sourceTableName, sourceNodeId.Item1,
                        sourceNodeId.Item3);
                    command.ExecuteNonQuery();
                }
                else
                {
                    // Rebuild the table instead of the old table.
                    command.Parameters.Clear();
                    command.Parameters.Add("name", SqlDbType.NVarChar, 128);
                    command.Parameters["name"].Value = sourceTableName;
                    command.Parameters.Add("schema", SqlDbType.NVarChar, 128);
                    command.Parameters["schema"].Value = tableSchema;

                    //Record default value constrain of source table
                    var defaultConstrainInfo = new List<Tuple<string, string, string>>();
                    //list<DefaultConstrainName, ColumnName, DefaultValue> 
                    command.CommandText = @"
                    select DC.name as DC, C.name, definition
                    from sys.tables T
                    join sys.schemas SC
                    on SC.name = @schema and SC.schema_id = T.schema_id
                    join sys.default_constraints DC
                    on DC.parent_object_id = T.object_id and T.name = @name
                    join sys.columns C
                    on C.column_id = DC.parent_column_id and C.object_id = T.object_id";
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            defaultConstrainInfo.Add(Tuple.Create(reader["DC"].ToString(), reader["name"].ToString(),
                                reader["definition"].ToString()));
                        }
                        reader.Close();
                    }

                    //Record information of indexes of source table.
                    var indexInfo = new List<IndexOrConstraintInformationRecord>();
                    command.CommandText = @"
                    select K.name indexname, K.index_id, K.type_desc, K.is_unique, K.is_primary_key, C.name referencename
                    from sys.tables T
                    join sys.schemas SC
                    on SC.name = @schema and SC.schema_id = T.schema_id
                    join sys.indexes K
                    on T.object_id = K.object_id and T.name = @name 
                    join sys.index_columns I
                    on I.object_id = K.object_id and I.index_id = K.index_id
                    join sys.columns C
                    on I.object_id = C.object_id and I.column_id = C.column_id
                    order by K.index_id, I.index_id";

                    using (var reader = command.ExecuteReader())
                    {
                        int indexId = -1;
                        while (reader.Read())
                        {
                            if (reader["indexname"].ToString() != "")
                            {
                                var columnIndexId = Convert.ToInt32(reader["index_id"].ToString());
                                if (indexId != columnIndexId)
                                {
                                    indexInfo.Add(
                                        new IndexOrConstraintInformationRecord
                                        {
                                            name = reader["indexname"].ToString(),
                                            cluseterType = reader["type_desc"].ToString(),
                                            isUnique = (reader["is_unique"].ToString() == "True"),
                                            isPrimaryKey = (reader["is_primary_key"].ToString() == "True"),
                                            isConstraint = 0,
                                            indexColumns = new List<string>()
                                        });
                                    indexId = columnIndexId;
                                }
                                indexInfo.Last().indexColumns.Add(reader["referencename"].ToString());
                            }
                        }
                        reader.Close();
                    }

                    command.CommandText = @"
                    select KC.name 
                    from sys.key_constraints KC
                    join sys.schemas SC
                    on SC.schema_id  = KC.schema_id and SC.name = @schema 
                    join sys.tables T
                    on T.schema_id = SC.schema_id and T.name = @name and T.object_id  = KC.parent_object_id";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            foreach (var it in indexInfo)
                            {
                                var str = reader["name"].ToString();
                                if (str.ToLower() == it.name.ToLower())
                                {
                                    it.isConstraint = 1;
                                }
                            }
                        }
                        reader.Close();
                    }

                    //Get the column name of the source table.
                    var allColumnsInNewTable = new List<string>();
                    var allColumnsInNewTable2 = new List<string>();
                    const string getColumnNameInTable = @"
                    select C.name
                    from sys.schemas SC
                    join sys.tables T
                    on SC.schema_id = T.schema_id
                    join sys.columns C
                    on C.object_id = T.object_id
                    where SC.name = @tableschema and T.name = @tablename 
                    order by C.column_id ";
                    command.Parameters.Clear();
                    command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                    command.Parameters["tableschema"].Value = tableSchema;
                    command.Parameters.Add("tablename", SqlDbType.NVarChar, 128);
                    command.Parameters["tablename"].Value = sourceTableName;
                    command.CommandText = getColumnNameInTable;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var column = reader["name"].ToString();
                            if (column.ToLower() == edgeColumnName.ToLower())
                            {
                                allColumnsInNewTable2.Add("[" + column + "]");
                                column =
                                    string.Format(
                                        "ISNULL(ISNULL([{0}], 0x) + [GraphView_InsertEdgeInternalTable].[binary], 0x) as [{0}]",
                                        edgeColumnName);
                                allColumnsInNewTable.Add(column);
                            }
                            else if (column.ToLower() == edgeColumnName.ToLower() + "outdegree")
                            {
                                allColumnsInNewTable2.Add("[" + column + "]");
                                column =
                                    string.Format(
                                        "ISNULL(([{0}] + [GraphView_InsertEdgeInternalTable].sinkCount), 0) as [{0}]",
                                        column);
                                allColumnsInNewTable.Add(column);
                            }
                            else
                            {
                                allColumnsInNewTable.Add("[" + sourceTableName + "].[" + column + "]");
                                allColumnsInNewTable2.Add("[" + column + "]");
                            }
                        }
                    }
                    var hashSetOfUserSuppliedEdgeAttribute =
                        new HashSet<string>(userSuppliedEdgeAttributeInfo.Select(x => x.Item1));
                    string aggregeteFunctionName = tableSchema + '_' + sourceTableName + '_' + edgeColumnName + '_' + "Encoder";
                    var tempTableForVariable =
                        allEdgeAttributeNameInOrder.Select(
                            x =>
                                string.Format(", {0}",
                                    (hashSetOfUserSuppliedEdgeAttribute.Contains(x) ? ("tempTable.[" + x + "]") : "null")));
                    var tempStringForVariable = string.Join("", tempTableForVariable);
                    string aggregateFunction = aggregeteFunctionName + "(sinkTable.[GlobalNodeId]" +
                                               tempStringForVariable +
                                               ")";
                    string tempSourceTableName = "tempSourceTable_" + RandomString();

                    //Build new source node table. 
                    const string createTempSourceTable = @"
                    select *
                    into [{0}].[{1}]
                    from [{0}].[{2}]
                    where 1 != 1;
                    SET IDENTITY_INSERT [{0}].[{1}] ON";
                    command.Parameters.Clear();
                    command.CommandText = string.Format(createTempSourceTable, tableSchema, tempSourceTableName,
                        sourceTableName);
                    command.ExecuteNonQuery();

                    const string insertTempSourceTable = @"
                    Select tempTable.[{2}] source, [{3}].{4} as binary, Count([sinkTable].[GlobalNodeId]) as sinkCount into #ParallelOptimalTempTable
                    From {5} tempTable
                    Join [{3}].[{6}] sinkTable
                    On sinkTable.[{7}] = tempTable.[{8}]
                    Group by tempTable.[{2}];

                    insert into [{3}].[{10}] ({11})
                    select {1}
                    From #ParallelOptimalTempTable
                    as [GraphView_InsertEdgeInternalTable]
                    right join [{3}].[{0}]
                    on [{0}].[{9}] = [GraphView_InsertEdgeInternalTable].source;
                    drop table #ParallelOptimalTempTable";
                    command.Parameters.Clear();
                    command.CommandText = string.Format(insertTempSourceTable, sourceTableName,
                        string.Join(",", allColumnsInNewTable), sourceNodeId.Item3, tableSchema, aggregateFunction,
                        randomTempTableName, sinkTableName, sinkNodeId.Item1, sinkNodeId.Item3, sourceNodeId.Item1,
                        tempSourceTableName, string.Join(",", allColumnsInNewTable2));
                    command.ExecuteNonQuery();

                    //Drop the old source node table and rename the new source table
                    const string dropTable = @"
                        drop table {0}";
                    command.Parameters.Clear();
                    command.CommandText = string.Format(dropTable, sourceTableName);
                    command.ExecuteNonQuery();
                    const string turnOffIdentityInsert = @"
                    SET IDENTITY_INSERT [{0}].[{1}] OFF";
                    command.CommandText = string.Format(turnOffIdentityInsert, tableSchema, tempSourceTableName);
                    command.ExecuteNonQuery();
                    const string changeTempTableName = @"
                    sp_rename '[{0}].[{1}]', '{2}'; ";
                    command.CommandText = string.Format(changeTempTableName, tableSchema, tempSourceTableName,
                        sourceTableName);
                    command.ExecuteNonQuery();

                    //Rebuild indexes or constraint in the new source node table
                    command.Parameters.Clear();
                    const string buildUnique = @"
                    ALTER TABLE {0}
                    ADD constraint {1}
                    {2}({3})";
                    const string buildIndex = @"
                    Create {0} {1} index {2}
                    on {3}({4})";
                    var tableNameWithSchema = "[" + tableSchema + "].[" + sourceTableName + "]";

                    foreach (var it in indexInfo)
                    {
                        if (it.isPrimaryKey || it.isConstraint == 1) //primary key or unique constraint
                        {
                            var type = it.isPrimaryKey ? "Primary key" : "Unique";
                            command.CommandText = string.Format(buildUnique, tableNameWithSchema,
                                it.name, type, string.Join(",", it.indexColumns));
                        }
                        else if (it.isConstraint == 0) //index
                        {
                            var unique = it.isUnique ? "unique" : "";
                            command.CommandText = string.Format(buildIndex, unique, it.cluseterType,
                                it.name, tableNameWithSchema, string.Join(",", it.indexColumns));
                        }
                        command.ExecuteNonQuery();
                    }

                    //Rebuild default constrain of source table
                    command.Parameters.Clear();
                    const string defaultConstrainRebuild = @"
                    ALTER TABLE [{0}].[{1}] ADD CONSTRAINT [{2}] DEFAULT ({3}) FOR [{4}]";
                    foreach (var it in defaultConstrainInfo)
                    {
                        command.CommandText = string.Format(defaultConstrainRebuild, tableSchema, sourceTableName,
                            it.Item1, it.Item3, it.Item2);
                        command.ExecuteNonQuery();
                    }

                    const string updateReversedEdgeData = @"
                        UPDATE [{3}].[{0}] SET [InDegree] += sourceCount
                        From (
                            Select tempTable.[{1}] as Sink, count(*) as sourceCount
                            From {2} tempTable
                            Join [{5}]
                            On [{5}].[{6}] = tempTable.[{7}]
                            Group by tempTable.[{1}]
                        ) as [GraphView_InsertEdgeInternalTable]
                        Where [GraphView_InsertEdgeInternalTable].Sink = [{0}].[{4}]";
                    command.CommandText = string.Format(updateReversedEdgeData, sinkTableName, sinkNodeId.Item3,
                        randomTempTableName, tableSchema, sinkNodeId.Item1, sourceTableName, sourceNodeId.Item1,
                        sourceNodeId.Item3);
                    command.ExecuteNonQuery();
                }

                //Drop temp table
                var dropTempTable = @"Drop table {0}";
                command.CommandText = string.Format(dropTempTable, randomTempTableName);
                command.ExecuteNonQuery();
                transaction.Commit();
            }
            catch (Exception error)
            {
                transaction.Rollback();
                throw new BulkInsertEdgeException(error.Message);
            }
        }

        public class FileInfo
        {
            public static string FieldTerminator { get; set; }
            public static string RowTerminator { get; set; }
            public static string ByDefaultType { get; set; }
            public static bool SkipScanLabel { get; set; }
            public string FileName { get; set; }

            public List<string> FileHeader { get; set; }
            public Dictionary<string, string> ColumnToType { get; set; }
            public List<string> Labels { get; set; }
            public int LabelOffset { get; set; }

            public static readonly Dictionary<string ,string> TypeDict = new Dictionary<string ,string>
            {
                {"int", "int"},
                {"long", "bigint"},
                {"float","float"},
                {"double", "float"},
                {"boolean","bit"},
                {"byte", "tinyint"},
                {"short","smallint"},
                {"char","nchar(1) COLLATE Latin1_General_100_CS_AI"},
                {"string","nvarchar(4000) COLLATE Latin1_General_100_CS_AI"}
            }; 

            public FileInfo(string filename)
            {
                FileName = filename;
                ColumnToType = new Dictionary<string, string>();
            }

            public virtual void ParseHeader()
            {
            }

            public void getHeader()
            {
                using (var reader = new StreamReader(FileName))
                {
                    var head = reader.ReadLine();
                    var words = new string[1];
                    words[0] = FieldTerminator;
                    FileHeader = head.Split(words, StringSplitOptions.None).ToList();

                }
            }

            //After assigning LabelColumn, gets the the list of label in the file
            protected void getLabel()
            {
                var dataColumnName = new List<string>();
                var columnDataType = new List<string>();
                int fieldNumber = FileHeader.Count;
                for (int i = 0; i < fieldNumber; i++)
                {
                    columnDataType.Add("nvarchar");
                    dataColumnName.Add(RandomString());
                }
                var labelDict =  new Dictionary<string, bool>();
                using (var reader = new BulkInsertFileDataReader(FileName, FieldTerminator, RowTerminator,
                    dataColumnName, columnDataType, true))
                {
                    if (SkipScanLabel)
                    {
                        if (reader.Read())
                        {
                            labelDict[reader.GetValue(LabelOffset).ToString().ToLower()] = true;
                        }
                    }
                    else
                    {
                        while (reader.Read())
                        {
                            labelDict[reader.GetValue(LabelOffset).ToString().ToLower()] = true;
                        }
                    }
                }
                Labels = labelDict.Select(x => x.Key).ToList();
            }

            protected bool addColumn(string columnName, string columnType)
            {
                 
                if (ColumnToType.ContainsKey(columnName))
                {
                    return false;
                }
                ColumnToType[columnName] = columnType.ToLower();
                return true;
            }

        }


        private class NodeFileInfo : FileInfo
        {
            public string UserId { get; set; } //user Id's name and data type
            public int UserIdOffset { set; get; } 
            public string NameSpace { get; set; }

            public NodeFileInfo(string fileName) : base(fileName)
            {
            }

            public override void ParseHeader()
            {
                //Get label column and label list
                var labelColumns =
                    FileHeader.Select((x, i) => Tuple.Create(x, i)).Where(x  => (x.Item1.ToLower() == ":label")).ToArray();
                if (labelColumns.Length == 0)
                {
                    throw new BulkInsertNodeException(String.Format("The file {0} does not contain Label column.",
                        FileName));
                }
                else if (labelColumns.Length > 1)
                {
                    throw new BulkInsertNodeException(
                        String.Format("The file {0} contains more than one Label column.", FileName));
                }
                LabelOffset = labelColumns[0].Item2;
                getLabel();

                int count = -1;
                //Get user id name and namespace, parse header
                foreach (var iterator in FileHeader)
                {
                    count++;
                    if (String.Equals(iterator, ":label", StringComparison.CurrentCultureIgnoreCase))
                    {
                        continue;
                    }
                    var firstChar = iterator.ToLower().IndexOf(":id");
                    if (firstChar != -1)
                    {
                        UserIdOffset = count;
                        if (UserId != null)
                        {
                            throw new BulkInsertNodeException(
                                String.Format("The file {0} contains more than one  id columns.", FileName));
                        }
                        int startIndex = iterator.IndexOf("(");
                        int endIndex = iterator.IndexOf(")");
                        if (startIndex != -1 && endIndex != -1)
                        {
                            int length = endIndex - startIndex - 1;
                            NameSpace = iterator.Substring(startIndex + 1, length).ToLower();
                        }
                        else
                        {
                            NameSpace = "";
                        }
                        UserId = iterator.Substring(0, firstChar).ToLower();
                    }
                    else
                    {
                        var colon = iterator.IndexOf(":");
                        string columnType;
                        string columnName;
                        if (colon == -1)
                        {
                            columnType = ByDefaultType;
                            columnName = iterator.ToLower();
                        }
                        else
                        {
                            columnName = iterator.Substring(0, colon).ToLower();
                            columnType = iterator.Substring(colon + 1, iterator.Length - colon - 1).ToLower();
                            if (TypeDict.ContainsKey(columnType))
                            {
                               columnType = TypeDict[columnType];
                            }
                        }
                        if (!addColumn(columnName.ToLower(), columnType))
                        {
                            throw new BulkInsertNodeException(String.Format("The file \"{0}\" contains the same column name.",
                                iterator));
                        }
                        
                    }

                }
                if (UserId == null)
                {
                        throw new BulkInsertNodeException(
                            String.Format("The file {0} does not contain any id column.", FileName));
                }
            }
        }

        private class EdgeFileInfo : FileInfo
        {
            public int StartIdOffset { get; set; }
            public int EndIdOffset { get; set; }
            public string StartNameSpace { get; set; }
            public string EndNameSpace { get; set; }

            public string sinkTable { get; set; }

            public EdgeFileInfo(string fileName) : base(fileName)
            {
            }
            public override void ParseHeader()
            {

                var typeColumns =
                    FileHeader.Select((x, i) => Tuple.Create(x, i)).Where(x  => (x.Item1.ToLower() == ":type")).ToArray();
                if (typeColumns.Length == 0)
                {
                    throw new BulkInsertNodeException(String.Format("The file {0} does not contain type column.",
                        FileName));
                }
                else if (typeColumns.Length > 1)
                {
                    throw new BulkInsertEdgeException(
                        String.Format("The file {0} contains more than one type column.", FileName));
                }
                LabelOffset = typeColumns[0].Item2;
                getLabel();

                int count = -1;
                //Get user id name and namespace, parse header
                foreach (var iterator in FileHeader)
                {
                    count++;
                    if (String.Equals(iterator, ":type", StringComparison.CurrentCultureIgnoreCase))
                    {
                        continue;
                    }
                    var firstChar = iterator.ToLower().IndexOf(":start_id");
                    if (firstChar != -1)
                    {
                        StartIdOffset = count;
                        if (StartNameSpace != null)
                        {
                            throw new BulkInsertNodeException(
                                String.Format("The file {0} contains more than one start id columns.", FileName));
                        }
                        int startIndex = iterator.IndexOf("(");
                        int endIndex = iterator.IndexOf(")");
                        if (startIndex != -1 && endIndex != -1)
                        {
                            int length = endIndex - startIndex - 1;
                            StartNameSpace = iterator.Substring(startIndex + 1, length).ToLower();
                        }
                        else
                        {
                            StartNameSpace = "";
                        }
                    }
                    else
                    {
                        var pos = iterator.ToLower().IndexOf(":end_id");
                        if (pos != -1)
                        {
                            EndIdOffset = count;
                            if (EndNameSpace != null)
                            {
                                throw new BulkInsertNodeException(
                                    String.Format("The file {0} contains more than one end id columns.", FileName));
                            }
                            int startIndex = iterator.IndexOf("(");
                            int endIndex = iterator.IndexOf(")");
                            if (startIndex != -1 && endIndex != -1)
                            {
                                int length = endIndex - startIndex - 1;
                                EndNameSpace = iterator.Substring(startIndex + 1, length).ToLower();
                            }
                            else
                            {
                                EndNameSpace = "";
                            }
                        }
                        else
                        {
                            var colon = iterator.IndexOf(":");
                            string columnType;
                            string columnName;
                            if (colon == -1)
                            {
                                columnName = iterator.ToLower();
                                columnType = ByDefaultType;
                            }
                            else
                            {
                                columnName = iterator.Substring(0, colon).ToLower();
                                columnType = iterator.Substring(colon + 1, iterator.Length - colon - 1).ToLower();
                                if (TypeDict.ContainsKey(columnType))
                                {
                                   columnType = TypeDict[columnType];
                                }
                            }
                            if (!addColumn(columnName.ToLower(), columnType))
                            {
                                throw new BulkInsertNodeException(
                                    String.Format("The file \"{0}\" contains the same column name.",
                                        iterator));
                            }
                        }
                    }
                }
                if (StartNameSpace == null || EndNameSpace == null)
                {
                        throw new BulkInsertNodeException(
                            String.Format("The file {0} does not contain any id column.", FileName));
                }
            }
        }

        private class EdgeInfo
        {
            public Dictionary<string, string> AttributeToType { get; set; }
            public string Sink;

            public EdgeInfo ()
            {
                AttributeToType = new Dictionary<string, string>();
            }
            public bool AddAtrribute(string attributeName, string attributeType)
            {
                if (AttributeToType.ContainsKey(attributeName))
                {
                    return false;
                }
                AttributeToType[attributeName] = attributeType;
                return true;
            }

            public static bool operator==(EdgeInfo a, EdgeInfo b)
            {
                if (a.Sink == b.Sink)
                {
                    Dictionary<string, string> x = a.AttributeToType;
                    Dictionary<string, string> y = b.AttributeToType;
                    if (x.Count != y.Count)
                    {
                        return false;
                    }

                    foreach (var it in x)
                    {
                        if (!y.ContainsKey(it.Value))
                        {
                            return false;
                        }
                        else
                        {
                            if (!String.Equals(y[it.Value], it.Key, StringComparison.CurrentCultureIgnoreCase))
                            {
                                return false;
                            }
                        }
                    }
                    return true;
                }
                return false;
            }

            public static bool operator !=(EdgeInfo a, EdgeInfo b)
            {
                return !(a == b);
            }

            public override bool Equals(object obj)
            {
                throw new NotImplementedException();
            }

            public override int GetHashCode()
            {
                throw new NotImplementedException();
            }

            public string Tostring(string edgeName)
            {
                const string edgeString = @"
                    [ColumnRole: ""Edge"", Reference: ""{0}"" {1}]
                    [{2}] [varchar](max)";
                var attributes = string.Join(", ",  AttributeToType.Select(x => x.Key + ": \"" + x.Value + "\""));
                if (!string.IsNullOrEmpty(attributes))
                {
                    attributes = ", Attributes: {" + attributes + "}";
                }
                return string.Format(edgeString, Sink, attributes, edgeName);
            } 
        }

        private class NodeInfo
        {
            public string tableName;
            public Tuple<string, string> UserId { get; set; } //user Id's name and data type
            public Dictionary<string, string> PropetyToType { get; set; }
            public Dictionary<string, EdgeInfo> EdgesToInfo { get; set; }

            public NodeInfo()
            {
                PropetyToType = new Dictionary<string, string>();
                EdgesToInfo = new Dictionary<string, EdgeInfo>();
            }
            public bool AddProperty(string columnName, string columnType)
            {
                if (PropetyToType.ContainsKey(columnName))
                {
                    if (PropetyToType[columnName] != columnType.ToLower())
                    {
                        return false;
                    }
                }
                PropetyToType[columnName] = columnType.ToLower();
                return true;
            }

            public bool AddEdge(string edgeName, EdgeInfo info)
            {
                if (EdgesToInfo.ContainsKey(edgeName))
                {
                    if (EdgesToInfo[edgeName] != info)
                    {
                        return false;
                    }
                }
                EdgesToInfo[edgeName] = info;
                return true;
            }

            public override string ToString()
            {
                const string createNodeTable = @"
                    CREATE TABLE [{0}](
                        [ColumnRole: ""NodeId""]
                        [{1}] {2},
                        {3}
                        {4}
                    )";

                const string propertyString = @"
                        [ColumnRole: ""Property""]
                        [{0}] {1}";
                var properties = string.Join(",\n",
                    PropetyToType.Select(x => string.Format(propertyString, x.Key, x.Value)));
                if (!string.IsNullOrEmpty(properties))
                {
                    properties += ',';
                }
                var edges = string.Join(",\n", EdgesToInfo.Select(x => x.Value.Tostring(x.Key)));
                return string.Format(createNodeTable, tableName, UserId.Item1, UserId.Item2, properties, edges);
            }
        }

        private string convertSqlType(string x)
        {
            int firstchar = x.IndexOf("(");
            if (firstchar == -1)
            {
                return x;
            }
            else
            {
                return x.Substring(0, firstchar);
            }

        }

        /// <summary>
        /// Imports nodes and edges data into GraphView.
        /// Runs the following command to enable minimal logging,
        /// which will highly enhance the performance of bulk loading:
        /// USE master; ALTER DATABASE database_name SET RECOVERY BULK_LOGGED;
        /// </summary>
        /// <param name="nodesFileName"> The list of node file name(s)</param>
        /// <param name="edgesFileName"> the list of edge file name(s)</param>
        /// <param name="directory"> The directory of the node and edge data files</param>
        /// <param name="skipScanLabel"> True, notifies GraphView that every node file has only one label and 
        /// every edge file has only one type. This will improve the performance of importing.</param>
        /// <param name="fieldTerminator"> The field terminator of data files</param>
        /// <param name="byDefaultType"> The default data type.</param>
        public void Import(IList<string> nodesFileName, IList<string> edgesFileName, string directory,
            bool skipScanLabel = false, string fieldTerminator = ",", string byDefaultType = "string")
        {
            if (!string.IsNullOrEmpty(directory))
            {
                if (Directory.Exists(directory))
                {
                    nodesFileName = nodesFileName.Select(x => directory + "\\" + x).ToList();
                    edgesFileName = edgesFileName.Select(x => directory + "\\" + x).ToList();
                }
                else
                {
                    throw new BulkInsertNodeException(String.Format("The directory {0} does not exist.", directory));
                }
            }
            if (FileInfo.TypeDict.ContainsKey(byDefaultType.ToLower()))
            {
                byDefaultType = FileInfo.TypeDict[byDefaultType.ToLower()];
            }
            else
            {
                throw new BulkInsertNodeException("The type by default is not supported. The type supported includes:\n" +
                                                  "int,long,float,double,boolean,byt,short,char,string\n");
            }
            FileInfo.FieldTerminator = fieldTerminator;
            FileInfo.RowTerminator = "\r\n";
            FileInfo.ByDefaultType = byDefaultType;
            FileInfo.SkipScanLabel = skipScanLabel;

            //Collects file header's information
            var nodeFileToInfo = new Dictionary<string, NodeFileInfo>();
            foreach (var it in nodesFileName)
            {
                if (!File.Exists(it))
                {
                    throw new BulkInsertNodeException(String.Format("The file {0} does not exist.", it));
                }
                else
                {
                    var temp = new NodeFileInfo(it);

                    temp.getHeader();
                    temp.ParseHeader();
                    nodeFileToInfo[it] = temp;
                }
            }

            var edgeFileToInfo = new Dictionary<string, EdgeFileInfo>();
            foreach (var it in edgesFileName)
            {
                if (!File.Exists(it))
                {
                    throw new BulkInsertEdgeException(String.Format("The file {0} does not exist.", it));
                }
                else
                {
                    var temp = new EdgeFileInfo(it);
                    temp.getHeader();
                    temp.ParseHeader();
                    edgeFileToInfo[it] = temp;
                }
            }
            
            var nameSpaceToNodeTableSet = new Dictionary<string, HashSet<string>>();

            //Generates node table's information
            var nodeTableToInfo = new Dictionary<string, NodeInfo>();
            foreach (var it in nodeFileToInfo)
            {
                NodeFileInfo nodeFile = it.Value;
                foreach (var iterator in nodeFile.Labels)
                {
                    NodeInfo temp;
                    if (nodeTableToInfo.ContainsKey(iterator))
                    {
                        temp = nodeTableToInfo[iterator];
                    }
                    else
                    {
                        temp = new NodeInfo();
                    }
                    
                    //Assigns properties
                    foreach (var VARIABLE in nodeFile.ColumnToType)
                    {
                        if (!temp.AddProperty(VARIABLE.Key, VARIABLE.Value))
                        {
                            throw new BulkInsertNodeException(
                                String.Format(
                                    "The label \"{0}\" contains column \"{1}\" in different types in two different file",
                                    iterator, VARIABLE.Key));
                        }
                    }

                    //Assigns user id
                    var userid = Tuple.Create(nodeFile.UserId.ToLower(), byDefaultType.ToLower());
                    if (temp.UserId != null && !(userid.Item1 == temp.UserId.Item1 || userid.Item2 == temp.UserId.Item2))
                    {
                        throw new BulkInsertNodeException(String.Format("The label \"{0}\" contains two differenct ids in two node files",
                            iterator));
                    }
                    temp.UserId = userid;

                    temp.tableName = iterator;
                    nodeTableToInfo[iterator] = temp;

                    //Updates name space dictionary
                    if (!nameSpaceToNodeTableSet.ContainsKey(nodeFile.NameSpace))
                    {
                       nameSpaceToNodeTableSet[nodeFile.NameSpace] = new HashSet<string>();
                    }
                    HashSet<string> nodeTableSet = nameSpaceToNodeTableSet[nodeFile.NameSpace];
                    if (!nodeTableSet.Contains(iterator))
                    {
                        nodeTableSet.Add(iterator);
                    }
                }
            }

            //Generates edge file's information
            foreach (var it in edgeFileToInfo)
            {
                EdgeFileInfo edgeFile = it.Value;
                HashSet<string> startNodeTable = nameSpaceToNodeTableSet[edgeFile.StartNameSpace];
                HashSet<string> endNodeTable = nameSpaceToNodeTableSet[edgeFile.EndNameSpace];

                var edge = new EdgeInfo();
                if (endNodeTable.Count > 2)
                {
                    throw new BulkInsertEdgeException("One edge cannot refer to two different node tables");
                }
                else if (endNodeTable.Count < 1)
                {
                    throw new BulkInsertEdgeException(
                        string.Format("Cannot find the namespace \"{0}\" in node files",
                            edgeFile.EndNameSpace));
                }

                foreach (var VARIABLE in endNodeTable)
                {
                    edgeFile.sinkTable = edge.Sink = VARIABLE;
                }

                foreach (var VARIABLE in edgeFile.ColumnToType)
                {
                    if (!edge.AddAtrribute(VARIABLE.Key, VARIABLE.Value))
                    {
                        throw new BulkInsertEdgeException(
                            string.Format("The Edge data file \"{0}\" contains two attributes of same name.", it.Key));
                    }
                }
                foreach (var iterator in edgeFile.Labels)
                {
                    foreach (var VARIABLE in startNodeTable)
                    {
                        if (!nodeTableToInfo[VARIABLE].AddEdge(iterator, edge))
                        {
                            throw new BulkInsertEdgeException(
                                string.Format("There exists edge type \"{0}\" conflicts on node table \"{1}\" ",
                                    iterator, VARIABLE));
                        }
                    }
                }
            }

            var transaction = Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            command.CommandTimeout = 0;
            try
            {
                //Creates node table
                foreach (var pair in nodeTableToInfo)
                {
                    CreateNodeTable(pair.Value.ToString(), transaction);

                    const string dropConstrain = @"
                    ALTER TABLE {0} DROP CONSTRAINT {1}";
                    string constrainName = "dbo" + pair.Value.tableName + "_PK_GlobalNodeId";
                    command.CommandText = string.Format(dropConstrain, pair.Value.tableName, constrainName);
                    command.ExecuteNonQuery();

                    string indexName = "dbo" + pair.Value.tableName + "_UQ_" + pair.Value.UserId.Item1;
                    command.CommandText = string.Format(dropConstrain, pair.Value.tableName, indexName);
                    command.ExecuteNonQuery();
                }

                //Bulk inserts nodes
                foreach (var pair in nodeFileToInfo)
                {
                    var nodeFile = pair.Value;
                    //Bulk insert
                    var dataColumnName = new List<string>(nodeFile.FileHeader.Count);
                    var columnDataType = new List<string>(nodeFile.FileHeader.Count);

                    using (var it = nodeFile.ColumnToType.GetEnumerator())
                    {
                        for (int i = 0; i < nodeFile.FileHeader.Count; i++)
                        {
                            if (i == nodeFile.UserIdOffset)
                            {
                                dataColumnName.Add(nodeFile.UserId);
                                columnDataType.Add(convertSqlType(byDefaultType));
                            }
                            else if (i == nodeFile.LabelOffset)
                            {
                                dataColumnName.Add("label");
                                columnDataType.Add(convertSqlType("nvarchar(4000)"));
                            }
                            else
                            {
                                if (it.MoveNext())
                                {
                                    dataColumnName.Add(it.Current.Key);
                                    columnDataType.Add(convertSqlType(it.Current.Value));
                                }
                            }
                        }
                    }
                    foreach (var it in nodeFile.Labels)
                    {
                        var tableNameWithSchema = "dbo." + it;
                        using (var sqlBulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.TableLock, transaction))
                        {
                            sqlBulkCopy.BulkCopyTimeout = 0;
                            using (
                                var reader = skipScanLabel
                                    ? new BulkInsertFileDataReader(nodeFile.FileName, fieldTerminator, "\r\n",
                                        dataColumnName, columnDataType, true)
                                    : new BulkInsertFileDataReader(nodeFile.FileName, fieldTerminator, "\r\n",
                                        dataColumnName, columnDataType, true, nodeFile.LabelOffset, it))
                            {
                                foreach (var variable in dataColumnName)
                                {
                                    if (variable != "label")
                                    {
                                        sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(variable, variable));
                                    }
                                }
                                sqlBulkCopy.DestinationTableName = tableNameWithSchema;
                                sqlBulkCopy.WriteToServer(reader);
                            }
                        }
                    }
                }

                //Rebuilds cluster index on node table
                foreach (var pair in nodeTableToInfo)
                {
                    const string createPrimaryKey = @"
                    ALTER TABLE {0} ADD CONSTRAINT {1} PRIMARY KEY (GlobalNodeId)";
                    string constrainName = "dbo" + pair.Value.tableName + "_PK_GlobalNodeId";
                    command.CommandText = string.Format(createPrimaryKey, pair.Value.tableName, constrainName);
                    command.ExecuteNonQuery();

                    const string dropIndex = @"
                    ALTER TABLE {0} ADD CONSTRAINT {1} UNIQUE ({2})";
                    string indexName = "dbo" + pair.Value.tableName + "_UQ_" + pair.Value.UserId.Item1;
                    command.CommandText = string.Format(dropIndex, pair.Value.tableName, indexName,
                        pair.Value.UserId.Item1);
                    command.ExecuteNonQuery();
                }

                //Bulk inserts edges
                foreach (var pair in edgeFileToInfo)
                {
                    var edgeFile = pair.Value;
                    var dataColumnName = new List<string>(edgeFile.FileHeader.Count);
                    var columnDataType = new List<string>(edgeFile.FileHeader.Count);

                    using (var it = edgeFile.ColumnToType.GetEnumerator())
                    {
                        for (int i = 0; i < edgeFile.FileHeader.Count; i++)
                        {
                            if (i == edgeFile.StartIdOffset)
                            {
                                dataColumnName.Add("startid");
                                columnDataType.Add(convertSqlType(byDefaultType));
                            }
                            else if (i == edgeFile.EndIdOffset)
                            {
                                dataColumnName.Add("endid");
                                columnDataType.Add(convertSqlType(byDefaultType));
                            }
                            if (i == edgeFile.LabelOffset)
                            {
                                dataColumnName.Add("type");
                                columnDataType.Add(convertSqlType(byDefaultType));
                            }
                            else
                            {
                                if (it.MoveNext())
                                {
                                    dataColumnName.Add(it.Current.Key);
                                    columnDataType.Add(convertSqlType(it.Current.Value));
                                }
                            }
                        }
                    }

                    HashSet<string> startNodeTable = nameSpaceToNodeTableSet[edgeFile.StartNameSpace];
                    foreach (var edgeColumnName in edgeFile.Labels)
                    {
                        //Create temp table for bulk inserting edge data
                        var randomTempTableName = "dbo." + edgeColumnName + edgeFile.sinkTable + "_" + RandomString();
                        var attributes = string.Join(",\n", edgeFile.ColumnToType.Select(x => x.Key + " " + x.Value));
                        const string createTempTable = @"
                        Create table {0}
                        (
                            startid {1},
                            endid {1},
                            {2}
                        )";

                        command.CommandText = string.Format(createTempTable, randomTempTableName, byDefaultType,
                            attributes);
                        command.ExecuteNonQuery();

                        //Bulk inset
                        using (var sqlBulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.TableLock, transaction))
                        {
                            sqlBulkCopy.BulkCopyTimeout = 0;
                            using (
                                var reader = skipScanLabel
                                    ? new BulkInsertFileDataReader(edgeFile.FileName, fieldTerminator, "\r\n",
                                        dataColumnName, columnDataType, true)
                                    : new BulkInsertFileDataReader(edgeFile.FileName, fieldTerminator, "\r\n",
                                        dataColumnName, columnDataType, true, edgeFile.LabelOffset, edgeColumnName))
                            {
                                foreach (var it in dataColumnName)
                                {
                                    if (it != "type")
                                    {
                                        sqlBulkCopy.ColumnMappings.Add(it, it);
                                    }
                                }
                                sqlBulkCopy.DestinationTableName = randomTempTableName;
                                sqlBulkCopy.WriteToServer(reader);
                            }
                        }

                        //Creates clustered index on sink node in temp table
                        string clusteredIndexName = "sinkIndex_" + RandomString();
                        const string createClusteredIndex = @"
                        create clustered index [{0}] on {1}([endid])";
                        command.Parameters.Clear();
                        command.CommandText = string.Format(createClusteredIndex, clusteredIndexName,
                            randomTempTableName);
                        command.ExecuteNonQuery();

                        foreach (var sourceTableName in startNodeTable)
                        {
                            //Updates database
                            string aggregeteFunctionName = "dbo_" + sourceTableName + '_' + edgeColumnName + '_' +
                                                           "Encoder";
                            var tempStringForVariable = string.Join(", ", edgeFile.ColumnToType.Select(x => x.Key));
                            if (!string.IsNullOrEmpty(tempStringForVariable))
                            {
                                tempStringForVariable = "," + tempStringForVariable;
                            }
                            string aggregateFunction = aggregeteFunctionName + "([sinkTable].[GlobalNodeId]" +
                                                       tempStringForVariable +
                                                       ")";
                            const string updateEdgeData = @"
                            Select [{0}].globalnodeid, [GraphView_InsertEdgeInternalTable].binary, [GraphView_InsertEdgeInternalTable].sinkCount into #ParallelOptimalTempTable
                            From 
                            (
                            Select tempTable.[{2}] source, [{3}].{4} as binary, count([sinkTable].[GlobalNodeId]) as sinkCount
                            From {5} tempTable
                            Join [{3}].[{6}] sinkTable
                            On sinkTable.[{7}] = tempTable.[{8}]
                            Group by tempTable.[{2}]
                            )
                            as [GraphView_InsertEdgeInternalTable],
                            [{3}].[{0}]
                            Where [{0}].[{9}] = [GraphView_InsertEdgeInternalTable].source;
                            UPDATE [{3}].[{0}] SET {1} .WRITE(temp.[binary], null, null), {1}OutDegree += sinkCount
                            from #ParallelOptimalTempTable temp
                            where temp.globalnodeid = [{0}].globalnodeid;
                            drop table #ParallelOptimalTempTable;";
                            command.Parameters.Clear();
                            var sinkTableId = nodeTableToInfo[edgeFile.sinkTable].UserId.Item1;
                            var sourceTableId = nodeTableToInfo[sourceTableName].UserId.Item1;
                            command.CommandText = string.Format(updateEdgeData, sourceTableName, edgeColumnName,
                                "startid",
                                "dbo", aggregateFunction, randomTempTableName, edgeFile.sinkTable, sinkTableId,
                                "endid", sourceTableId);
                            command.ExecuteNonQuery();

                            const string updateReversedEdgeData = @"
                            UPDATE [{3}].[{0}] SET [InDegree] += sourceCount
                            From (
                                Select tempTable.[{1}] as Sink, count(*) as sourceCount
                                From {2} tempTable
                                Join [{5}]
                                On [{5}].[{6}] = tempTable.[{7}]
                                Group by tempTable.[{1}]
                            ) as [GraphView_InsertEdgeInternalTable]
                            Where [GraphView_InsertEdgeInternalTable].Sink = [{0}].[{4}]";
                            command.CommandText = string.Format(updateReversedEdgeData, edgeFile.sinkTable, "endid",
                                randomTempTableName, "dbo", sinkTableId, sourceTableName, sourceTableId,
                                "startid");
                            command.ExecuteNonQuery();

                            //Drops temp table 
                            const string dropTempTable = @"
                            drop table {0}";
                            command.CommandText = string.Format(dropTempTable, randomTempTableName);
                            command.ExecuteNonQuery();
                        }
                    }
                }
                transaction.Commit();
            }

            catch (Exception error)
            {
                transaction.Rollback();
                throw new BulkInsertNodeException(error.Message);
            }
        }
    }
}