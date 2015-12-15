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
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
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
            return "DEBUG";
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
            private readonly string _rowterminator;
            private readonly string _fieldterminator;
            private readonly StreamReader _streamReader;
            private readonly IList<int> _kmp;

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

            public BulkInsertFileDataReader(string fileName, string fieldterminator,
                string rowterminator, IList<string> datacolumn, IList<string> dataType)
            {
                _streamReader = new StreamReader(fileName);
                _rowterminator = rowterminator;
                _fieldterminator = fieldterminator;
                _columnIndex = new Dictionary<string, int>();
                _dataType = dataType;
                _dataColumn = datacolumn;
                _countRow = 1;
                _kmp = new List<int>();
                PreKmp(rowterminator, _kmp);

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

            public bool Read()
            {
                bool getRowTerminator = false;
                var builder = new StringBuilder();
                var ichar = _streamReader.Read();
                int rowTerminatorPosition = 0;

                while (ichar != -1)
                {
                    if (rowTerminatorPosition == -1)
                    {
                        builder.Append((char) ichar);
                        ichar = _streamReader.Read();
                        rowTerminatorPosition = 0;
                    }
                    else if (_rowterminator[rowTerminatorPosition] == (char) ichar)
                    {
                        builder.Append((char) ichar);
                        rowTerminatorPosition++;
                        if (rowTerminatorPosition == _rowterminator.Length)
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

                builder.Length -= _rowterminator.Length;
                var rowstring = builder.ToString();
                _value = rowstring.Split(_fieldterminator.ToCharArray());
                if (_value == null || _value.Count() != FieldCount)
                {
                    if (_streamReader.Peek() == -1)
                    {
                        return false;
                    }
                    const string error = @"It should be {0} field(s) in the {1} row of data file";
                    throw new BulkInsertNodeException(string.Format(error, FieldCount, _countRow));
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
            List<string> dataColumnName = null, string fieldTerminator = "\t", string rowTerminator = "\r\n")
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
                        columnDataType))
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
        /// Data file should contain source node id and sink node id on the first and second columns,
        /// then followed by the columns of user-supplied edge attribute.
        /// </summary>
        /// <param name="dataFileName"> The name of data file.</param>
        /// <param name="tableSchema"> The Schema name of node table. Default by "dbo".</param>
        /// <param name="sourceTableName"> The source node table name of node table.</param>
        /// <param name="sourceNodeIdName"> The node id name of source node table.</param>
        /// <param name="sinkTableName"> The sink node table name of node table.</param>
        /// <param name="sinkNodeIdName"> The node id name of sink node table.</param>
        /// <param name="edgeColumnName"> The edge column name in source node table.</param>
        /// <param name="dataEdgeAttributeName"> User-supplied edge attribute name in data file in order.
        /// By default(null), data file should exactly contain all the Edge Attribute in creating order.
        /// Empty stands for that data file doesn't provide edge attribute.</param>
        /// <param name="fieldTerminator"> The field terminator of data file. Default by "\t".</param>
        /// <param name="rowTerminator"> The row terminator of data file. Default by "\r\n".</param>
        /// <param name="updateMethod"> Choose update method or rebuild table method to implement bulk insert edge.

        public void BulkInsertEdge(string dataFileName, string tableSchema, string sourceTableName,
            string sourceNodeIdName, string sinkTableName, string sinkNodeIdName, string edgeColumnName,
            List<string> dataEdgeAttributeName = null, string fieldTerminator = "\t", string rowTerminator = "\r\n",
            bool updateMethod = true)
        {
            //Data types mapping from C# into sql and .Net.
            var typeDictionary = new Dictionary<string, Tuple<string, string>>
            {
                {"int", new Tuple<string, string>("int", "Int32")},
                {"long", new Tuple<string, string>("bigint", "Int64")},
                {"double", new Tuple<string, string>("float", "Double")},
                {"string", new Tuple<string, string>("nvarchar(4000)", "String")}
            };
            
            //Check validity of input
            if (string.IsNullOrEmpty(sourceTableName))
            {
                throw new BulkInsertEdgeException("The string of source table name is null or empty.");
            }
            if (string.IsNullOrEmpty(sinkTableName ))
            {
                throw new BulkInsertEdgeException("The string of sink table name is null or empty.");
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
                throw new BulkInsertEdgeException("The string of sink node Id name is null or empty.");
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
                //Check validity of table name in GraphView(source and sink node table)
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

                //Get the name and datatype of user-defined node id of source and sink table
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
                                string.Format("\"{0}\" is not a node id name of sink node table\"{1}\"", sinkNodeIdName,
                                    sinkTableName));
                        }
                        sinkNodeId = Tuple.Create(reader["ColumnName"].ToString(), reader["DATA_TYPE"].ToString(),
                            reader["ColumnName"] + "_sink_" + RandomString(), reader["CHARACTER_MAXIMUM_LENGTH"].ToString());
                    }
                    else
                    {
                        throw new BulkInsertEdgeException(
                            string.Format("There doesn't exist a node id in sink note table \"{0}\".", sinkTableName));
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
                var  dataColumnName = new List<string>();
                var  columnDataType = new List<string>();
                dataColumnName.Add(sourceNodeId.Item3);
                columnDataType.Add(sourceNodeId.Item2);

                dataColumnName.Add(sinkNodeId.Item3);
                columnDataType.Add(sinkNodeId.Item2);

                foreach (var it in userSuppliedEdgeAttributeInfo)
                {
                   dataColumnName.Add(it.Item1); 
                   columnDataType.Add(it.Item2); 
                }
                

                using (var sqlBulkCopy = new SqlBulkCopy(Conn,SqlBulkCopyOptions.TableLock, transaction))
                {
                    sqlBulkCopy.BulkCopyTimeout = 0;
                    using (var reader = new BulkInsertFileDataReader(dataFileName, fieldTerminator, rowTerminator,
                            dataColumnName,
                            columnDataType))
                    {
                        foreach (var it in dataColumnName)
                        {
                            sqlBulkCopy.ColumnMappings.Add(it, it);
                        }
                        sqlBulkCopy.DestinationTableName = randomTempTableName;
                        sqlBulkCopy.WriteToServer(reader);
                    }
                }

                //Bulk insert edge data into temp table
                //const string bulkInsertEdge = @"
                //BULK INSERT {0}
                //   FROM ""{1}""
                //   WITH 
                //      (
                //         FIELDTERMINATOR = '{2}',
                //         ROWTERMINATOR = '{3}',
                //         tablock
                //      )";
                //command.Parameters.Clear();
                //command.CommandText = string.Format(bulkInsertEdge, randomTempTableName, dataFileName, fieldTerminator,
                //    rowTerminator);
                //command.ExecuteNonQuery();

                //Create clustered index on sink node in temp table
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
                            Select tempTable.[{1}] as sink, count(*) as sourceCount
                            From {2} tempTable
                            Join [{5}]
                            On [{5}].[{6}] = tempTable.[{7}]
                            Group by tempTable.[{1}]
                        ) as [GraphView_InsertEdgeInternalTable]
                        Where [GraphView_InsertEdgeInternalTable].sink = [{0}].[{4}]";
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
                            Select tempTable.[{1}] as sink, count(*) as sourceCount
                            From {2} tempTable
                            Join [{5}]
                            On [{5}].[{6}] = tempTable.[{7}]
                            Group by tempTable.[{1}]
                        ) as [GraphView_InsertEdgeInternalTable]
                        Where [GraphView_InsertEdgeInternalTable].sink = [{0}].[{4}]";
                    command.CommandText = string.Format(updateReversedEdgeData, sinkTableName, sinkNodeId.Item3,
                        randomTempTableName, tableSchema, sinkNodeId.Item1);
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
    }
} 