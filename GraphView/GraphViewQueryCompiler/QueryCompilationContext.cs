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
using System.Linq;

namespace GraphView
{
    internal enum RawRecordFieldType
    {
        VertexId,
        EdgeSource,
        EdgeOffset,
        EdgeSink,
        AdjacencyList,
        ReverseAdjacencyList,
        Value
    }

    /// <summary>
    /// TableHeader defines the columns of a temporary table
    /// </summary>
    internal class TempTableHeader
    {
        Dictionary<string, Tuple<int, RawRecordFieldType>> columnSet;

        public TempTableHeader()
        {
            columnSet = new Dictionary<string, Tuple<int, RawRecordFieldType>>();
        }

        public TempTableHeader(List<Tuple<string, RawRecordFieldType>> columnList)
        {
            columnSet = new Dictionary<string, Tuple<int, RawRecordFieldType>>(columnList.Count);
            
            for (int i = 0; i < columnList.Count; i++)
            {
                columnSet[columnList[i].Item1] = new Tuple<int, RawRecordFieldType>(i, columnList[i].Item2);
            }
        }

        public void AddColumn(string columnName, RawRecordFieldType ptype, int index)
        {
            // If the same column name has appeared before, the newly defined column
            // will override the older one and the older one will not be accessible.
            columnSet[columnName] = new Tuple<int, RawRecordFieldType>(index, ptype);
        }

        public int GetColumnIndex(string columnName)
        {
            return columnSet.ContainsKey(columnName) ? columnSet[columnName].Item1 : -1;
        }
    }

    internal class RawRecordField
    {
        public string TableAlias { get; set; }
        public string ColumnName { get; set; }
        public RawRecordFieldType ColumnType { get; set; }

        public RawRecordField(string tableAlias, string propertyName, RawRecordFieldType type)
        {
            TableAlias = tableAlias;
            ColumnName = propertyName;
            ColumnType = type;
        }

        public override bool Equals(object obj)
        {
            RawRecordField field = obj as RawRecordField;
            if (field == null)
            {
                return false;
            }

            return TableAlias == field.TableAlias && ColumnName == field.ColumnName;
        }

        public override int GetHashCode()
        {
            return string.Format("{0}.{1}", TableAlias, ColumnName).GetHashCode();
        }
    }

    /// <summary>
    /// Query compilation context records  the definitions of node/edge variables. 
    /// </summary>
    internal class QueryCompilationContext
    {
        public QueryCompilationContext ParentContext { get; set; }

        // A collection of temporary tables defined in the script.
        // A temporary table has a table name, a table header defining column names and their types 
        // and an execution operator producing the records that fill in the table 
        public Dictionary<string, Tuple<TempTableHeader, GraphViewExecutionOperator>> TableVariableCollection { get; private set; }

        public Dictionary<RawRecordField, int> RawRecordLayout { get; private set; }

        public QueryCompilationContext()
        {
            TableVariableCollection = new Dictionary<string, Tuple<TempTableHeader, GraphViewExecutionOperator>>();
            RawRecordLayout = new Dictionary<RawRecordField, int>();
        }

        public QueryCompilationContext(QueryCompilationContext parentContext)
        {
            ParentContext = parentContext;
            TableVariableCollection = new Dictionary<string, Tuple<TempTableHeader, GraphViewExecutionOperator>>();
            RawRecordLayout = new Dictionary<RawRecordField, int>();
        }

        public QueryCompilationContext(Dictionary<string, Tuple<TempTableHeader, GraphViewExecutionOperator>> tmpTables)
        {
            TableVariableCollection = tmpTables;
            RawRecordLayout = new Dictionary<RawRecordField, int>();
        }

        public void AddField(string tableAlias, string propertyName, RawRecordFieldType type)
        {
            int index = RawRecordLayout.Count;
            RawRecordLayout[new RawRecordField(tableAlias, propertyName, type)] = index;
        }

        public TempTableHeader ToTableHeader()
        {
            TempTableHeader header = new TempTableHeader();
            foreach (var pair in RawRecordLayout.OrderBy(e => e.Value))
            {
                header.AddColumn(pair.Key.ColumnName, pair.Key.ColumnType, pair.Value);
            }

            return header;
        }

         

        // A collection of node table variables
        private readonly Dictionary<string, WTableReferenceWithAlias> _nodeTableDictionary =
            new Dictionary<string, WTableReferenceWithAlias>(StringComparer.OrdinalIgnoreCase);

        // A collection of edge variables
        private readonly Dictionary<string, Tuple<WSchemaObjectName, WColumnReferenceExpression>> _edgeDictionary =
            new Dictionary<string, Tuple<WSchemaObjectName, WColumnReferenceExpression>>(StringComparer.OrdinalIgnoreCase);

        // A caching collection of a mapping from unbound node properties to node table aliases 
        // & unbound edge attributes to edge aliases in the current context.
        private Dictionary<string, string> _columnToAliasMapping;

        // A mapping from edges referenced by node/node view in the query context to the physical node table/node view name which the edges are bound to
        // (node table/node view name, edge column name) -> node table/node view name which the edges are bound to
        private readonly Dictionary<Tuple<string,string>, string> _edgeNodeBinding =
            new Dictionary<Tuple<string,string>, string>();
 

        public Dictionary<string, WTableReferenceWithAlias> NodeTableDictionary
        {
            get { return _nodeTableDictionary; }
        }

        public Dictionary<Tuple<string, string>, string> EdgeNodeBinding
        {
            get { return _edgeNodeBinding; }
        }

        public Dictionary<string, Tuple<WSchemaObjectName, WColumnReferenceExpression>> EdgeDictionary
        {
            get { return _edgeDictionary; }
        }

        /// <summary>
        /// Retrieves the mapping from ubound node properties to noda table aliases and edge attributes to edge aliases.
        /// If the caching dictionary is empty, updates the cache using columns information in the metatable.
        /// Otherwise, returns the caching dictionary.
        /// </summary>
        /// <param name="columnsOfNodeTables"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetColumnToAliasMapping(
            Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> columnsOfNodeTables)
        {
            if (_columnToAliasMapping == null)
            {
                _columnToAliasMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var duplicateColumns = new HashSet<string>();
                foreach (var kvp in NodeTableDictionary)
                {
                    var nodeTable = kvp.Value as WNamedTableReference;
                    if (nodeTable != null)
                    {
                        var nodeTableObjectName = nodeTable.TableObjectName;
                        var nodeTableTuple = WNamedTableReference.SchemaNameToTuple(nodeTableObjectName);
                        foreach (
                            var property in
                                columnsOfNodeTables[nodeTableTuple].Where(e => e.Value.Role != WNodeTableColumnRole.Edge)
                                    .Select(e => e.Key))
                        {
                            if (!_columnToAliasMapping.ContainsKey(property.ToLower()))
                            {
                                _columnToAliasMapping[property.ToLower()] = kvp.Key;
                            }
                            else
                            {
                                duplicateColumns.Add(property.ToLower());
                            }
                        }
                    }
                }
                foreach (var kvp in EdgeDictionary)
                {
                    var tuple = kvp.Value;
                    string schema = tuple.Item1.SchemaIdentifier.Value.ToLower();
                    string sourceTableName = tuple.Item1.BaseIdentifier.Value.ToLower();
                    string edgeName = tuple.Item2.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                    var bindNodeTableTuple =new Tuple<string, string>(schema, _edgeNodeBinding[new Tuple<string, string>(sourceTableName,edgeName)]);
                    var edgeProperties =
                        columnsOfNodeTables[bindNodeTableTuple][edgeName].EdgeInfo;
                    foreach (var attribute in edgeProperties.ColumnAttributes)
                    {
                        if (!_columnToAliasMapping.ContainsKey(attribute.ToLower()))
                        {
                            _columnToAliasMapping[attribute.ToLower()] = kvp.Key;
                        }
                        else
                        {
                            duplicateColumns.Add(attribute.ToLower());
                        }
                    }


                }
                foreach (var col in duplicateColumns)
                    _columnToAliasMapping.Remove(col);
            }
            return _columnToAliasMapping;
        }
    }
}
