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
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class WSqlTableContext
    {
        public WSqlTableContext UpperLevel { get; set; }

        private readonly Dictionary<MatchEdge, ColumnStatistics> _edgeStatisticses =
            new Dictionary<MatchEdge, ColumnStatistics>();

        private readonly Dictionary<MatchEdge, Dictionary<string, string>> _pathPredicatesValueDictionary =
            new Dictionary<MatchEdge, Dictionary<string, string>>();

        private readonly Dictionary<string, WTableReferenceWithAlias> _nodeTableDictionary =
            new Dictionary<string, WTableReferenceWithAlias>(StringComparer.CurrentCultureIgnoreCase);

        private readonly Dictionary<string, Tuple<WSchemaObjectName, WColumnReferenceExpression>> _edgeDictionary =
            new Dictionary<string, Tuple<WSchemaObjectName, WColumnReferenceExpression>>(StringComparer.CurrentCultureIgnoreCase);

        private Dictionary<string, string> _columnTableAliasMapping;

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

        public ColumnStatistics GetEdgeStatistics(MatchEdge edge)
        {
            return _edgeStatisticses[edge];
        }

        public Dictionary<string, string> GetPathPredicatesValueDict(MatchEdge edge)
        {
            return _pathPredicatesValueDictionary[edge];
        }

        public WTableReferenceWithAlias this[string name]
        {
            get
            {
                var currentContext = this;
                while (currentContext != null)
                {
                    WTableReferenceWithAlias value;
                    if (currentContext.NodeTableDictionary.TryGetValue(name, out value))
                        return value;
                    currentContext = currentContext.UpperLevel;
                }
                return null;
            }
        }

        public bool CheckTable(string name)
        {
            return this[name] != null;
        }

        /// <summary>
        /// Bind edge/edge view to node/node view
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="edgeColumn"></param>
        /// <param name="nodeTable"></param>
        /// <param name="graphMetaData"></param>
        /// <returns>Return null when there not exists the binding node/node view, 
        /// otherwise return name of the node/node view bound to the edge/edge view</returns>
        public string BindEdgeToNode(string schema, string edgeColumn, string nodeTable, GraphMetaData graphMetaData)
        {
            var edgeNodeTuple = new Tuple<string, string>(nodeTable, edgeColumn);
            string resNode;
            if (_edgeNodeBinding.TryGetValue(edgeNodeTuple, out resNode))
                return resNode;

            var nodeTuple = new Tuple<string, string>(schema, nodeTable);
            if (graphMetaData.ColumnsOfNodeTables[nodeTuple].ContainsKey(edgeColumn))
            {
                _edgeNodeBinding[edgeNodeTuple] = nodeTable.ToLower();
                return nodeTable;
            }
            else if (graphMetaData.NodeViewMapping.ContainsKey(nodeTuple))
            {
                foreach (var node in graphMetaData.NodeViewMapping[nodeTuple])
                {
                    if (graphMetaData.ColumnsOfNodeTables[new Tuple<string, string>(schema, node)].ContainsKey(edgeColumn))
                    {
                        if (string.IsNullOrEmpty(resNode))
                            resNode = node;
                        else
                            return null;
                    }
                }
                _edgeNodeBinding[edgeNodeTuple] = resNode;
                return resNode;
            }
            else
            {
                return null;
            }
        }

        public Dictionary<string, string> GetColumnTableMapping(
            Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> columnsOfNodeTables)
        {
            if (_columnTableAliasMapping == null)
            {
                _columnTableAliasMapping = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
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
                            if (!_columnTableAliasMapping.ContainsKey(property.ToLower()))
                            {
                                _columnTableAliasMapping[property.ToLower()] = kvp.Key;
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
                        if (!_columnTableAliasMapping.ContainsKey(attribute.ToLower()))
                        {
                            _columnTableAliasMapping[attribute.ToLower()] = kvp.Key;
                        }
                        else
                        {
                            duplicateColumns.Add(attribute.ToLower());
                        }
                    }


                }
                foreach (var col in duplicateColumns)
                    _columnTableAliasMapping.Remove(col);
            }
            return _columnTableAliasMapping;
        }

        public Dictionary<string, WTableReferenceWithAlias> GetUpperTableDictionary()
        {
            var currentContext = UpperLevel;
            var upperTableDictionary =
                new Dictionary<string, WTableReferenceWithAlias>(StringComparer.CurrentCultureIgnoreCase);
            while (currentContext != null)
            {
                foreach (var table in currentContext.NodeTableDictionary)
                    upperTableDictionary.Add(table.Key, table.Value);
                currentContext = currentContext.UpperLevel;
            }
            return upperTableDictionary;
        }

        public void AddNodeTable(string name, WTableReferenceWithAlias tableName)
        {
            if (_nodeTableDictionary.ContainsKey(name) || _edgeDictionary.ContainsKey(name))
                throw new GraphViewException("Duplicate Alias");
            _nodeTableDictionary.Add(name, tableName);
        }

        //public void AddEdgeReference(string name, WSchemaObjectName sourceNodeName, WEdgeColumnReferenceExpression edgeReference)
        public void AddEdgeReference(MatchEdge edge)
        {
            var edgeAlias = edge.EdgeAlias;
            if (_nodeTableDictionary.ContainsKey(edgeAlias) || _edgeDictionary.ContainsKey(edgeAlias))
                throw new GraphViewException("Duplicate Alias");
            _edgeDictionary.Add(edgeAlias,
                new Tuple<WSchemaObjectName, WColumnReferenceExpression>(edge.SourceNode.NodeTableObjectName,
                    edge.EdgeColumn));
        }

        public void AddEdgeStatistics(MatchEdge edge, ColumnStatistics statistics)
        {
            _edgeStatisticses.Add(edge, statistics);
        }

        public void AddPathPredicateValue(MatchEdge path, string attribute, string value)
        {
            var attrDict = _pathPredicatesValueDictionary.GetOrCreate(path);
            if (attrDict.ContainsKey(attribute.ToLower()))
                throw new GraphViewException(string.Format(
                    "Only one value can be specified for the given attribute {0}", attribute));
            attrDict.Add(attribute, value);
        }
    }
}
