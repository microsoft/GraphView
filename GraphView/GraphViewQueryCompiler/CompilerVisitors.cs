using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    /// <summary>
    /// The visitor that classifies table references in a FROM clause
    /// into named table references and others. A named table reference
    /// represents the entire collection of vertices in the graph. 
    /// Other table references correspond to query derived tables, 
    /// variable tables defined earlier in the script, or table-valued 
    /// functions.
    /// </summary>
    internal class TableClassifyVisitor : WSqlFragmentVisitor
    {
        private List<WNamedTableReference> vertexTableList;
        private List<WTableReferenceWithAlias> nonVertexTableList;
        
        public void Invoke(
            WFromClause fromClause, 
            List<WNamedTableReference> vertexTableList, 
            List<WTableReferenceWithAlias> nonVertexTableList)
        {
            this.vertexTableList = vertexTableList;
            this.nonVertexTableList = nonVertexTableList;

            foreach (WTableReference tabRef in fromClause.TableReferences)
            {
                tabRef.Accept(this);
            }
        }

        public override void Visit(WNamedTableReference node)
        {
            vertexTableList.Add(node);
        }

        public override void Visit(WQueryDerivedTable node)
        {
            nonVertexTableList.Add(node);
        }

        public override void Visit(WSchemaObjectFunctionTableReference node)
        {
            nonVertexTableList.Add(node);
        }

        public override void Visit(WVariableTableReference node)
        {
            nonVertexTableList.Add(node);
        }
    }

    /// <summary>
    /// The visitor that constructs AggregationBlocks according to side-effect 
    /// TVFs, global filter TVFs, global map TVFs, modification TVFs and some 
    /// special TVFs in a FROM clause. AggregationBlocks are used to optimize.
    /// </summary>
    internal class GlobalDependencyVisitor : WSqlFragmentVisitor
    {
        public List<AggregationBlock> blocks;

        public void Invoke(WFromClause fromClause)
        {
            blocks = new List<AggregationBlock>()
            {
                new AggregationBlock()
            };

            if (fromClause == null)
            {
                return;
            }

            foreach (WTableReference tabRef in fromClause.TableReferences)
            {
                tabRef.Accept(this);
            }
            for (int index = blocks.Count - 1; index >= 1; --index)
            {
                if (blocks[index].RootTableAlias == "dummy" && !blocks[index].TableInputDependency.Any())
                {
                    blocks.RemoveAt(index);
                }
            }
        }

        public override void Visit(WNamedTableReference node)
        {
            blocks.Last().AddTable(node);
        }

        public override void Visit(WQueryDerivedTable node)
        {
            blocks.Add(new AggregationBlock(node));
        }

        public override void Visit(WSchemaObjectFunctionTableReference node)
        {
            if (node is WAggregateTableReference ||
                node is WBarrierTableReference ||
                node is WBoundNodeTableReference ||
                node is WCoinTableReference ||
                node is WConstantReference ||
                node is WDedupGlobalTableReference ||
                node is WGroupTableReference ||
                node is WInjectTableReference ||
                node is WOrderGlobalTableReference ||
                node is WRangeGlobalTableReference ||
                node is WSampleGlobalTableReference ||
                node is WSelectOneTableReference ||
                node is WSelectColumnTableReference ||
                node is WStoreTableReference ||
                node is WSubgraphTableReference ||
                node is WTreeTableReference)
            {
                blocks.Add(new AggregationBlock(node));
            }
            else if (new ModificationVisitor().Invoke(node))
            {
                blocks.Add(new AggregationBlock(node));
            }
            else
            {
                blocks.Last().AddTable(node);
            }
        }

        public override void Visit(WVariableTableReference node)
        {
            blocks.Last().AddTable(node);
        }
    }

    /// <summary>
    /// The visitor that checks if a table reference has Modification TVFs or not.
    /// </summary>
    internal class ModificationVisitor : WSqlFragmentVisitor
    {
        public bool hasModification;

        public bool Invoke(WSqlFragment sqlFragment)
        {
            hasModification = false;
            if (sqlFragment != null)
            {
                sqlFragment.Accept(this);
            }
            return hasModification;
        }

        public override void Visit(WSchemaObjectFunctionTableReference node)
        {
            if (node is WAddETableReference ||
                node is WAddVTableReference ||
                node is WCommitTableReference ||
                node is WDropTableReference ||
                node is WUpdatePropertiesTableReference)
            {
                hasModification = true;
            }
            else
            {
                base.Visit(node);
            }
        }
    }

    /// <summary>
    /// The visitor that traverses the syntax tree and returns the columns 
    /// accessed in current query fragment for each provided table alias. 
    /// This visitor is used to determine what vertex/edge properties are projected 
    /// when a JSON query is sent to the underlying system to retrieve vertices and edges. 
    /// </summary>
    internal class AccessedTableColumnVisitor : WSqlFragmentVisitor
    {
        // A collection of table aliases and their columns
        // accessed in the query block
        Dictionary<string, HashSet<string>> accessedColumns;
        private bool _isOnlyTargetTableReferenced;

        public Dictionary<string, HashSet<string>> Invoke(WSqlFragment sqlFragment, System.Collections.IEnumerable targetTableReferences,
            out bool isOnlyTargetTableReferecend)
        {
            _isOnlyTargetTableReferenced = true;
            accessedColumns = new Dictionary<string, HashSet<string>>();

            if (sqlFragment != null)
            {
                foreach (string tabAlias in targetTableReferences)
                {
                    accessedColumns.Add(tabAlias, new HashSet<string>());
                }

                sqlFragment.Accept(this);

                foreach (string tableRef in targetTableReferences)
                {
                    if (accessedColumns[tableRef].Count == 0)
                    {
                        accessedColumns.Remove(tableRef);
                    }
                }
            }

            isOnlyTargetTableReferecend = _isOnlyTargetTableReferenced;
            return accessedColumns;
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            string columnName = node.ColumnName;
            string tableAlias = node.TableReference;

            if (tableAlias == null)
            {
                throw new QueryCompilationException("Identifier " + columnName + " must be bound to a table alias.");
            }

            if (accessedColumns.ContainsKey(tableAlias))
            {
                accessedColumns[tableAlias].Add(columnName);
            }
            else
            {
                _isOnlyTargetTableReferenced = false;
            }
        }

        public override void Visit(WMatchPath node)
        {
            foreach (var sourceEdge in node.PathEdgeList)
            {
                WSchemaObjectName source = sourceEdge.Item1;
                string tableAlias = source.BaseIdentifier.Value;
                WEdgeColumnReferenceExpression edge = sourceEdge.Item2;

                if (accessedColumns.ContainsKey(tableAlias))
                {
                    switch (edge.EdgeType)
                    {
                        case WEdgeType.OutEdge:
                            accessedColumns[tableAlias].Add(ColumnGraphType.OutAdjacencyList.ToString());
                            break;
                        case WEdgeType.InEdge:
                            accessedColumns[tableAlias].Add(ColumnGraphType.InAdjacencyList.ToString());
                            break;
                        case WEdgeType.BothEdge:
                            accessedColumns[tableAlias].Add(ColumnGraphType.BothAdjacencyList.ToString());
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Turn a SQL-style boolean WValueExpression to lower case
    /// </summary>
    internal class BooleanWValueExpressionVisitor : WSqlFragmentVisitor
    {
        public void Invoke(
            WBooleanExpression booleanExpression)
        {
            if (booleanExpression != null)
                booleanExpression.Accept(this);
        }

        public override void Visit(WValueExpression valueExpression)
        {
            bool bool_value;
            // JSON requires a lower case string if it is a boolean value
            if (!valueExpression.SingleQuoted && bool.TryParse(valueExpression.Value, out bool_value))
                valueExpression.Value = bool_value.ToString().ToLowerInvariant();
        }
    }

    /// <summary>
    /// Transfrom WColumnReferenceExpression for JsonQuery
    /// e.g. After Invoke((WHERE N_0.age = 27 AND N_0.flag = true))
    /// The booleanExpression.toString() will show
    /// (WHERE age._value = 27 AND flag._value = true)
    /// </summary>
    internal class NormalizeNodePredicatesWColumnReferenceExpressionVisitor : WSqlFragmentVisitor
    {
        //
        // <key: encode name with only letters, digits and underscore
        //  value: original column name>
        //
        private readonly Dictionary<string, string> referencedProperties;
        private readonly HashSet<string> flatProperties;
        private readonly HashSet<string> skipTableNames;

        public void AddFlatProperties(HashSet<string> ps)
        {
            foreach (string s in ps)
            {
                this.flatProperties.Add(s);
            }
        }

        public void AddSkipTableName(string name)
        {
            this.skipTableNames.Add(name);
        }

        public NormalizeNodePredicatesWColumnReferenceExpressionVisitor(string partitionKey)
        {
            this.referencedProperties = new Dictionary<string, string>();
            this.flatProperties = new HashSet<string> { GremlinKeyword.NodeID, GremlinKeyword.Label };
            this.skipTableNames = new HashSet<string>();
            if (partitionKey != null) {
                this.flatProperties.Add(partitionKey);
            }
        }

        public Dictionary<string, string> Invoke(WBooleanExpression booleanExpression)
        {
            if (booleanExpression != null)
                booleanExpression.Accept(this);

            return this.referencedProperties;
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            IList<Identifier> columnList = columnReference.MultiPartIdentifier.Identifiers;
            string propertyName = "";

            if (columnList.Count == 2)
            {
                string tableName = columnList[0].Value;
                if (this.skipTableNames.Contains(tableName))
                {
                    return;
                }

                string originalColumnName = columnList[1].Value;
                if (this.flatProperties.Contains(originalColumnName)) {
                    return;
                }

                string encodeName = EncodeString(originalColumnName);
                this.referencedProperties[encodeName] = originalColumnName;
                columnList[0].Value = encodeName;
                columnList[1].Value = DocumentDBKeywords.KW_PROPERTY_VALUE;
            }
            else {
                throw new QueryCompilationException("Identifier " + columnList + " should be bound to a table.");
            }
        }

        private static string EncodeString(string str)
        {
            char[] result = new char[str.Length * 6];
            int idx = 0;
            result[idx++] = 'D';
            foreach (char ch in str)
            {
                if (char.IsDigit(ch) ||
                    (ch >= 'A' && ch <= 'Z') ||
                    ch >= 'a' && ch <= 'z') {
                    result[idx++] = ch;
                }
                else {
                    result[idx++] = '_';
                    result[idx++] = 'x';
                    string tmp = Convert.ToString((int)ch, 16).ToUpper();
                    foreach (char c in tmp) {
                        result[idx++] = c;
                    }
                    result[idx++] = '_';
                }
            }
            return new string(result, 0, idx);
        }
    }

    /// <summary>
    /// DMultiPartIdentifierVisitor traverses a boolean expression and
    /// change all the WMultiPartIdentifiers to DMultiPartIdentifiers for normalization
    /// </summary>
    internal class DMultiPartIdentifierVisitor : WSqlFragmentVisitor
    {
        public HashSet<string> NeedsConvertion;

        public DMultiPartIdentifierVisitor()
        {
            this.NeedsConvertion = new HashSet<string>();
        }

        public void Invoke(WBooleanExpression booleanExpression)
        {
            if (booleanExpression != null) {
                booleanExpression.Accept(this);
            }
        }

        //
        // E_0.|id => E_0['|id']
        //
        public override void Visit(WColumnReferenceExpression node)
        {
            if (this.NeedsConvertion.Count == 0 || this.NeedsConvertion.Contains(node.TableReference))
            {
                node.MultiPartIdentifier = new DMultiPartIdentifier(node.MultiPartIdentifier);
            }
        }
    }


    /// <summary>
    /// Return how many times have GraphView runtime functions appeared in a BooleanExpression
    /// </summary>
    internal class GraphviewRuntimeFunctionCountVisitor : WSqlFragmentVisitor
    {
        private int runtimeFunctionCount;

        public int Invoke(
            WBooleanExpression booleanExpression)
        {
            runtimeFunctionCount = 0;

            if (booleanExpression != null)
                booleanExpression.Accept(this);

            return runtimeFunctionCount;
        }

        public override void Visit(WExistsPredicate existsPredicate)
        {
            this.runtimeFunctionCount++;
            existsPredicate.AcceptChildren(this);
        }

        public override void Visit(WFunctionCall fcall)
        {
            switch (fcall.FunctionName.Value.ToLowerInvariant())
            {
                case "withinarray":
                case "withoutarray":
                case "hasproperty":
                    runtimeFunctionCount++;
                    break;
            }
        }
    }

    internal class JsonServerStringArrayUnfoldVisitor : WSqlFragmentVisitor
    {
        private readonly HashSet<string> flatProperties;
        private readonly HashSet<string> skipTableNames;

        public JsonServerStringArrayUnfoldVisitor(HashSet<string> flatProperties)
        {
            this.flatProperties = flatProperties;
            this.flatProperties.Add(GremlinKeyword.NodeID);
            this.flatProperties.Add(GremlinKeyword.Label);
            this.skipTableNames = new HashSet<string>();
        }

        public void AddFlatAttribute(string attr)
        {
            this.flatProperties.Add(attr);
        }

        public void AddSkipTableName(string table)
        {
            this.skipTableNames.Add(table);
        }

        public void Invoke(WBooleanExpression booleanExpression)
        {
            booleanExpression?.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            IList<Identifier> columnList = columnReference.MultiPartIdentifier.Identifiers;
            string propertyName = "";

            if (columnList.Count == 2)
            {
                string tableName = columnList[0].Value;
                if (this.skipTableNames.Contains(tableName))
                {
                    return;
                }

                string originalColumnName = columnList[1].Value;
                if (this.flatProperties.Contains(originalColumnName))
                {
                    return;
                }

                columnReference.AddIdentifier("*");
                columnReference.AddIdentifier("_value");
            }
            else
            {
                throw new QueryCompilationException("Identifier " + columnList + " should be bound to a table.");
            }
        }
    }

    /// <summary>
    /// BooleanExprNormalizeVisitor traverses a boolean expression 
    /// and normalizes it to a list of conjunctive predicates.
    /// </summary>
    internal class BooleanExpressionNormalizeVisitor : WSqlFragmentVisitor
    {
        private List<WBooleanExpression> normalizedList;

        public List<WBooleanExpression> Invoke(WBooleanExpression expr)
        {
            this.normalizedList = new List<WBooleanExpression>();
            expr.Accept(this);

            return this.normalizedList;
        }

        private void Extract(WBooleanExpression expr)
        {
            this.normalizedList.Add(expr);
        }

        public override void Visit(WBooleanBinaryExpression node)
        {
            if (node.BooleanExpressionType == BooleanBinaryExpressionType.And)
            {
                base.Visit(node);
            }
            else
            {
                this.Extract(new WBooleanParenthesisExpression { Expression = node });
            }
        }

        public override void Visit(WBooleanComparisonExpression node)
        {
            this.Extract(node);
        }

        public override void Visit(WBooleanIsNullExpression node)
        {
            //Extract(node);
            throw new QueryCompilationException("WBooleanIsNullExpression is not supported yet.");
        }

        public override void Visit(WBetweenExpression node)
        {
            //Extract(node);
            throw new QueryCompilationException("WBetweenExpression is not supported yet.");
        }

        public override void Visit(WLikePredicate node)
        {
            //Extract(node);
            throw new QueryCompilationException("WBetweenExpression is not supported yet.");

        }

        public override void Visit(WInPredicate node)
        {
            this.Extract(node);
        }

        public override void Visit(WSubqueryComparisonPredicate node)
        {
            //Extract(node);
            throw new QueryCompilationException("WSubqueryComparisonPredicate is not supported yet.");
        }

        public override void Visit(WExistsPredicate node)
        {
            this.Extract(node);
        }

        public override void Visit(WBooleanNotExpression node)
        {
            this.Extract(node);
        }
    }
}
