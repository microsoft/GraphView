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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public enum QualifiedJoinType
    {
        FullOuter,
        Inner,
        LeftOuter,
        RightOuter
    }

    public enum JoinHint
    {
        Hash,
        Loop,
        Merge,
        None,
        Remote
    }

    public enum UnqualifiedJoinType
    {
        CrossApply, 
        CrossJoin, 
        OuterApply
    }

    public abstract partial class WTableReference : WSqlFragment
    {
        /// <summary>
        /// Returns a list of table references in this WTableReference expression.
        /// </summary>
        /// <returns>A list of table references</returns>
        internal virtual IList<string> TableAliases()
        {
            return new List<string>(); 
        }

    }
    public abstract partial class WTableReferenceWithAlias : WTableReference 
    {
        internal Identifier Alias { set; get; }
        //internal int Low { get; set; }
        //internal int High { get; set; }
        //internal bool IsLocal { get; set; }
        //internal bool IsReverse { get; set; }
    }

    public abstract partial class WTableReferenceWithAliasAndColumns : WTableReferenceWithAlias
    {
        internal IList<Identifier> Columns { set; get; }
    }

    public partial class WSpecialNamedTableReference : WTableReferenceWithAlias
    {
        internal WSchemaObjectName TableObjectName { get; set; }
        internal IList<WTableHint> TableHints { set; get; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            string tableObjectString = null;
            if (TableObjectName.ServerIdentifier != null)
            {
                tableObjectString = string.Format("[{0}]", TableObjectName.ServerIdentifier.Value);
            }
            if (TableObjectName.DatabaseIdentifier != null)
            {
                tableObjectString = string.Format(tableObjectString == null ? "[{0}]" : ".[{0}]", TableObjectName.DatabaseIdentifier.Value);
            }
            if (TableObjectName.SchemaIdentifier != null)
            {
                tableObjectString = string.Format(tableObjectString == null ? "[{0}]" : ".[{0}]", TableObjectName.SchemaIdentifier.Value);
            }
            if (TableObjectName.BaseIdentifier != null)
            {
                tableObjectString = string.Format(tableObjectString == null ? "{0}" : ".{0}", TableObjectName.BaseIdentifier.Value);
            }

            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1}", indent, tableObjectString);
            if (Alias != null)
                sb.Append(" AS " + string.Format("[{0}]", Alias.Value));
            if (TableHints != null && TableHints.Count > 0)
            {
                sb.Append(" WITH (");
                var index = 0;
                for (var count = TableHints.Count; index < count; ++index)
                {
                    if (index > 0)
                        sb.Append(", ");
                    sb.Append(TableHints[index]);
                }
                sb.Append(')');
            }

            return sb.ToString();
        }
    }

    public partial class WNamedTableReference : WTableReferenceWithAlias
    {
        internal WSchemaObjectName TableObjectName { set; get; }

        internal IList<WTableHint> TableHints { set; get; }

        // SchemaObjectName cannot be modified externally. 
        // We use this field to add a new table reference to the parsed tree.
        public string TableObjectString { set; get; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            TableObjectString = TableObjectName == null ? TableObjectString : TableObjectName.ToString();

            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1}", indent, TableObjectString);
            if (Alias != null)
                sb.Append(" AS " + string.Format("[{0}]", Alias.Value));
            if (TableHints != null && TableHints.Count > 0)
            {
                sb.Append(" WITH (");
                var index = 0;
                for (var count = TableHints.Count; index < count; ++index)
                {
                    if (index > 0)
                        sb.Append(", ");
                    sb.Append(TableHints[index]);
                }
                sb.Append(')');
            }

            return sb.ToString();
        }

        internal string ToStringWithoutRange()
        {
            TableObjectString = TableObjectName == null ? TableObjectString : TableObjectName.ToString();

            var sb = new StringBuilder();
            sb.AppendFormat("{0}", TableObjectString);
            if (Alias != null)
                sb.Append(" AS " + string.Format("[{0}]", Alias.Value));
            if (TableHints != null && TableHints.Count > 0)
            {
                sb.Append(" WITH (");
                var index = 0;
                for (var count = TableHints.Count; index < count; ++index)
                {
                    if (index > 0)
                        sb.Append(", ");
                    sb.Append(TableHints[index]);
                }
                sb.Append(')');
            }

            return sb.ToString();
        }

        internal override IList<string> TableAliases()
        {
            var aliases = new List<string>(1) { Alias != null ? Alias.Value : TableObjectName.BaseIdentifier.Value };

            return aliases;
        }

        internal static Tuple<string, string> SchemaNameToTuple(WSchemaObjectName name)
        {
            return
                name == null
                    ? null
                    : new Tuple<string, string>(
                        name.SchemaIdentifier == null
                            ? "dbo"
                            : name.SchemaIdentifier.Value.ToLower(CultureInfo.CurrentCulture),
                        name.BaseIdentifier.Value.ToLower(CultureInfo.CurrentCulture));
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public partial class WQueryDerivedTable : WTableReferenceWithAliasAndColumns
    {
        internal WSelectQueryExpression QueryExpr { set; get; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            sb.AppendFormat("{0}(\r\n", indent);
            sb.AppendFormat("{0}\r\n", QueryExpr.ToString(indent + "  "));
            sb.AppendFormat("{0}) AS [{1}]", indent, Alias.Value);

            if (Columns != null && Columns.Count > 0)
            {
                sb.Append('(');
                for (var i = 0; i < Columns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(Columns[i].Value);
                }
                sb.Append(')');
            }

            return sb.ToString();
        }

        internal override IList<string> TableAliases()
        {
            var aliases = new List<string>(1) { Alias.Value };

            return aliases;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (QueryExpr != null)
                QueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSchemaObjectFunctionTableReference : WTableReferenceWithAliasAndColumns
    {
        public IList<WScalarExpression> Parameters { get; set; }

        public WSchemaObjectName SchemaObject { get; set; }

        internal override bool OneLine()
        {
            if (Parameters == null)
            {
                return true;
            }

            foreach (WScalarExpression para in Parameters)
            {
                if (!para.OneLine())
                {
                    return false;
                }
            }

            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1}(", indent, SchemaObject);

            if (OneLine())
            {
                if (Parameters != null)
                {
                    for (int i = 0; i < Parameters.Count; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(", ");
                        }
                        sb.Append(Parameters[i].ToString(""));
                    }
                    sb.Append(")");
                }
            }
            else
            {
                sb.Append("\r\n");
                for (int i = 0; i < Parameters.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(",\r\n");
                    }
                    sb.Append(Parameters[i].ToString(indent + "  "));
                }
                sb.AppendFormat("\r\n{0})", indent);
            }

            if (Alias != null)
                sb.Append(" AS [" + Alias.Value + "]");

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SchemaObject != null)
                SchemaObject.Accept(visitor);
            if (Parameters != null)
            {
                var index = 0;
                for (var count = Parameters.Count; index < count; ++index)
                {
                    Parameters[index].Accept(visitor);
                }
            }
            base.AcceptChildren(visitor);
        }

        internal virtual GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            throw new NotImplementedException();
        }
    }

    public partial class WOptionalTableReference : WSchemaObjectFunctionTableReference
    {
        internal void Split(out WSelectQueryBlock contextSelect, out WSelectQueryBlock optionalSelectQuery)
        {
            WScalarSubquery optionalInput = Parameters[0] as WScalarSubquery;
            if (optionalInput == null)
            {
                throw new SyntaxErrorException("The input of an optional table reference must be a scalar subquery.");
            }
            WBinaryQueryExpression binaryQuery = optionalInput.SubQueryExpr as WBinaryQueryExpression;
            if (binaryQuery == null || binaryQuery.BinaryQueryExprType != BinaryQueryExpressionType.Union || !binaryQuery.All)
            {
                throw new SyntaxErrorException("The input of an optional table reference must be a UNION ALL binary query expression.");
            }

            contextSelect = binaryQuery.FirstQueryExpr as WSelectQueryBlock;
            optionalSelectQuery = binaryQuery.SecondQueryExpr as WSelectQueryBlock;

            if (contextSelect == null || optionalSelectQuery == null)
            {
                throw new SyntaxErrorException("The input of an optional table reference must be a UNION ALL binary query and the two sub-queries must be a select query block.");
            }
        }
    }

    public partial class WCoalesceTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WConstantReference : WSchemaObjectFunctionTableReference
    {
        public bool IsList { get; set; }
    }

    public partial class WDecomposeTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WFlatMapTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WKeyTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WBoundNodeTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WPropertiesTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WAllPropertiesTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WAllValuesTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WPathTableReference : WSchemaObjectFunctionTableReference
    {
        internal static void GetPathStepListAndByFuncList(
            QueryCompilationContext context, GraphViewCommand command,
            IList<WScalarExpression> parameters,
            out List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList,
            out List<ScalarFunction> byFuncList)
        {
            //
            // If the boolean value is true, then it's a subPath to be unfolded
            //
            pathStepList = new List<Tuple<ScalarFunction, bool, HashSet<string>>>();
            byFuncList = new List<ScalarFunction>();
            QueryCompilationContext byInitContext = new QueryCompilationContext(context);
            byInitContext.ClearField();
            byInitContext.AddField(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            foreach (WScalarExpression expression in parameters)
            {
                WFunctionCall basicStep = expression as WFunctionCall;
                WValueExpression stepLabel = expression as WValueExpression;
                WColumnReferenceExpression subPath = expression as WColumnReferenceExpression;
                WScalarSubquery byFunc = expression as WScalarSubquery;

                if (basicStep != null)
                {
                    pathStepList.Add(
                        new Tuple<ScalarFunction, bool, HashSet<string>>(
                            basicStep.CompileToFunction(context, command), false, new HashSet<string>()));
                }
                else if (stepLabel != null)
                {
                    if (!pathStepList.Any())
                    {
                        pathStepList.Add(new Tuple<ScalarFunction, bool, HashSet<string>>(null, false, new HashSet<string>()));
                    }
                    pathStepList.Last().Item3.Add(stepLabel.Value);
                }
                else if (subPath != null)
                {
                    pathStepList.Add(
                        new Tuple<ScalarFunction, bool, HashSet<string>>(
                            subPath.CompileToFunction(context, command), true, new HashSet<string>()));
                }
                else if (byFunc != null)
                {
                    byFuncList.Add(byFunc.CompileToFunction(byInitContext, command));
                }
                else {
                    throw new QueryCompilationException(
                        "The parameter of WPathTableReference can only be a WFunctionCall/WValueExpression/WColumnReferenceExpression/WScalarSubquery.");
                }
            }
        }
    }

    public partial class WInjectTableReference : WSchemaObjectFunctionTableReference
    {
        public bool IsList { get; set; }
    }

    public partial class WOrderTableReference : WSchemaObjectFunctionTableReference
    {
        public List<Tuple<WScalarExpression, IComparer>> OrderParameters { get; set; }
    }

    public partial class WOrderLocalTableReference : WOrderTableReference {}

    public partial class WOrderGlobalTableReference : WOrderTableReference {}

    public partial class WRepeatTableReference : WSchemaObjectFunctionTableReference
    {
        internal void Split(out WSelectQueryBlock contextSelect, out WSelectQueryBlock repeatSelectQuery)
        {
            WScalarSubquery repeatInput = Parameters[0] as WScalarSubquery;
            if (repeatInput == null)
            {
                throw new SyntaxErrorException("The input of a repeat table reference must be a scalar subquery.");
            }
            WBinaryQueryExpression binaryQuery = repeatInput.SubQueryExpr as WBinaryQueryExpression;
            if (binaryQuery == null || binaryQuery.BinaryQueryExprType != BinaryQueryExpressionType.Union || !binaryQuery.All)
            {
                throw new SyntaxErrorException("The input of a repeat table reference must be a UNION ALL binary query expression.");
            }

            contextSelect = binaryQuery.FirstQueryExpr as WSelectQueryBlock;
            repeatSelectQuery = binaryQuery.SecondQueryExpr as WSelectQueryBlock;

            if (contextSelect == null || repeatSelectQuery == null)
            {
                throw new SyntaxErrorException("The input of a repeat table reference must be a UNION ALL binary query and the two sub-queries must be a select query block.");
            }
        }
    }

    public partial class WSampleGlobalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSampleLocalTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WValuesTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WValueTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WUnfoldTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WUnionTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMatchTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMatchStartTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMatchEndTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WProjectTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WGroupTableReference : WSchemaObjectFunctionTableReference
    {
        public bool IsProjectingACollection { get; set; }
    }

    public partial class WTreeTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WValueMapTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WPropertyMapTableReference : WSchemaObjectFunctionTableReference { }

    public abstract partial class WEdgeToVertexTableReference : WSchemaObjectFunctionTableReference
    {
        internal abstract TraversalOperator.TraversalTypeEnum GetTraversalTypeParameter();
    }

    public partial class WEdgeToSourceVertexTableReference : WEdgeToVertexTableReference { }

    public partial class WEdgeToSinkVertexTableReference : WEdgeToVertexTableReference { }

    public partial class WEdgeToOtherVertexTableReference : WEdgeToVertexTableReference { }

    public partial class WEdgeToBothVertexTableReference : WEdgeToVertexTableReference { }

    public abstract partial class WVertexToEdgeTableReference : WSchemaObjectFunctionTableReference
    {
        internal abstract Tuple<bool, bool> GetAdjListDecoderCrossApplyTypeParameter();
    }

    public partial class WVertexToForwardEdgeTableReference : WVertexToEdgeTableReference { }

    public partial class WVertexToBackwordEdgeTableReference : WVertexToEdgeTableReference { }

    public partial class WVertexToBothEdgeTableReference : WVertexToEdgeTableReference { }

    public partial class WChooseTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WChooseWithOptionsTableReference : WSchemaObjectFunctionTableReference
    {
        private bool IsOptionNone(WValueExpression option)
        {
            return !option.SingleQuoted && option.Value.Equals("null", StringComparison.OrdinalIgnoreCase);
        }
    }


    public class WPropertyExpression : WPrimaryExpression
    {
        /// <summary>
        /// Indicate whether this property is to append(=list) or override(=single)
        /// </summary>
        public GremlinKeyword.PropertyCardinality Cardinality { get; set; }

        /// <summary>
        /// Property's name
        /// </summary>
        public WValueExpression Key { get; set; }

        /// <summary>
        /// Property's value
        /// </summary>
        public WScalarExpression Value { get; set; }

        /// <summary>
        /// Only valid for vertex property
        /// </summary>
        public Dictionary<WValueExpression, WScalarExpression> MetaProperties { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            Key?.Accept(visitor);
            Value?.Accept(visitor);

            foreach (KeyValuePair<WValueExpression, WScalarExpression> kvp in MetaProperties)
            {
                kvp.Key.Accept(visitor);
                kvp.Value.Accept(visitor);
            }

            base.AcceptChildren(visitor);
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}({1}, {2}, {3}",
                indent,
                Cardinality == GremlinKeyword.PropertyCardinality.List ? "list" : "single",
                Key.ToString(), Value.ToString());
            if (MetaProperties.Count > 0)
            {
                sb.Append(", Meta: (");
                bool isFirst = true;
                foreach (var metaProperty in MetaProperties)
                {
                    sb.Append($"{(isFirst ? "" : ", ")}{metaProperty.Key}:{metaProperty.Value}");
                    isFirst = false;
                }
                sb.Append(")");
            }
            sb.Append(")");
            return sb.ToString();
        }
    }

    public partial class WAddVTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WAddETableReference : WSchemaObjectFunctionTableReference { }
    
    public partial class WSideEffectTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WDedupGlobalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WDedupLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WDropTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WUpdatePropertiesTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WStoreTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSubgraphTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WAggregateTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WBarrierTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WCommitTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WExpandTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMapTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WCoinTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WCountLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMaxLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMinLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WMeanLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSumLocalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSimplePathTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WCyclicPathTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WPath2TableReference : WSchemaObjectFunctionTableReference {}

    public partial class WRangeGlobalTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WRangeLocalTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WSelectTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSelectOneTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WSelectColumnTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WIdTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WLabelTableReference : WSchemaObjectFunctionTableReference {}

    public partial class WFilterTableReference : WSchemaObjectFunctionTableReference { }

    public partial class WJoinParenthesisTableReference : WTableReference
    {
        internal WTableReference Join { get; set; }

        internal override bool OneLine()
        {
            return Join.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format("{0}({1})", indent, Join.ToString());
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("{0}(\r\n", indent);
                sb.AppendFormat("{0}{1}\r\n", indent, Join.ToString());
                sb.AppendFormat("{0})\r\n", indent);

                return sb.ToString();
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Join != null)
            {
                Join.Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WJoinTableReference : WTableReference
    {
        internal WTableReference FirstTableRef { set; get; }
        internal WTableReference SecondTableRef { set; get; }

        internal override IList<string> TableAliases()
        {
            var a1 = FirstTableRef.TableAliases();
            var a2 = SecondTableRef.TableAliases();

            var aliases = new List<string>(a1.Count + a2.Count);
            aliases.AddRange(a1);
            aliases.AddRange(a2);

            return aliases;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstTableRef != null)
                FirstTableRef.Accept(visitor);
            if (SecondTableRef != null)
                SecondTableRef.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WQualifiedJoin : WJoinTableReference
    {
        internal QualifiedJoinType QualifiedJoinType { set; get; }
        internal WBooleanExpression JoinCondition { set; get; }
        internal JoinHint JoinHint { set; get; }

        internal override bool OneLine()
        {
            return FirstTableRef.OneLine() &&
                   SecondTableRef.OneLine() &&
                   JoinCondition.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            sb.Append(FirstTableRef.ToString(indent)+"\n");

            sb.AppendFormat(" {1}{0}\n ", TsqlFragmentToString.JoinType(QualifiedJoinType, JoinHint), indent);

            //if (SecondTableRef.OneLine())
            //{
            //    sb.Append(SecondTableRef.ToString());
            //}
            //else
            //{
                //sb.Append("\r\n");
                sb.Append(SecondTableRef.ToString(indent));
            //}

            sb.Append("\n"+indent +"ON ");

            //if (JoinCondition.OneLine())
            //{
            //    sb.Append(JoinCondition.ToString());
            //}
            //else
            //{
                //sb.Append("\r\n");
                sb.Append(JoinCondition.ToString(""));
            //}

            return sb.ToString();
        }


        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (JoinCondition != null)
                JoinCondition.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WUnqualifiedJoin : WJoinTableReference
    {
        public UnqualifiedJoinType UnqualifiedJoinType { get; set; }
        internal override bool OneLine()
        {
            if (FirstTableRef == null) return SecondTableRef.OneLine();
            return FirstTableRef.OneLine() && SecondTableRef.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            if (OneLine())
            {
                if (FirstTableRef != null)
                {
                    sb.AppendFormat("{0}{1} ", indent, FirstTableRef.ToString(""));
                }
                sb.Append(TsqlFragmentToString.JoinType(UnqualifiedJoinType));
                sb.AppendFormat(" {0}", SecondTableRef.ToString(""));
            }
            else
            {
                if (FirstTableRef != null)
                {
                    sb.Append(FirstTableRef.ToString(indent + "  "));
                    sb.Append("\r\n");
                }
                sb.AppendFormat("{0}  {1}\r\n", indent, TsqlFragmentToString.JoinType(UnqualifiedJoinType));
                sb.Append(SecondTableRef.ToString(indent + "  "));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public partial class WParenthesisTableReference : WTableReference
    {
        internal WTableReference Table { get; set; }

        internal override bool OneLine()
        {
            return Table.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}({1})", indent, Table.ToString(""));
            }
            return string.Format(CultureInfo.CurrentCulture, "{0}(\r\n{1})", indent, Table.ToString(indent + "    "));
        }

        internal override IList<string> TableAliases()
        {
            return Table.TableAliases();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Table != null)
                Table.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public enum VariableTableType
    {
        Unknown,
        Vertex,
        Edge,
        Value,
        Hybrid
    }

    public partial class WVariableTableReference : WTableReferenceWithAlias
    {
        internal WVariableReference Variable { get; set; }
        //internal VariableTableType VariableTableType { get; set; }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1} as [{2}]", indent, Variable.ToString(), Alias.Value);
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor == null)
                return;
            visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (this.Variable != null)
                this.Variable.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }
}
