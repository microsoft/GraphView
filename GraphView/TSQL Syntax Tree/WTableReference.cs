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
using System.Globalization;
using System.Text;
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
        internal int Low { get; set; }
        internal int High { get; set; }
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

    public partial class WCoalesceTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WConstantReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBoundBothEdgeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBoundOutEdgeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBoundInEdgeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WFlatMapTableReference : WSchemaObjectFunctionTableReference
    {

    }
    public partial class WKeyTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WLocalTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBoundBothNodeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBoundOutNodeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WPropertiesTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WPathTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WInjectTableReference : WSchemaObjectFunctionTableReference
    {

    }

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

    public partial class WValuesTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WValueTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WUnfoldTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WUnionTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WProjectTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WGroupTableReference : WSchemaObjectFunctionTableReference
    {
        
    }

    public partial class WAddETableReference : WSchemaObjectFunctionTableReference
    {
        public string ConstructEdgeJsonDocument(out List<string> projectedFieldList)
        {
            projectedFieldList = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);
            JObject edgeJsonDocument = new JObject();

            // Skip edgeSourceScalarFunction, edgeSinkScalarFunction, otherVTag
            for (var i = 3; i < Parameters.Count; i += 2)
            {
                var key = (Parameters[i] as WValueExpression).Value;
                //var value = (Parameters[i + 1] as WValueExpression).ToString();
                //edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, value, key).ToString();

                GraphViewJsonCommand.UpdateProperty(edgeJsonDocument, Parameters[i] as WValueExpression,
                    Parameters[i + 1] as WValueExpression);

                if (!projectedFieldList.Contains(key))
                    projectedFieldList.Add(key);
            }

            return edgeJsonDocument.ToString();
        }
    }

    public partial class WAddVTableReference : WSchemaObjectFunctionTableReference
    {
        public JObject ConstructNodeJsonDocument(out List<string> projectedFieldList)
        {
            JObject nodeJsonDocument = new JObject();
            projectedFieldList = new List<string>(GraphViewReservedProperties.ReservedNodeProperties);

            for (var i = 0; i < Parameters.Count; i += 2)
            {
                var key = (Parameters[i] as WValueExpression).Value;

                //nodeJsonDocument = GraphViewJsonCommand.insert_property(nodeJsonDocument, value, key).ToString();
                GraphViewJsonCommand.UpdateProperty(nodeJsonDocument, Parameters[i] as WValueExpression,
                    Parameters[i + 1] as WValueExpression);

                if (!projectedFieldList.Contains(key))
                    projectedFieldList.Add(key);
            }

            //nodeJsonDocument = GraphViewJsonCommand.insert_property(nodeJsonDocument, "[]", "_edge").ToString();
            //nodeJsonDocument = GraphViewJsonCommand.insert_property(nodeJsonDocument, "[]", "_reverse_edge").ToString();
            nodeJsonDocument["_edge"] = new JArray();
            nodeJsonDocument["_reverse_edge"] = new JArray();
            nodeJsonDocument["_nextEdgeOffset"] = 0;

            return nodeJsonDocument;
        }
    }

    public partial class WSideEffectTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WDedupTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WDropNodeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WDropEdgeTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WDropPropertiesTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WUpdateNodePropertiesTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WUpdateEdgePropertiesTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WStoreTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WBarrierTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WExpandTableReference : WSchemaObjectFunctionTableReference
    {

    }

    public partial class WMapTableReference : WSchemaObjectFunctionTableReference
    {

    }

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
