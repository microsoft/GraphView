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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
//using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public enum BinaryExpressionType
    {
        Add,
        BitwiseAnd,
        BitwiseOr,
        BitwiseXor,
        StringConcatenate,
        Divide,
        Modulo,
        Multiply,
        Subtract
    }

    public enum UnaryExpressionType
    {
        BitwiseNot,
        Negative,
        Positive
    }

    public enum ColumnType
    {
        IdentityCol,
        PseudoColumnAction,
        PseudoColumnCuid,
        PseudoColumnIdentity,
        PseudoColumnRowGuid,
        Regular,
        RowGuidCol,
        Wildcard
    }

    public abstract partial class WScalarExpression : WSqlFragment 
    {
    }

    public partial class WBinaryExpression : WScalarExpression
    {
        internal WScalarExpression FirstExpr { get; set; }
        internal WScalarExpression SecondExpr { get; set; }
        internal BinaryExpressionType ExpressionType { get; set; }

        internal override bool OneLine()
        {
            return FirstExpr.OneLine() && SecondExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.Append(FirstExpr.ToString(indent));
            sb.AppendFormat(" {0} ", TsqlFragmentToString.BinaryExpressionType(ExpressionType));

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstExpr != null)
                FirstExpr.Accept(visitor);
            if (SecondExpr != null)
                SecondExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WUnaryExpression : WScalarExpression
    {
        internal UnaryExpressionType ExpressionType { get; set; }
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}{1}{2}", indent, 
                    TsqlFragmentToString.UnaryExpressionType(ExpressionType), Expression.ToString(""));
            }
            else
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}{1}\r\n{2}", indent,
                    TsqlFragmentToString.UnaryExpressionType(ExpressionType), Expression.ToString(indent));
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WPrimaryExpression : WScalarExpression { }

    public partial class WEdgeColumnReferenceExpression : WColumnReferenceExpression
    {
        internal WEdgeType EdgeType;
        internal string FirstEdgeAlias;

        internal string Alias;
        internal int MinLength { get; set; }
        internal int MaxLength { get; set; }
        internal Dictionary<string, string> AttributeValueDict { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(64);
            if (Alias == null || Alias.Length == 0) 
            {
                sb.Append(string.Format("{0}{1}", indent, MultiPartIdentifier.ToString()));
            }
            else
            {
                sb.Append(string.Format("{0}{1} AS {2}", indent, MultiPartIdentifier.ToString(), Alias));
            }
            return sb.ToString();
        }
    }

    public enum WEdgeType
    {
        Undefined,
        InEdge,
        OutEdge,
        BothEdge
    }

    public enum ColumnGraphType
    {
        Value,
        VertexId,
        VertexObject,
        EdgeObject,
        EdgeSource,
        EdgeId,
        EdgeSink,
        OutAdjacencyList,
        InAdjacencyList,
        BothAdjacencyList
    }

    /// <summary>
    /// A reference to a column. 
    /// 
    /// Columns in the SELECT clause are wrapped by the WSelectElement class.
    /// This class represents columns as scalar expressions.
    /// In particular, when WColumnReferenceExpression is of type *, it appears in function calls such as COUNT(*)
    /// </summary>
    public partial class WColumnReferenceExpression : WPrimaryExpression
    {
        internal WMultiPartIdentifier MultiPartIdentifier { get; set; }
        internal ColumnType ColumnType { get; set; }
        internal ColumnGraphType ColumnGraphType { get; set; }

        public WColumnReferenceExpression() { }

        public WColumnReferenceExpression(string tableName, string columnName)
        {
            ColumnType = ColumnType.Regular;

            Identifier tableIdent = tableName != null ? new Identifier { Value = tableName } : null;
            Identifier columnIdent = columnName != null ? new Identifier { Value = columnName } : null;

            MultiPartIdentifier = new WMultiPartIdentifier(tableIdent, columnIdent);
        }

        public WColumnReferenceExpression(string tableName, string columnName, ColumnGraphType columnGraphType)
        {
            ColumnType = ColumnType.Regular;
            ColumnGraphType = columnGraphType;

            Identifier tableIdent = tableName != null ? new Identifier { Value = tableName } : null;
            Identifier columnIdent = columnName != null ? new Identifier { Value = columnName } : null;

            MultiPartIdentifier = new WMultiPartIdentifier(tableIdent, columnIdent);
        }

        internal void Add(Identifier identifier)
        {
            if (MultiPartIdentifier == null)
            {
                MultiPartIdentifier = new WMultiPartIdentifier();
            }
            if (MultiPartIdentifier.Identifiers == null)
                MultiPartIdentifier.Identifiers = new List<Identifier>();

            MultiPartIdentifier.Identifiers.Add(identifier);
        }

        internal void AddIdentifier(string identValue)
        {
            var ident = new Identifier { Value = identValue };
            Add(ident);
        }

        public string ColumnName
        {
            get
            {
                switch (MultiPartIdentifier.Count)
                {
                    case 1:
                        return MultiPartIdentifier[0].Value;
                    case 2:
                        return MultiPartIdentifier[1].Value;
                    default:
                        return null;
                }
            }
        }

        public string TableReference
        {
            get
            {
                return MultiPartIdentifier.Count == 2 ? MultiPartIdentifier[0].Value : null;
            }
        }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            switch (ColumnType)
            {
                case ColumnType.Regular:
                {
                    return MultiPartIdentifier != null
                        ? string.Format(CultureInfo.CurrentCulture, "{0}{1}", indent,
                            MultiPartIdentifier)
                        : "";
                }
                case ColumnType.Wildcard:
                    {
                        return string.Format(CultureInfo.CurrentCulture, "{0}*", indent);
                    }
                default:
                    // throw new GraphViewException("Undefined column type");
                    return MultiPartIdentifier != null
                        ? string.Format(CultureInfo.CurrentCulture, "{0}{1}", indent,
                            MultiPartIdentifier)
                        : "";
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override bool Equals(object obj)
        {
            WColumnReferenceExpression colRef = obj as WColumnReferenceExpression;
            if (colRef == null || MultiPartIdentifier.Count != colRef.MultiPartIdentifier.Count)
            {
                return false;
            }

            for (int i = 0; i < MultiPartIdentifier.Count; i++)
            {
                if (MultiPartIdentifier[i].Value != colRef.MultiPartIdentifier[i].Value)
                {
                    return false;
                }
            }

            return true;
        }

        public override int GetHashCode()
        {
            return ToString("").GetHashCode();
        }
    }

    public partial class WScalarSubquery : WPrimaryExpression
    {
        //internal WSelectQueryExpression SubQueryExpr { get; set; }
        internal WSqlStatement SubQueryExpr { get; set; }

        internal override bool OneLine()
        {
            return SubQueryExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format("{0}({1})", indent, SubQueryExpr.ToString(""));
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("{0}(\r\n", indent);
                sb.Append(SubQueryExpr.ToString(indent + "  "));
                sb.AppendFormat("\r\n{0})", indent);

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
            if (SubQueryExpr != null)
                SubQueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WFunctionCall : WPrimaryExpression
    {
        internal WCallTarget CallTarget { get; set; }
        internal Identifier FunctionName { get; set; }
        internal IList<WScalarExpression> Parameters { get; set; }
        internal UniqueRowFilter UniqueRowFilter { get; set; }

        internal override bool OneLine()
        {
            return Parameters == null || Parameters.All(pe => pe.OneLine());
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            var newLine = false;

            if (CallTarget != null)
                sb.AppendFormat("{0}{1}.{2}(", indent, CallTarget, FunctionName.Value);
            else
                sb.AppendFormat("{0}{1}(", indent, FunctionName.Value);

            if (UniqueRowFilter == UniqueRowFilter.Distinct)
                sb.Append("DISTINCT ");

            for (var i = 0; i < Parameters.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (Parameters[i].OneLine())
                {
                    sb.Append(Parameters[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(Parameters[i].ToString(indent + " "));
                    newLine = true;
                }
            }

            if (newLine)
            {
                sb.AppendFormat("\r\n{0})", indent);
            }
            else
            {
                sb.Append(")");
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Parameters != null)
            {
                var index = 0;
                for (var count = Parameters.Count; index < count; ++index)
                    Parameters[index].Accept(visitor);
            }
            if (CallTarget != null)
                CallTarget.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// A value expression can be a variable or a literal. 
    /// </summary>
    public partial class WValueExpression : WPrimaryExpression 
    {
        internal string Value { get; set; }
        internal bool SingleQuoted { get; set; }

        public WValueExpression(string value, bool quoted)
        {
            Value = value;
            SingleQuoted = quoted;
        }
        public WValueExpression() { }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            return string.Format(CultureInfo.CurrentCulture, SingleQuoted ? "{0}'{1}'" : "{0}{1}", indent, Value);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public partial class WVariableReference : WValueExpression
    {
        public WVariableReference() {}

        public string Name { get; set; }

        internal override string ToString(string indent)
        {
            return Name;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            //TODO
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            //TODO
        }
    }

    public partial class WParenthesisExpression : WPrimaryExpression
    {
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (Expression.OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}({1})", indent, Expression.ToString(""));
            }
            else
            {
                var sb = new StringBuilder(128);

                sb.AppendFormat("{0}(\r\n", indent);
                sb.AppendFormat("{0}\r\n", Expression.ToString(indent));
                sb.AppendFormat("{0})", indent);

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
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WCaseExpression : WPrimaryExpression
    {
        internal WScalarExpression ElseExpr { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (ElseExpr != null)
                ElseExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSearchedCaseExpression : WCaseExpression
    {
        internal IList<WSearchedWhenClause> WhenClauses;

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();

            sb.AppendFormat("{0}( CASE", indent);
            var newIndent = indent + "    ";

            foreach (var t in WhenClauses)
            {
                sb.Append("\r\n");
                sb.Append(t.ToString(newIndent));
            }

            if (ElseExpr != null)
            {
                sb.Append("\r\n");
                sb.AppendFormat("{0}ELSE ", newIndent);

                if (ElseExpr.OneLine())
                {
                    sb.Append(ElseExpr.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(ElseExpr.ToString(newIndent + " "));
                }
            }

            sb.AppendFormat("\r\n{0}END )", indent);

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (WhenClauses != null)
            {
                var index = 0;
                for (var count = WhenClauses.Count; index < count; ++index)
                    WhenClauses[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }

    }

    public partial class WCastCall : WPrimaryExpression
    {
        internal WScalarExpression Parameter { get; set; }
        internal WDataTypeReference DataType { get; set; }

        internal override bool OneLine()
        {
            return Parameter.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(128);

            sb.AppendFormat("{0}CAST(", indent);

            if (Parameter.OneLine())
            {
                sb.Append(Parameter.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(Parameter.ToString(indent + " "));
            }

            sb.AppendFormat(" AS {0})", DataType.ToString(""));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Parameter != null)
                Parameter.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public class WRepeatConditionExpression: WPrimaryExpression
    {
        internal bool EmitContext { get; set; }
        internal bool StartFromContext { get; set; }
        internal int RepeatTimes { get; set; }
        internal WBooleanExpression EmitCondition { get; set; }
        internal WBooleanExpression TerminationCondition { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (EmitCondition != null)
                EmitCondition.Accept(visitor);
            if (TerminationCondition != null)
                TerminationCondition.Accept(visitor);
            base.AcceptChildren(visitor);
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("{0}RepeatCondition(", indent);
            if (RepeatTimes >= 0)
            {
                sb.Append(RepeatTimes);
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(TerminationCondition?.ToString(indent + "  "));
            }

            if (EmitCondition != null)
            {
                sb.Append(",\r\n");
                sb.Append(EmitCondition?.ToString(indent + "  "));
            }

            sb.Append(")");

            return sb.ToString();
        }
    }

    public class WColumnNameList : WPrimaryExpression
    {
        internal List<WValueExpression> ColumnNameList { get; set; }

        public WColumnNameList(List<WValueExpression> columnNameList)
        {
            ColumnNameList = columnNameList;
        }

        internal override bool OneLine()
        {
            return true;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("{0}(", indent);
            for (var i = 0; i < ColumnNameList.Count; i++)
            {
                if (i == 0)
                    sb.AppendFormat("{0}", ColumnNameList[i].ToString(""));
                else
                    sb.AppendFormat(", {0}", ColumnNameList[i].ToString(""));
            }

            sb.Append(")");

            return sb.ToString();
        }
    }

    public enum QuoteType
    {
        NotQuoted = 0,
        DoubleQuote, 
        SquareBracket
    }

    public class Identifier : WSqlFragment
    {
        public string Value { get; set; }

        public QuoteType QuoteType { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            switch(QuoteType)
            {
                case QuoteType.DoubleQuote:
                    return string.Format("{0}\"{1}\"", indent, Value);
                case QuoteType.SquareBracket:
                    return string.Format("{0}[{1}]", indent, Value);
                default:
                    return indent + Value;
            }
        }
    }
}
