using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;


namespace GraphView.TSQL_Syntax_Tree
{
    public partial class WCommonTableExpression:WSqlStatement
    {
        public IList<Identifier> Columns { get; set; }
        public Identifier ExpressionName { get; set; }
        public WSelectQueryExpression QueryExpression { get; set; }
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0};WITH [{1}]", indent, ExpressionName.Value);
            if (Columns!=null && Columns.Any())
            {
                sb.Append(" (");
                for (var i = 0; i < Columns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(Columns[i]);
                }
            }
            sb.AppendFormat("\r\n{0}AS\r\n",indent);
            sb.AppendFormat("{0}(\n",indent);
            sb.AppendFormat("{0}\r\n", QueryExpression.ToString(indent));
            sb.AppendFormat("{0})", indent);

            return sb.ToString();
        }

        public string ToStringBeginWithComma(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0},[{1}]", indent, ExpressionName.Value);
            if (Columns != null && Columns.Any())
            {
                sb.Append(" (");
                for (var i = 0; i < Columns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(Columns[i]);
                }
            }
            sb.AppendFormat("\r\n{0}AS\r\n", indent);
            sb.AppendFormat("{0}(\n", indent);
            sb.AppendFormat("{0}\r\n", QueryExpression.ToString(indent));
            sb.AppendFormat("{0})", indent);

            return sb.ToString();
        }


        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (QueryExpression != null)
                QueryExpression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WMultiCommonTableExpression : WSqlStatement
    {
        public IList<WCommonTableExpression> WCommonTableExpressions;
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            if (WCommonTableExpressions != null && WCommonTableExpressions.Any())
            {
                sb.Append(WCommonTableExpressions[0].ToString(indent));
                for (var i = 1; i < WCommonTableExpressions.Count; ++i)
                    sb.Append("\r\n").Append(WCommonTableExpressions[i].ToStringBeginWithComma(indent));
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
            if (WCommonTableExpressions != null)
            {
                for (var i = 0; i < WCommonTableExpressions.Count; ++i)
                    WCommonTableExpressions[i].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }
}
