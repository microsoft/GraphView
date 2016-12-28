using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public partial class WChoose : WSqlStatement
    {
        internal List<WSelectQueryBlock> InputExpr { get; set; }

        public override string ToString()
        {
            List<string> ChooseString = new List<string>();
            foreach (var x in InputExpr)
                ChooseString.Add(x.ToString());
            return string.Join("", ChooseString);
        }
    }

    public partial class WChoose2 : WTableReference
    {
        internal Dictionary<WScalarExpression, WSqlStatement> ChooseDict;
        internal WBooleanExpression PredicateExpr;
        internal WSqlStatement ChooseSqlStatement;
        internal Identifier Alias;
        internal override string ToString(string indent)
        {
            return "WChoose(" + ChooseDict.Count.ToString() + ") AS" + "[" + Alias.Value + "]";
        }
    }

    public partial class WCoalesce : WSqlStatement
    {
        internal List<WSqlFragment> InputExpr { get; set; }
        internal int CoalesceNumber { get; set; }
        public override string ToString()
        {
            List<string> ChooseString = new List<string>();
            foreach (var x in InputExpr)
                ChooseString.Add(x.ToString());
            return string.Join("", ChooseString);
        }
    }

    public partial class WAddV : WTableReference
    {
        internal WSqlStatement SqlStatement;
        internal Identifier Alias;
        internal override string ToString(string indent)
        {
            return "WAddV() AS" + "[" + Alias.Value + "]";
        }
    }

    public partial class WAddE : WTableReference
    {
        internal WSqlStatement SqlStatement;
        internal Identifier Alias;
        internal override string ToString(string indent)
        {
            return "WAddE() AS" + "[" + Alias.Value + "]";
        }
    }

    public class WRepeatPath: WSqlFragment
    {
        public WSelectQueryBlock SubQueryExpr;
        public bool IsEmitBefore;
        public bool IsEmitAfter;
        public bool IsEmitTrue;
        public bool IsUntilBefore;
        public bool IsUntilAfter;
        public bool IsTimes;
        public WSqlFragment ConditionSubQueryBlock;
        public WBooleanExpression ConditionBooleanExpr;
        public long Times;
        public Dictionary<string, string> Parameters;

        public WRepeatPath()
        {
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            sb.Append("( "+ SubQueryExpr.ToString() + " )");
            sb.Append("\r\n");
            sb.Append("Until ( " + ConditionSubQueryBlock.ToString() + " )");
            return sb.ToString();
        }
    }

    public class WRepeathNodePath : WRepeatPath
    {
        public WRepeathNodePath()
        {
        }
    }

    public class WRepeatEdgePath : WRepeatPath
    {
        public WEdgeColumnReferenceExpression EdgeAlias;

        public WRepeatEdgePath() { }
    }
}