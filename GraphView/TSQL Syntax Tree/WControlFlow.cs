using System.Collections.Generic;
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

    public partial class WOptional : WTableReference
    {
        internal WSqlStatement SqlStatement;
        internal Identifier Alias;
        internal override string ToString(string indent)
        {
            return "WOptional() AS" + "[" + Alias.Value + "]";
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

    public partial class WCoalesce2 : WTableReference
    {
        internal Identifier Alias;
        internal List<WSqlStatement> CoalesceQuery;

        internal override string ToString(string indent)
        {
            return "WCoalesce2(" + CoalesceQuery.Count.ToString() + ") AS" + "[" + Alias.Value + "]";
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

    public partial class WSideEffect : WTableReference
    {
        internal WSqlStatement SqlStatement;
        internal Identifier Alias;
        internal override string ToString(string indent)
        {
            return "WSideEffect() AS" + "[" + Alias.Value + "]";
        }
    }

    public partial class WRepeat : WTableReference
    {
        internal WSqlStatement SqlStatement;
        internal WBooleanExpression UntilCondition;
        internal Identifier Alias;
        internal int MaxLoops;
        internal override string ToString(string indent)
        {
            return "WRepeat() AS" + "[" + Alias.Value + "]";
        }
    }
}
