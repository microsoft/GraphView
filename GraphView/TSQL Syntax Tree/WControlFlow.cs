using System.Collections.Generic;

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
        internal override string ToString(string indent)
        {
            return "WChoose(" + ChooseDict.Count.ToString() + ")";
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
}
