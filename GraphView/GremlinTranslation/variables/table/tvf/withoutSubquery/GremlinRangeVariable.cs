using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRangeVariable : GremlinTableVariable
    {
        public int Low { get; set; }
        public int High { get; set; }
        public bool IsReverse { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinContextVariable InputVaribale { get; set; }

        public GremlinRangeVariable(GremlinVariable inputVariable, int low, int high, GremlinKeyword.Scope scope, bool isReverse): base(GremlinVariableType.Table)
        {
            InputVaribale = new GremlinContextVariable(inputVariable);
            Low = low;
            High = high;
            Scope = scope;
            IsReverse = isReverse;
        }

        internal override void Populate(string property)
        {
            InputVaribale.Populate(property);
            base.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(InputVaribale.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVaribale.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(Low));
            parameters.Add(SqlUtil.GetValueExpr(High));
            parameters.Add(Scope == GremlinKeyword.Scope.Local ? SqlUtil.GetValueExpr(1) : SqlUtil.GetValueExpr(-1));
            parameters.Add(IsReverse ? SqlUtil.GetValueExpr(1): SqlUtil.GetValueExpr(-1));

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Range, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}