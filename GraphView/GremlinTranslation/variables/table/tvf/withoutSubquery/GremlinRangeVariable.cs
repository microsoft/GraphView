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
            this.InputVaribale = new GremlinContextVariable(inputVariable);
            this.Low = low;
            this.High = high;
            this.Scope = scope;
            this.IsReverse = isReverse;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                return this.InputVaribale.Populate(property, null);
            }
            else if (this.InputVaribale.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVaribale.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.InputVaribale.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(this.Low));
            parameters.Add(SqlUtil.GetValueExpr(this.High));
            parameters.Add(this.Scope == GremlinKeyword.Scope.Local ? SqlUtil.GetValueExpr(1) : SqlUtil.GetValueExpr(-1));
            parameters.Add(this.IsReverse ? SqlUtil.GetValueExpr(1): SqlUtil.GetValueExpr(-1));

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Range, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}