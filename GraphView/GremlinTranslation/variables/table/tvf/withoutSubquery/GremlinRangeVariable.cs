using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRangeVariable : GremlinFilterTableVariable
    {
        public int Low { get; set; }
        public int High { get; set; }
        public bool IsReverse { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinRangeVariable(GremlinVariable inputVariable, int low, int high, GremlinKeyword.Scope scope, bool isReverse): base(inputVariable.GetVariableType())
        {
            this.InputVariable = inputVariable;
            this.Low = low;
            this.High = high;
            this.Scope = scope;
            this.IsReverse = isReverse;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.InputVariable.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.InputVariable.Populate(property, label);
            }
            return populateSuccessfully;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (Scope == GremlinKeyword.Scope.Local)
            {
                parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            }

            parameters.Add(SqlUtil.GetValueExpr(this.Low));
            parameters.Add(SqlUtil.GetValueExpr(this.High));
            parameters.Add(this.IsReverse ? SqlUtil.GetValueExpr(1): SqlUtil.GetValueExpr(-1));

            var tableRef = Scope == GremlinKeyword.Scope.Global
                ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.RangeGlobal, parameters, GetVariableName())
                : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.RangeLocal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}