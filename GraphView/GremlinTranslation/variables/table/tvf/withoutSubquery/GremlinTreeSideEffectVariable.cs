using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinTreeSideEffectVariable : GremlinScalarTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinPathVariable PathVariable { get; set; }

        public GremlinTreeSideEffectVariable(string sideEffectKey, GremlinPathVariable pathVariable)
        {
            this.SideEffectKey = sideEffectKey;
            this.PathVariable = pathVariable;
            this.Labels.Add(sideEffectKey);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() {this};
            variableList.AddRange(this.PathVariable.FetchAllVars());
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                return PathVariable.Populate(property, null);
            }
            else if (this.PathVariable.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.SideEffectKey));
            parameters.Add(this.PathVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Tree, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
