using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinTreeSideEffectVariable : GremlinTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinPathVariable PathVariable { get; set; }

        public GremlinTreeSideEffectVariable(string sideEffectKey, GremlinPathVariable pathVariable) : base(GremlinVariableType.Tree)
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
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.PathVariable.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.PathVariable.Populate(property, label);
            }
            return populateSuccessfully;
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
