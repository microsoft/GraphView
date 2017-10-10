using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertyMapVariable : GremlinTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinPropertyMapVariable(GremlinVariable inputVariable, List<string> propertyKeys) : base(GremlinVariableType.Table)
        {
            this.InputVariable = inputVariable;
            this.PropertyKeys = propertyKeys;
        }

        internal override bool Populate(string property, string label = null)
        {
            return false;
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
            parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.PropertyKeys.Select(SqlUtil.GetValueExpr));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.PropertyMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
