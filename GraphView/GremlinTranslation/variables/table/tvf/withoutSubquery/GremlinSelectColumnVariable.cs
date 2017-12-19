using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectColumnVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public GremlinKeyword.Column Column { get; set; }

        public GremlinSelectColumnVariable(GremlinVariable inputVariable, GremlinKeyword.Column column) : base(GremlinVariableType.Unknown)
        {
            GremlinVariableType inputVariableType = inputVariable.GetVariableType();
            if (!(GremlinVariableType.NULL <= inputVariableType && inputVariableType <= GremlinVariableType.Map ||
                  inputVariableType == GremlinVariableType.Path || inputVariableType == GremlinVariableType.Tree))
            {
                throw new SyntaxErrorException("The inputVariable of select() can not be " + inputVariableType);
            }

            this.InputVariable = inputVariable;
            this.Column = column;
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
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
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
            parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(this.Column == GremlinKeyword.Column.Keys ? "Keys" : "Values"));
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SelectColumn, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
