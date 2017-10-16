using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUnfoldVariable : GremlinTableVariable
    {
        public GremlinVariable UnfoldVariable { get; set; }

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable) : base(
            unfoldVariable.GetVariableType() == GremlinVariableType.Map
                ? GremlinVariableType.MapEntry
                : GremlinVariableType.Unknown) 
        {
            this.UnfoldVariable = unfoldVariable;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                this.UnfoldVariable.Populate(property, null);
                return true;
            }
            else if (this.UnfoldVariable.Populate(property, label))
            {
                base.Populate(property, null);
                return true;
            }
            else
            {
                return false;
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.UnfoldVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
        
            parameters.Add(this.UnfoldVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
