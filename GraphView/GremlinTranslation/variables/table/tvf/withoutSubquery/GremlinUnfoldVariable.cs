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

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable)
            : base(GremlinVariableType.Table)
        {
            UnfoldVariable = unfoldVariable;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);
            UnfoldVariable.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(UnfoldVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
        
            parameters.Add(UnfoldVariable.DefaultProjection().ToScalarExpression());
            if (ProjectedProperties.Count == 0)
            {
                parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.TableDefaultColumnName));
            }
            foreach (var projectProperty in ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
