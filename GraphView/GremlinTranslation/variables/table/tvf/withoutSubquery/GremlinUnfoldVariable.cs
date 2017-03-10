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

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable, GremlinVariableType variableType)
            : base(variableType)
        {
            UnfoldVariable = unfoldVariable;
        }

        internal override bool ContainsLabel(string label)
        {
            return false;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return UnfoldVariable.GetUnfoldVariableType();
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            ProjectedProperties.Add(property);

            UnfoldVariable.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (UnfoldVariable is GremlinListVariable)
            {
                parameters.Add((UnfoldVariable as GremlinListVariable).ToScalarExpression());
            }
            else
            {
                parameters.Add(UnfoldVariable.DefaultVariableProperty().ToScalarExpression());
            }
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
