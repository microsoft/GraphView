using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDecompose1Variable: GremlinTableVariable
    {
        public GremlinVariable ComposeVariable { get; set; }

        public GremlinDecompose1Variable(GremlinVariable composeVariable) : base(GremlinVariableType.Table)
        {
            ComposeVariable = composeVariable;
        }

        internal override void Populate(string property)
        {
            if (ComposeVariable is GremlinPathVariable)
                ComposeVariable.PopulateStepProperty(property);
            else
                ComposeVariable.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetColumnReferenceExpr(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName));
            foreach (var projectProperty in ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Decompose1, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
