using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDecompose1Variable: GremlinTableVariable
    {
        public List<GremlinVariable> ComposeVariableList { get; set; }
        public string DefaultProjectionKey { get; set; }

        public GremlinDecompose1Variable(List<GremlinVariable> composeVariableList) : base(GremlinVariableType.Table)
        {
            ComposeVariableList = composeVariableList;
        }

        public GremlinDecompose1Variable(GremlinVariable composeVariable) : base(GremlinVariableType.Table)
        {
            ComposeVariableList = new List<GremlinVariable> {composeVariable};
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return DefaultProjectionKey == null
                ? GetVariableProperty(GremlinKeyword.TableDefaultColumnName)
                : GetVariableProperty(DefaultProjectionKey);
        }

        internal override void Populate(string property)
        {
            foreach (var composeVariable in ComposeVariableList)
            {
                composeVariable.Populate(property);
            }
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
