using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDecompose1Variable: GremlinTableVariable
    {
        public List<GremlinPathStepVariable> ComposeVariableList { get; set; }
        public string DefaultProjectionKey { get; set; }

        public GremlinDecompose1Variable(List<GremlinPathStepVariable> composeVariableList) : base(GremlinVariableType.Table)
        {
            ComposeVariableList = composeVariableList;
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
            parameters.Add(SqlUtil.GetColumnReferenceExpr("C", GremlinKeyword.TableDefaultColumnName));
            foreach (var projectProperty in ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Decompose1, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
