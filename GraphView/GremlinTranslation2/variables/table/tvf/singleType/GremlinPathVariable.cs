using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathVariable: GremlinScalarTableVariable
    {
        private List<GremlinVariableProperty> pathList;

        public GremlinPathVariable(List<GremlinVariableProperty> pathList)
        {
            this.pathList = pathList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var path in pathList)
            {
                parameters.Add(SqlUtil.GetColumnReferenceExpr(path.GremlinVariable.GetVariableName(), path.VariableProperty));    
            }

            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Path, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
