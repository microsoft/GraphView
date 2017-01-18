using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupVariable: GremlinScalarTableVariable
    {
        public GremlinToSqlContext ParentContext { get; set; }
        public List<object> Parameters { get; set; }

        public GremlinGroupVariable(GremlinToSqlContext parentContext, string groupByKey = null)
        {
            ParentContext = parentContext;
            Parameters = new List<object>() { groupByKey };
        }

        internal override void Populate(string property)
        {
            ParentContext.Populate(property);
        }

        internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        {
            byTraversal.GetStartOp().InheritedVariableFromParent(ParentContext);
            GremlinToSqlContext byContext = byTraversal.GetEndOp().GetContext();
            Parameters.Add(byContext);
        }

        internal override void By(GremlinToSqlContext currentContext, string name)
        {
            Parameters.Add(name);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (var parameter in Parameters)
            {
                if (parameter is GremlinToSqlContext)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery((parameter as GremlinToSqlContext).ToSelectQueryBlock()));
                }
                else
                {
                    parameters.Add(SqlUtil.GetValueExpr(parameter));
                }
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
