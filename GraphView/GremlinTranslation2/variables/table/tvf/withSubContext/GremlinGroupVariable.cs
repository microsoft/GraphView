using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupVariable: GremlinScalarTableVariable
    {
        public List<object> Parameters { get; set; }
        public string SideEffectKey { get; set; }

        public GremlinGroupVariable(string sideEffectKey, List<object> parameters)
        {
            SideEffectKey = sideEffectKey;
            Parameters = new List<object>(parameters);
        }

        internal override void Populate(string property)
        {
            //ParentContext.Populate(property);
        }

        //internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        //{
        //    byTraversal.GetStartOp().InheritedVariableFromParent(ParentContext);
        //    GremlinToSqlContext byContext = byTraversal.GetEndOp().GetContext();
        //    Parameters.Add(byContext);
        //}

        //internal override void By(GremlinToSqlContext currentContext, string name)
        //{
        //    Parameters.Add(name);
        //}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
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
