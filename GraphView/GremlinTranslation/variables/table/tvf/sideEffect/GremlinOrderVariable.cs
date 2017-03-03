using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOrderVariable: GremlinTableVariable
    {
        public List<object> ByList { get; set; }
        public List<IComparer> OrderList { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, List<object> byList, List<IComparer> orderList, GremlinKeyword.Scope scope)
        {
            ByList = byList;
            OrderList = orderList;
            Scope = scope;
            InputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            var secondTableRef = Scope == GremlinKeyword.Scope.global ?
                SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderGlobal, parameters, this, GetVariableName())
              : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderLocal, parameters, this, GetVariableName());

            (secondTableRef as WOrderTableReference).OrderParameters = new List<Tuple<WScalarExpression, IComparer>>();
            for (var i = 0; i < OrderList.Count; i++)
            {
                if (ByList[i] is string)
                {
                    var scalarExpr = InputVariable.GetVariableProperty(ByList[i] as string).ToScalarExpression();
                    (secondTableRef as WOrderTableReference).OrderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalarExpr, OrderList[i]));
                    (secondTableRef as WOrderTableReference).Parameters.Add(scalarExpr);
                }
                else if (ByList[i] is GremlinToSqlContext)
                {
                    var scalarQuery = SqlUtil.GetScalarSubquery((ByList[i] as GremlinToSqlContext).ToSelectQueryBlock());
                    (secondTableRef as WOrderTableReference).OrderParameters.Add(
                        new Tuple<WScalarExpression, IComparer>(scalarQuery, OrderList[i]));
                    (secondTableRef as WOrderTableReference).Parameters.Add(scalarQuery);
                }
                else
                {
                    throw new QueryCompilationException();    
                }
            }

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
