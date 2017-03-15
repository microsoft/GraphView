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
        public List<Tuple<GremlinToSqlContext, IComparer>> ByModulatingList;
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingList, GremlinKeyword.Scope scope)
            :base(GremlinVariableType.Table)
        {
            ByModulatingList = byModulatingList;
            Scope = scope;
            InputVariable = inputVariable;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var by in ByModulatingList)
            {
                if (by.Item1 != null)
                {
                    variableList.AddRange(by.Item1.FetchVarsFromCurrAndChildContext());
                }
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            var tableRef = Scope == GremlinKeyword.Scope.global
              ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderGlobal, parameters, GetVariableName())
              : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderLocal, parameters, GetVariableName());

            var wOrderTableReference = tableRef as WOrderTableReference;
            if (wOrderTableReference != null)
                wOrderTableReference.OrderParameters = new List<Tuple<WScalarExpression, IComparer>>();

            if (Scope == GremlinKeyword.Scope.local)
            {
                ((WOrderLocalTableReference)tableRef).Parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            }

            foreach (var pair in ByModulatingList)
            {
                WScalarExpression scalrExpr;
                if (pair.Item1 == null)
                {
                    scalrExpr = SqlUtil.GetValueExpr(null);
                }
                else
                {
                    scalrExpr = SqlUtil.GetScalarSubquery(pair.Item1.ToSelectQueryBlock());
                }
                var orderTableReference = tableRef as WOrderTableReference;
                orderTableReference?.OrderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalrExpr, pair.Item2));
                orderTableReference?.Parameters.Add(scalrExpr);
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
