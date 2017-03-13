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
        public List<Tuple<object, IComparer>> ByModulatingMap;
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, List<Tuple<object, IComparer>> byModulatingMap, GremlinKeyword.Scope scope)
            :base(GremlinVariableType.Table)
        {
            ByModulatingMap = byModulatingMap;
            Scope = scope;
            InputVariable = inputVariable;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var by in ByModulatingMap)
            {
                if (by.Item1 is GremlinToSqlContext)
                {
                    variableList.AddRange(((GremlinToSqlContext)by.Item1).FetchVarsFromCurrAndChildContext());
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

            foreach (var pair in ByModulatingMap)
            {
                WScalarExpression scalrExpr;
                if (pair.Item1 is GremlinToSqlContext)
                {
                    scalrExpr = SqlUtil.GetScalarSubquery(((GremlinToSqlContext)pair.Item1).ToSelectQueryBlock());
                }
                else if (pair.Item1 is GremlinKeyword.Column)
                {
                    GremlinKeyword.Column column = (GremlinKeyword.Column) pair.Item1;
                    scalrExpr = column == GremlinKeyword.Column.keys
                        ? SqlUtil.GetValueExpr("keys")
                        : SqlUtil.GetValueExpr("values");
                }
                else if (pair.Item1 == "")
                {
                    scalrExpr = SqlUtil.GetValueExpr(null);
                }
                else
                {
                    throw new ArgumentException();
                }
                var orderTableReference = tableRef as WOrderTableReference;
                orderTableReference?.OrderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalrExpr, pair.Item2));
                orderTableReference?.Parameters.Add(scalrExpr);
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
