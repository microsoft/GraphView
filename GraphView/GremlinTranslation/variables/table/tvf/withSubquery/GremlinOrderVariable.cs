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
        public Dictionary<GremlinToSqlContext, IComparer> ByModulatingMap;
        public GremlinKeyword.Scope Scope { get; set; }
        public GremlinVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, Dictionary<GremlinToSqlContext, IComparer> byModulatingMap, GremlinKeyword.Scope scope)
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
                variableList.AddRange(by.Key.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            var tableRef = Scope == GremlinKeyword.Scope.global ?
                SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderGlobal, parameters, GetVariableName())
              : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderLocal, parameters, GetVariableName());

            var wOrderTableReference = tableRef as WOrderTableReference;
            if (wOrderTableReference != null)
                wOrderTableReference.OrderParameters = new List<Tuple<WScalarExpression, IComparer>>();

            foreach (var pair in ByModulatingMap)
            {
                var scalarQuery = SqlUtil.GetScalarSubquery(pair.Key.ToSelectQueryBlock());
                var orderTableReference = tableRef as WOrderTableReference;
                orderTableReference?.OrderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalarQuery, pair.Value));
                orderTableReference?.Parameters.Add(scalarQuery);
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
