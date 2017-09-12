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
        public GremlinContextVariable InputVariable { get; set; }
        public GremlinOrderVariable(GremlinVariable inputVariable, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingList, GremlinKeyword.Scope scope)
            :base(GremlinVariableType.Table)
        {
            ByModulatingList = byModulatingList;
            Scope = scope;
            InputVariable = new GremlinContextVariable(inputVariable);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(InputVariable);
            foreach (var by in ByModulatingList)
            {
                variableList.AddRange(by.Item1.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var by in ByModulatingList)
            {
                variableList.AddRange(by.Item1.FetchAllTableVars());
            }
            return variableList;
        }

        internal override void Populate(string property)
        {
            InputVariable.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            var tableRef = Scope == GremlinKeyword.Scope.Global
              ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderGlobal, parameters, GetVariableName())
              : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OrderLocal, parameters, GetVariableName());

            var wOrderTableReference = tableRef as WOrderTableReference;
            if (wOrderTableReference != null)
                wOrderTableReference.OrderParameters = new List<Tuple<WScalarExpression, IComparer>>();

            if (Scope == GremlinKeyword.Scope.Local)
            {
                ((WOrderLocalTableReference)tableRef).Parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            }

            foreach (var pair in ByModulatingList)
            {
                WScalarExpression scalarExpr = SqlUtil.GetScalarSubquery(pair.Item1.ToSelectQueryBlock());

                var orderTableReference = tableRef as WOrderTableReference;
                orderTableReference?.OrderParameters.Add(new Tuple<WScalarExpression, IComparer>(scalarExpr, pair.Item2));
                orderTableReference?.Parameters.Add(scalarExpr);
            }

            if (Scope == GremlinKeyword.Scope.Local)
            {
                foreach (var property in ProjectedProperties)
                {
                    wOrderTableReference.Parameters.Add(SqlUtil.GetValueExpr(property));
                }
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
