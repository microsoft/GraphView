using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoinVariable : GremlinTableVariable
    {
        public double Probability { get; set; }

        public GremlinCoinVariable(double probability) : base(GremlinVariableType.Table)
        {
            Probability = probability;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(Probability));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coin, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
