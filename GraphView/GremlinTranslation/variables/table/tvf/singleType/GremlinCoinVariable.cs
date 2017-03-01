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

        public GremlinCoinVariable(double probability)
        {
            Probability = probability;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(Probability));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coin, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
