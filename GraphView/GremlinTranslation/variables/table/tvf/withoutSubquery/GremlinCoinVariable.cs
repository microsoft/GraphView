using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoinVariable : GremlinFilterTableVariable
    {
        public double Probability { get; set; }

        public GremlinCoinVariable(GremlinVariable inputVariable, double probability) : base(inputVariable.GetVariableType())
        {
            this.Probability = probability;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.Probability));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coin, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
