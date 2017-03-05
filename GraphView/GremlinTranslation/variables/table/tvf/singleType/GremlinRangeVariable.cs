using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRangeVariable : GremlinTableVariable
    {
        public int Low { get; set; }
        public int High { get; set; }
        public bool IsReverse { get; set; }
        public bool IsLocal { get; set; }

        public GremlinRangeVariable(int low, int high, GremlinKeyword.Scope scope, bool isReverse): base(GremlinVariableType.Table)
        {
            Low = low;
            High = high;
            IsLocal = scope != GremlinKeyword.Scope.global;
            IsReverse = isReverse;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(Low));
            parameters.Add(SqlUtil.GetValueExpr(High));
            parameters.Add(IsLocal ? SqlUtil.GetValueExpr(1) : SqlUtil.GetValueExpr(-1));
            parameters.Add(IsReverse ? SqlUtil.GetValueExpr(-1): SqlUtil.GetValueExpr(1));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Range, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}