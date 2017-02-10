using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// Inject variable will be translated to a derived table reference
    /// in the SQL FROM clause, concatenating results from priorContext and injected values. 
    /// </summary>
    internal class GremlinInjectVariable : GremlinTableVariable
    {
        List<object> rows;

        public GremlinInjectVariable(List<object> values)
        {
            rows = values;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var row in rows)
            {
                if (row is string || row is int)
                {
                    var queryBlock = new WSelectQueryBlock();
                    queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(row), "_value"));
                    parameters.Add(SqlUtil.GetScalarSubquery(queryBlock));
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Inject, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            rows.AddRange(values);
        }
    }
}
