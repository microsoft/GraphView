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

        public GremlinInjectVariable(List<object> values): base(GremlinVariableType.Table)
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
                    queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(row), GremlinKeyword.TableDefaultColumnName));
                    parameters.Add(SqlUtil.GetScalarSubquery(queryBlock));
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Inject, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }

        internal override void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            rows.AddRange(values);
        }
    }
}
