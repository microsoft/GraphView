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
        // When priorContext is null, the corresponding table reference only contains injected values. 
        GremlinToSqlContext priorContext;

        public GremlinInjectVariable(GremlinToSqlContext priorContext, List<object> values)
        {
            this.priorContext = priorContext;
            rows = values;
        }

        public override WTableReference ToTableReference()
        {
            if (priorContext == null)
            {
                return SqlUtil.GetDerivedTable(GetInjectQueryBlock(), VariableName);

            }
            else
            {
                throw new NotImplementedException();;
            }
        }

        internal override  GremlinScalarVariable DefaultProjection()
        {
            // When priorContext is not null, the output table has one column,
            // and the column name is determined by priorContext.
            if (priorContext != null)
            {
                return priorContext.PivotVariable.DefaultProjection();
            }
            else
            {
                VariableName = GenerateTableAlias();
                return new GremlinVariableProperty(this, "_value");
            }
        }

        internal override void Populate(string property)
        {
            if (priorContext != null)
            {
                priorContext.Populate(property);
            }
        }

        internal override void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            rows.AddRange(values);
        }

        private WSelectQueryBlock GetInjectQueryBlock()
        {
            var selectBlock = new WSelectQueryBlock();
            foreach (var row in rows)
            {
                var valueExpr = SqlUtil.GetValueExpr(row);
                selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));
            }
            return selectBlock;
        }
    }
}
