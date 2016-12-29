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
    internal class GremlinInjectVariable : GremlinTableVariable, ISqlTable
    {
        List<string> rows;
        // When priorContext is null, the corresponding table reference only contains injected values. 
        GremlinToSqlContext2 priorContext;

        public GremlinInjectVariable(GremlinToSqlContext2 priorContext, params string[] values)
        {
            this.priorContext = priorContext;
            rows = new List<string>(values);
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override GremlinScalarVariable DefaultProjection()
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

        internal override void Populate(string name)
        {
            if (priorContext != null)
            {
                priorContext.Populate(name);
            }
        }

        internal override void Inject(GremlinToSqlContext2 currentContext, params string[] values)
        {
            rows.AddRange(values);
        }
    }
}
