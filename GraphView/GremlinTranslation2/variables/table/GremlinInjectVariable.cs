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
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "Inject_" + _count++;
        }

        List<object> rows;
        // When priorContext is null, the corresponding table reference only contains injected values. 
        GremlinToSqlContext priorContext;

        public GremlinInjectVariable(GremlinToSqlContext priorContext, params object[] values)
        {
            this.priorContext = priorContext;
            rows = new List<object>(values);
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        public List<WSelectElement> ToSelectElementList()
        {
            throw new NotImplementedException();
        }

        public GremlinScalarVariable DefaultProjection()
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

        public List<WSelectElement> ToPopulateProjection()
        {
            return null;
        }

        internal override void Populate(string name)
        {
            if (priorContext != null)
            {
                priorContext.Populate(name);
            }
        }

        internal override void Inject(GremlinToSqlContext currentContext, params string[] values)
        {
            rows.AddRange(values);
        }
    }
}
