using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinPropertyOp: GremlinTranslationOperator
    {
        private GremlinProperty property;

        public GremlinPropertyOp(GremlinProperty property)
        {
            this.property = property;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            GremlinVariableType pivotType = inputContext.PivotVariable.GetVariableType();
            if (   pivotType != GremlinVariableType.Vertex
                && pivotType != GremlinVariableType.Table
                && (this.property.Cardinality == GremlinKeyword.PropertyCardinality.List
                    || this.property.MetaProperties.Count > 0))
            {
                throw new QueryCompilationException("Only vertex can use PropertyCardinality.List and have meta properties");
            }
            inputContext.PivotVariable.Property(inputContext, property);

            return inputContext;
        }
    }
}
