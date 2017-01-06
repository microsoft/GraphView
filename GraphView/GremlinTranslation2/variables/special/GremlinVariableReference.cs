using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableReference: GremlinTableVariable, ISqlStatement
    {
        public GremlinToSqlContext Context { get; set; }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetVariableTableReference(VariableName);
        }

        public GremlinVariableReference()
        {
            VariableName = GenerateTableAlias();
        }

        public GremlinVariableReference(GremlinToSqlContext context)
        {
            Context = context;
            VariableName = GenerateTableAlias();
        }

        public virtual List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = Context.GetSetVariableStatements();
            statementList.Add(SqlUtil.GetSetVariableStatement(VariableName, SqlUtil.GetScalarSubquery(Context.ToSelectQueryBlock())));
            return statementList;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            Context.PivotVariable.Property(currentContext, properties);
        }
    }
}
