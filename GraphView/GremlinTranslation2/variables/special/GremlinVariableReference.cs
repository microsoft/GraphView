using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableReference: GremlinTableVariable, ISqlStatement
    {
        public GremlinToSqlContext Context;

        public override WTableReference ToTableReference()
        {
            return new WVariableTableReference()
            {
                Variable = GremlinUtil.GetVariableReference(VariableName),
                Alias = GremlinUtil.GetIdentifier(VariableName)
            };
        }

        public GremlinVariableReference() { }

        public GremlinVariableReference(GremlinToSqlContext context)
        {
            Context = context;
            VariableName = GenerateTableAlias();
        }

        public virtual List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = Context.GetSetVariableStatements();
            statementList.Add(new WSetVariableStatement()
            {
                Expression = new WScalarSubquery()
                {
                    SubQueryExpr = Context.ToSelectQueryBlock()
                },
                Variable = GremlinUtil.GetVariableReference(VariableName)
            });
            return statementList;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            Context.PivotVariable.Property(currentContext, properties);
        }
    }
}
