using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using GraphView.GremlinTranslationOps;

namespace GraphView
{
    internal abstract class GremlinTranslationOperator
    {
        public GremlinTranslationOperator InputOperator;
        public virtual GremlinToSqlContext GetContext()
        {
            return null;
        }
        public GremlinToSqlContext GetInputContext()
        {
            if (InputOperator != null) {
                return InputOperator.GetContext();
            } else {
                return new GremlinToSqlContext();
            }
        }
        public virtual WSqlFragment ToWSqlFragment() {
            return GetContext().ToSqlQuery();
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedVariable { get; set; }
        public bool IsInheritedEntireContext = false;
        public GremlinToSqlContext InheritedContext;

        public void SetContext(GremlinToSqlContext context)
        {
            IsInheritedEntireContext = true;
            InheritedContext = context;
        }
        public override GremlinToSqlContext GetContext()
        {
            if (IsInheritedEntireContext) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            newContext.RootVariable = InheritedVariable;
            newContext.SetCurrVariable(InheritedVariable);
            newContext.FromOuter = true;

            return newContext;
        }
    }
}
