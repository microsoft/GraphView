using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal abstract class GremlinTranslationOperator
    {
        public GremlinTranslationOperator InputOperator { get; set; }

        internal virtual GremlinToSqlContext GetContext()
        {
            return null;
        }

        internal virtual WSqlScript ToSqlScript() {
            return GetContext().ToSqlScript();
        }

        internal virtual void InheritedVariableFromParent(GremlinToSqlContext parentContext)
        {
            if (this is GremlinParentContextOp)
            {
                GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
                rootAsContextOp.InheritedPivotVariable = parentContext.PivotVariable;
                rootAsContextOp.InheritedTaggedVariables = parentContext.TaggedVariables;
            }
        }

        internal virtual void InheritedContextFromParent(GremlinToSqlContext parentContext)
        {
            if (this is GremlinParentContextOp)
            {
                GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
                rootAsContextOp.InheritedContext = parentContext.Duplicate();
            }
        }

        internal GremlinToSqlContext GetInputContext()
        {
            return InputOperator != null ? InputOperator.GetContext() : new GremlinToSqlContext();
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedPivotVariable { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        public Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>> InheritedTaggedVariables { get;
            set; }

        public GremlinParentContextOp()
        {
            InheritedTaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>();   
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            newContext.TaggedVariables = InheritedTaggedVariables;
            if (InheritedPivotVariable != null)
            {
                GremlinContextVariable newVariable = GremlinContextVariable.Create(InheritedPivotVariable);
                newContext.VariableList.Add(newVariable);
                newContext.PivotVariable = newVariable;
            } 
            return newContext;
        }
    }
}
