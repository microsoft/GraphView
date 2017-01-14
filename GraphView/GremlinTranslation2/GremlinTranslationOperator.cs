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
                var inheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>(parentContext.InheritedTaggedVariables.Copy());
                foreach (var item in parentContext.TaggedVariables)
                {
                    if (inheritedTaggedVariables.ContainsKey(item.Key))
                    {
                        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                    }
                    else
                    {
                        inheritedTaggedVariables[item.Key] = new List<GremlinVariable>();
                        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                    }
                }
                rootAsContextOp.InheritedTaggedVariables = inheritedTaggedVariables;
                rootAsContextOp.InheritedPathList = new List<GremlinMatchPath>(parentContext.PathList);
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
        public Dictionary<string, List<GremlinVariable>> InheritedTaggedVariables { get; set; }
        public List<GremlinMatchPath> InheritedPathList { get; set; }

        public GremlinParentContextOp()
        {
            InheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>();   
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            newContext.TaggedVariables = InheritedTaggedVariables;
            newContext.PathList = InheritedPathList;
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
