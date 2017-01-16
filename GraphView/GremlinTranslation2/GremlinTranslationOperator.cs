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

                //var inheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>();
                //foreach (var item in parentContext.InheritedTaggedVariables)
                //{
                //    if (inheritedTaggedVariables.ContainsKey(item.Key))
                //    {
                //        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                //    }
                //    else
                //    {
                //        inheritedTaggedVariables[item.Key] = new List<GremlinVariable>();
                //        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                //    }
                //}
                //foreach (var item in parentContext.TaggedVariables)
                //{
                //    if (inheritedTaggedVariables.ContainsKey(item.Key))
                //    {
                //        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                //    }
                //    else
                //    {
                //        inheritedTaggedVariables[item.Key] = new List<GremlinVariable>();
                //        inheritedTaggedVariables[item.Key].AddRange(item.Value);
                //    }
                //}

                //var inheritedVariableList = new List<GremlinVariable>();
                //foreach (var variable in parentContext.InheritedVariableList)
                //{
                //    inheritedVariableList.Add(variable);
                //}
                //foreach (var variable in parentContext.VariableList)
                //{
                //    inheritedVariableList.Add(variable);
                //}

                //rootAsContextOp.InheritedVariableList = inheritedVariableList;
                //rootAsContextOp.InheritedTaggedVariables = inheritedTaggedVariables;
                rootAsContextOp.InheritedPathList = new List<GremlinMatchPath>(parentContext.PathList);
                rootAsContextOp.ParentContext = parentContext;
                //rootAsContextOp.ParentVariable = parentContext.PivotVariable;
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
        //public Dictionary<string, List<GremlinVariable>> InheritedTaggedVariables { get; set; }
        public List<GremlinMatchPath> InheritedPathList { get; set; }
        //public List<GremlinVariable> InheritedVariableList { get; set; }
        public GremlinToSqlContext ParentContext { get; set; }
        //public GremlinVariable ParentVariable { get; set; }

        public GremlinParentContextOp()
        {
            //InheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>();
            //InheritedVariableList = new List<GremlinVariable>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            //newContext.InheritedTaggedVariables = InheritedTaggedVariables;
            //newContext.InheritedVariableList = InheritedVariableList;
            newContext.PathList = InheritedPathList;
            newContext.ParentContext = ParentContext;
            //newContext.ParentVariable = ParentVariable;
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
