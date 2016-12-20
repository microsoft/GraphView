using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using GraphView.GremlinTranslationOps;

namespace GraphView
{
    internal abstract class GremlinTranslationOperator
    {
        public List<string> Labels = new List<string>();
        public GremlinTranslationOperator InputOperator;
        public virtual GremlinToSqlContext GetContext()
        {
            return null;
        }
        public GremlinToSqlContext GetInputContext()
        {
            if (InputOperator != null) {
                GremlinToSqlContext context = InputOperator.GetContext();
                if (InputOperator.Labels != null)
                {
                    context.SetLabelsToCurrentVariable(InputOperator.Labels);
                }
                return context;
            } else {
                return new GremlinToSqlContext();
            }
        }
        public virtual WSqlScript ToSqlScript() {
            return GetContext().ToSqlScript();
        }

        public List<string> GetLabels()
        {
            return Labels;
        }

        public void ClearLabels()
        {
            Labels.Clear();
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedVariable { get; set; }
        public List<Projection> InheritedProjection;
        public bool IsInheritedEntireContext = false;
        public GremlinToSqlContext InheritedContext;
        public List<GremlinVariable> InheritedVariableList;
        public Dictionary<string, List<GremlinVariable>> InheritedAliasToGremlinVariableList;
        public List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> InheritedPathList;

        public void SetContext(GremlinToSqlContext context)
        {
            IsInheritedEntireContext = true;
            InheritedContext = context;
            InheritedProjection = new List<Projection>();
            InheritedVariableList = new List<GremlinVariable>();
            InheritedAliasToGremlinVariableList = new Dictionary<string, List<GremlinVariable>>();
            InheritedPathList = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
        }
        public override GremlinToSqlContext GetContext()
        {
            if (IsInheritedEntireContext) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();

            if (InheritedVariable != null)
            {
                newContext.RootVariable = InheritedVariable;
                newContext.InheritedVariableList = InheritedVariableList;
                newContext.AliasToGremlinVariableList = InheritedAliasToGremlinVariableList;
                newContext.SetCurrVariable(InheritedVariable);
                newContext.FromOuter = true;
                newContext.InheritedPathList = InheritedPathList;
            } 
            return newContext;
        }
    }
}
