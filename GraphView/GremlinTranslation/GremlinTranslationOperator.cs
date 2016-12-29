using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslation
{
    internal abstract class GremlinTranslationOperator
    {
        public List<string> Labels { get; set; }
        public GremlinTranslationOperator InputOperator { get; set; }

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
        public List<Projection> InheritedProjection { get; set; }
        public bool IsInheritedEntireContext { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        public List<GremlinVariable> InheritedVariableList { get; set; }
        public Dictionary<string, List<GremlinVariable>> InheritedAliasToGremlinVariableList { get; set; }
        public List<GremlinMatchPath> InheritedPathList { get; set; }

        public void SetContext(GremlinToSqlContext context)
        {
            IsInheritedEntireContext = true;
            InheritedContext = context;
            InheritedProjection = new List<Projection>();
            InheritedVariableList = new List<GremlinVariable>();
            InheritedAliasToGremlinVariableList = new Dictionary<string, List<GremlinVariable>>();
            InheritedPathList = new List<GremlinMatchPath>();
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
                //newContext.IsUsedInTVF = InheritedIsUsedInTVF;
            } 
            return newContext;
        }
    }
}
