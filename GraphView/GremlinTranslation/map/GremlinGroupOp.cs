using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupOp: GremlinTranslationOperator
    {
        public string SideEffect { get; set; }
        public object GroupBy { get; set; }
        public object ProjectBy { get; set; }

        public GremlinGroupOp()
        {
        }

        public GremlinGroupOp(string sideEffect)
        {
            SideEffect = sideEffect;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            List<object> byParameters = new List<object>();

            if (GroupBy == null || GroupBy as string == "")
            {
                GroupBy = inputContext.PivotVariable.DefaultProjection();
            }
            else
            {
                GroupBy = inputContext.PivotVariable.GetVariableProperty(GroupBy as string);
            }

            if (ProjectBy == null || ProjectBy as string == "")
            {
                ProjectBy = inputContext.PivotVariable.DefaultProjection();
            }
            else
            {
                ProjectBy = inputContext.PivotVariable.GetVariableProperty(ProjectBy as string);
            }

            if (GroupBy is GraphTraversal2)
            {
                (GroupBy as GraphTraversal2).GetStartOp().InheritedVariableFromParent(inputContext);
                byParameters.Add((GroupBy as GraphTraversal2).GetEndOp().GetContext());
            }
            else
            {
                byParameters.Add(GroupBy);
            }

            if (ProjectBy is GraphTraversal2)
            {
                (ProjectBy as GraphTraversal2).GetStartOp().InheritedVariableFromParent(inputContext);
                byParameters.Add((ProjectBy as GraphTraversal2).GetEndOp().GetContext());
            }
            else
            {
                byParameters.Add(ProjectBy);
            }

            inputContext.PivotVariable.Group(inputContext, SideEffect, byParameters);

            return inputContext;
        }

        public override void ModulateBy()
        {
            if (GroupBy == null)
            {
                GroupBy = "";
            }
            else if (ProjectBy == null)
            {
                ProjectBy = "";
            }
            else
            {
                throw new QueryCompilationException("The key and value traversals for group()-step have already been set");
            }
        }

        public override void ModulateBy(GraphTraversal2 paramOp)
        {
            if (GroupBy == null)
            {
                GroupBy = paramOp;
            }
            else if (ProjectBy == null)
            {
                ProjectBy = paramOp;
            }
            else
            {
                throw new QueryCompilationException("The key and value traversals for group()-step have already been set");
            }
        }

        public override void ModulateBy(string key)
        {
            if (GroupBy == null)
            {
                GroupBy = key;
            }
            else if (ProjectBy == null)
            {
                ProjectBy = key;
            }
            else
            {
                throw new QueryCompilationException("The key and value traversals for group()-step have already been set");
            }
        }
    }
}
