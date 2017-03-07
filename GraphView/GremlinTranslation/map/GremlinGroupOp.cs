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
            if (GroupBy == null)
            {
                byParameters.Add(inputContext.PivotVariable.DefaultProjection());
            }
            else if (GroupBy is string)
            {
                byParameters.Add((string)GroupBy == "" ? inputContext.PivotVariable.DefaultProjection()
                                                       : inputContext.PivotVariable.GetVariableProperty((string)GroupBy));
            }
            else if (GroupBy is GraphTraversal2)
            {
                ((GraphTraversal2)GroupBy).GetStartOp().InheritedVariableFromParent(inputContext);
                byParameters.Add(((GraphTraversal2)GroupBy).GetEndOp().GetContext());
            }

            if (ProjectBy == null)
            {
                byParameters.Add(inputContext.PivotVariable.DefaultProjection());
            }
            else if (ProjectBy is string)
            {
                byParameters.Add((string)ProjectBy == "" ? inputContext.PivotVariable.DefaultProjection()
                                                         : inputContext.PivotVariable.GetVariableProperty((string)ProjectBy));
            }
            else if (ProjectBy is GraphTraversal2)
            {
                ((GraphTraversal2)ProjectBy).GetStartOp().InheritedVariableFromParent(inputContext);
                byParameters.Add(((GraphTraversal2)ProjectBy).GetEndOp().GetContext());
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
