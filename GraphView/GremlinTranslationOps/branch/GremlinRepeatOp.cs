using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        public GraphTraversal2 RepeatTraversal { get; set; }
        public Predicate ConditionPredicate { get; set; }
        public GraphTraversal2 ConditionTraversal { get; set; }
        public bool IsEmitTrue { get; set; }
        public bool IsEmitBefore { get; set; }
        public bool IsEmitAfter { get; set; }
        public bool IsUntilBefore { get; set; }
        public bool IsUntilAfter { get; set; }
        public bool IsTimes { get; set; }
        public long Times { get; set; }

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
        }

        public GremlinRepeatOp()
        {
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //GremlinUtil.InheritedVariableFromParent(RepeatTraversal, inputContext);

            //WRepeatPath repeatPath = new WRepeatPath() {};
            //repeatPath.IsUntilBefore = IsUntilBefore;
            //repeatPath.IsUntilAfter = IsUntilAfter;
            //repeatPath.IsEmitTrue = IsEmitTrue;
            //repeatPath.IsEmitBefore = IsEmitBefore;
            //repeatPath.IsEmitAfter = IsEmitAfter;
            //repeatPath.IsTimes = IsTimes;
            //repeatPath.Times = Times;

            //inputContext.SaveCurrentState();
            //Dictionary<string, List<GremlinVariable>> RepeatOuterAliasList = new Dictionary<string, List<GremlinVariable>>();
            //foreach (var alias in inputContext.AliasToGremlinVariableList)
            //{
            //    RepeatOuterAliasList[alias.Key] = alias.Value.Copy();
            //}
            //GremlinToSqlContext context = RepeatTraversal.GetEndOp().GetContext();
            //repeatPath.SubQueryExpr = context.ToSelectQueryBlock();
            //foreach (var alias in context.AliasToGremlinVariableList)
            //{
            //    if (RepeatOuterAliasList.ContainsKey(alias.Key) &&
            //        RepeatOuterAliasList[alias.Key].Count != alias.Value.Count)
            //    {
            //        GremlinVariable outerLastVar = RepeatOuterAliasList[alias.Key].Last();
            //        GremlinVariable innerLastVar = alias.Value.Last();
            //        repeatPath.SubQueryExpr.SelectElements.Add(new ColumnProjection(innerLastVar.VariableName, "id", outerLastVar.VariableName + ".id").ToSelectElement());
            //    }
            //}

            //if (ConditionTraversal != null)
            //{
            //    //inherited from repeat traversal
            //    GremlinUtil.InheritedVariableFromParent(ConditionTraversal, context);
            //    var conditionContext = ConditionTraversal.GetEndOp().GetContext();
            //    repeatPath.ConditionSubQueryBlock = conditionContext.ToSqlBoolean();
            //}
            //if (ConditionPredicate != null)
            //{
            //    throw new NotImplementedException();
            //}

            //inputContext.ResetSavedState();

            //if (inputContext.CurrVariable is GremlinVertexVariable)
            //{
            //    GremlinPathNodeVariable newEdgeVar = new GremlinPathNodeVariable();
            //    inputContext.AddNewVariable(newEdgeVar);

            //    GremlinVertexVariable sinkVar = new GremlinVertexVariable();
            //    inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, sinkVar);
            //    inputContext.AddNewVariable(sinkVar);
            //    inputContext.SetDefaultProjection(sinkVar);
            //    inputContext.SetCurrVariable(sinkVar);

            //    inputContext.WithPaths[newEdgeVar.VariableName] = repeatPath;
            //}
            //else if (inputContext.CurrVariable is GremlinEdgeVariable)
            //{
            //    GremlinPathEdgeVariable newEdgeVar = new GremlinPathEdgeVariable(inputContext.CurrVariable as GremlinEdgeVariable);

            //    (repeatPath.SubQueryExpr.SelectElements[0] as WSelectScalarExpression).ColumnName =
            //        inputContext.GetSourceNode(inputContext.CurrVariable).VariableName + ".id";
            //    (repeatPath.SubQueryExpr.SelectElements[1] as WSelectScalarExpression).ColumnName =
            //        inputContext.CurrVariable.VariableName + ".id";
            //    (repeatPath.SubQueryExpr.SelectElements[2] as WSelectScalarExpression).ColumnName = inputContext.CurrVariable.VariableName;

            //    var oldPath =
            //        inputContext.NewPathList.Find(p => p.EdgeVariable.VariableName == inputContext.CurrVariable.VariableName);
            //    oldPath.EdgeVariable = newEdgeVar;

            //    inputContext.WithPaths[newEdgeVar.VariableName] = repeatPath;
            //    inputContext.AddNewVariable(newEdgeVar);
            //    inputContext.SetDefaultProjection(newEdgeVar);
            //    inputContext.SetCurrVariable(newEdgeVar);
            //}
            //else
            //{
            //    throw new NotImplementedException();
            //}

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                throw new NotImplementedException();
            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                GremlinDerivedVariable derivedVariable = new GremlinDerivedVariable(inputContext.ToSelectQueryBlock());

                GremlinUtil.InheritedVariableFromParent(RepeatTraversal, inputContext);
                Dictionary<string, List<GremlinVariable>> RepeatOuterAliasList =
                    new Dictionary<string, List<GremlinVariable>>();
                foreach (var alias in inputContext.AliasToGremlinVariableList)
                {
                    RepeatOuterAliasList[alias.Key] = alias.Value.Copy();
                }
                GremlinToSqlContext context = RepeatTraversal.GetEndOp().GetContext();
                var subQueryExpr = context.ToSelectQueryBlock();
                foreach (var alias in context.AliasToGremlinVariableList)
                {
                    if (RepeatOuterAliasList.ContainsKey(alias.Key) &&
                        RepeatOuterAliasList[alias.Key].Count != alias.Value.Count)
                    {
                        GremlinVariable outerLastVar = RepeatOuterAliasList[alias.Key].Last();
                        GremlinVariable innerLastVar = alias.Value.Last();
                        subQueryExpr.SelectElements.Add(
                            new ColumnProjection(innerLastVar.VariableName, "*", outerLastVar.VariableName)
                                .ToSelectElement());
                    }
                }
                (subQueryExpr.SelectElements[0] as WSelectScalarExpression).ColumnName =
                    inputContext.CurrVariable.VariableName + ".id";
                (subQueryExpr.SelectElements[1] as WSelectScalarExpression).ColumnName =
                    inputContext.GetSinkNode(inputContext.CurrVariable).VariableName + ".id";

                inputContext.ClearAndCreateNewContextInfo();
                inputContext.AddNewVariable(derivedVariable);

                List<object> PropertyKeys = new List<object>();
                PropertyKeys.Add(new WScalarSubquery() {SubQueryExpr = subQueryExpr});
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("repeat", PropertyKeys);
                GremlinTVFVariable tvfVariable = inputContext.CrossApplyToVariable(derivedVariable, secondTableRef,
                    Labels);
                inputContext.SetCurrVariable(tvfVariable);
                inputContext.SetDefaultProjection(tvfVariable);
            }
            else
            {
                throw new NotImplementedException();
            }

            return inputContext;
        }

    }
}
