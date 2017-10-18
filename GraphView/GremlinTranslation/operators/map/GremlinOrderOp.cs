using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal class GremlinOrderOp: GremlinTranslationOperator
    {
        public List<Tuple<GraphTraversal, IComparer>> ByModulatingList { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinOrderOp(GremlinKeyword.Scope scope)
        {
            ByModulatingList = new List<Tuple<GraphTraversal, IComparer>>();
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of order()-step can't be null.");
            }

            if (ByModulatingList.Count == 0)
            {
                 ByModulatingList.Add(new Tuple<GraphTraversal, IComparer>(GraphTraversal.__(), new IncrOrder()));
            }

            var newByModulatingList = new List<Tuple<GremlinToSqlContext, IComparer>>();
            foreach (var pair in ByModulatingList)
            {
                GraphTraversal traversal = pair.Item1;
                GremlinToSqlContext context = null;

                if (Scope == GremlinKeyword.Scope.Global)
                {
                    traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    context = traversal.GetEndOp().GetContext();
                }

                if (Scope == GremlinKeyword.Scope.Local)
                {
                    //g.V().groupCount().order(Local).by(Keys) or g.V().groupCount().order(Local).by(__.select(Keys))
                    if (traversal.GremlinTranslationOpList.Count >= 2 && traversal.GremlinTranslationOpList[1] is GremlinSelectColumnOp)
                    {
                        //FROM selectColumn(C._value, "Keys"/"Values")
                        GremlinToSqlContext newContext = new GremlinToSqlContext();
                        GremlinOrderLocalInitVariable initVar = new GremlinOrderLocalInitVariable(inputContext.PivotVariable);
                        newContext.VariableList.Add(initVar);
                        newContext.SetPivotVariable(initVar);

                        traversal.GetStartOp().InheritedContextFromParent(newContext);
                        context = traversal.GetEndOp().GetContext();
                    }
                    else
                    {
                        //FROM decompose1(C._value)
                        GremlinToSqlContext newContext = new GremlinToSqlContext();
                        GremlinDecompose1Variable decompose1 = new GremlinDecompose1Variable(inputContext.PivotVariable);
                        newContext.VariableList.Add(decompose1);
                        newContext.TableReferencesInFromClause.Add(decompose1);
                        newContext.SetPivotVariable(decompose1);

                        traversal.GetStartOp().InheritedContextFromParent(newContext);
                        context = traversal.GetEndOp().GetContext();
                    }
                }

                newByModulatingList.Add(new Tuple<GremlinToSqlContext, IComparer>(context, pair.Item2));
            }

            inputContext.PivotVariable.Order(inputContext, newByModulatingList, Scope);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal traversal)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal, IComparer>(traversal, new IncrOrder()));
        }

        public override void ModulateBy(GraphTraversal traversal, IComparer comparer)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal, IComparer>(traversal, comparer));
        }
    }
}
