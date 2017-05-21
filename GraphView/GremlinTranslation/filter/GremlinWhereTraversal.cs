using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinWhereTraversalOp : GremlinTranslationOperator
    {
        public GraphTraversal2 WhereTraversal { get; set; }

        public GremlinWhereTraversalOp(GraphTraversal2 whereTraversal)
        {
            WhereTraversal = whereTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            //where(whereTraversal)
            ConfigureStartAndEndSteps(WhereTraversal);

            WhereTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext whereContext = WhereTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.Where(inputContext, whereContext);

            return inputContext;
        }

        internal void ConfigureStartAndEndSteps(GraphTraversal2 whereTraversal)
        {
            if (whereTraversal.GremlinTranslationOpList.Count >= 2)
            {
                //__.as()
                GremlinAsOp asOp = whereTraversal.GremlinTranslationOpList[1] as GremlinAsOp;
                if (asOp != null)
                {
                    whereTraversal.GremlinTranslationOpList.RemoveAt(1); //remove as-step
                    whereTraversal.InsertGremlinOperator(1, new GremlinSelectOp(GremlinKeyword.Pop.Last, asOp.Labels.First()));
                }

                //__.Or()
                GremlinOrOp orOp = whereTraversal.GremlinTranslationOpList[1] as GremlinOrOp;
                if (orOp != null)
                {
                    if (orOp.IsInfix)
                    {
                        ConfigureStartAndEndSteps(orOp.FirstTraversal);
                        ConfigureStartAndEndSteps(orOp.SecondTraversal);
                    }
                    else
                    {
                        foreach (var traversal in orOp.OrTraversals)
                        {
                            ConfigureStartAndEndSteps(traversal);
                        }
                    }
                }

                //__.And()
                GremlinAndOp andOp = whereTraversal.GremlinTranslationOpList[1] as GremlinAndOp;
                if (andOp != null)
                {
                    if (andOp.IsInfix)
                    {
                        ConfigureStartAndEndSteps(andOp.FirstTraversal);
                        ConfigureStartAndEndSteps(andOp.SecondTraversal);
                    }
                    else
                    {
                        foreach (var traversal in andOp.AndTraversals)
                        {
                            ConfigureStartAndEndSteps(traversal);
                        }
                    }
                }

                //__.Not()
                GremlinNotOp notOp = whereTraversal.GremlinTranslationOpList[1] as GremlinNotOp;
                if (notOp != null)
                {
                    ConfigureStartAndEndSteps(notOp.NotTraversal);
                }
            }

            var lastOp = WhereTraversal.LastGremlinTranslationOp as GremlinAsOp;
            if (lastOp != null)
            {
                string label = lastOp.Labels.First();
                whereTraversal.GremlinTranslationOpList.Remove(whereTraversal.LastGremlinTranslationOp); //remove the last as-step
                whereTraversal.LastGremlinTranslationOp = WhereTraversal.GremlinTranslationOpList.Last();
                whereTraversal.AddGremlinOperator(new GremlinWherePredicateOp(Predicate.eq(label)));
            }
        }
    }
}
