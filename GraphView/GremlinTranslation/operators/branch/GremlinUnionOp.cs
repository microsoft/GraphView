using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionOp: GremlinTranslationOperator
    {
        public List<GraphTraversal> UnionTraversals { get; set; }

        public GremlinUnionOp(params GraphTraversal[] unionTraversals)
        {
            UnionTraversals = new List<GraphTraversal>();
            foreach (var unionTraversal in unionTraversals)
            {
                UnionTraversals.Add(unionTraversal);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of union()-step can't be null.");
            }

            List<GremlinToSqlContext> unionContexts = new List<GremlinToSqlContext>();
            foreach (var traversal in UnionTraversals)
            {
                traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                unionContexts.Add(traversal.GetEndOp().GetContext());
            }
            
            inputContext.PivotVariable.Union(ref inputContext, unionContexts);

            return inputContext;
        }
    }
}
