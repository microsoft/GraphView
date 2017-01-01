using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAndOp : GremlinTranslationOperator
    {
        public IList<GremlinTranslationOperator> ConjunctiveOperators { get; set; }

        public GremlinAndOp(params GraphTraversal2[] andTraversals)
        {
            ConjunctiveOperators = new List<GremlinTranslationOperator>();
            foreach (var traversal in andTraversals)
            {
                ConjunctiveOperators.Add(traversal.LastGremlinTranslationOp);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }
}
