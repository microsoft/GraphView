﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionOp: GremlinTranslationOperator
    {
        public List<GraphTraversal2> UnionTraversals { get; set; }

        public GremlinUnionOp(params GraphTraversal2[] unionTraversals)
        {
            UnionTraversals = new List<GraphTraversal2>();
            foreach (var unionTraversal in unionTraversals)
            {
                UnionTraversals.Add(unionTraversal);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            
            inputContext.PivotVariable.Union(ref inputContext, UnionTraversals);

            return inputContext;
        }
    }
}