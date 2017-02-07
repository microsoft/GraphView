using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public string SideEffect { get; set; }
        public List<object> ByParameters { get; set; }

        public GremlinGroupOp()
        {
            ByParameters = new List<object>();
        }

        public GremlinGroupOp(string sideEffect)
        {
            SideEffect = sideEffect;
            ByParameters = new List<object>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<object> byParameters = new List<object>();
            foreach (var parameter in ByParameters)
            {
                if (parameter is GraphTraversal2)
                {
                    (parameter as GraphTraversal2).GetStartOp().InheritedVariableFromParent(inputContext);
                    byParameters.Add((parameter as GraphTraversal2).GetEndOp().GetContext());
                }
                else
                {
                    byParameters.Add(parameter);
                }
            }

            inputContext.PivotVariable.Group(inputContext, SideEffect, byParameters);

            return inputContext;
        }

        public void ModulateBy()
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GraphTraversal2 paramOp)
        {
            ByParameters.Add(paramOp);
        }

        public void ModulateBy(string key)
        {
            ByParameters.Add(key);
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
            throw new NotImplementedException();
        }
    }
}
