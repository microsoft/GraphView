using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinBothVOp: GremlinTranslationOperator
    {
        public GremlinBothVOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            var currEdge = inputContext.CurrVariable;

            //TODO
            //GremlinUtil.CheckIsGremlinEdgeVariable(currEdge);

            //var existInPath = inputContext.Paths.Find(p => p.Item2 == currEdge);
            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("bothV", existInPath.Item1.VariableName, existInPath.Item3.VariableName);

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "bothV");

            //inputContext.AddNewVariable(newVariable);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);
            
            return inputContext;
        }
    }
}
