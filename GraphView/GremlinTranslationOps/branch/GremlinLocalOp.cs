using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinLocalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 LocalTraversal;
        public List<object> PropertyKeys;

        public GremlinLocalOp(GraphTraversal2 localTraversal)
        {
            LocalTraversal = localTraversal;
            PropertyKeys = new List<object>();
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.SaveCurrentState();
            GremlinUtil.InheritedVariableFromParent(LocalTraversal, inputContext);
            GremlinToSqlContext context = LocalTraversal.GetEndOp().GetContext();
            WScalarSubquery ScalarSubquery = new WScalarSubquery()
            {
                SubQueryExpr = context.ToSelectQueryBlock()
            };
            inputContext.ResetSavedState();

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                PropertyKeys.Add("node");
                PropertyKeys.Add(ScalarSubquery);
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("properties", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
                newVariable.Type = context.CurrVariable.Type;
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);

            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                PropertyKeys.Add("edge");
                PropertyKeys.Add(ScalarSubquery);
                var oldVariable = inputContext.GetSinkNode(inputContext.CurrVariable);
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("properties", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(oldVariable, secondTableRef, Labels);
                newVariable.Type = context.CurrVariable.Type;
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);
            }
            else
            {
                throw new NotImplementedException();
            }

            return inputContext;
        }
    }
}
