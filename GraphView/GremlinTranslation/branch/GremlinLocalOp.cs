using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinLocalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 LocalTraversal { get; set; }
        public List<object> PropertyKeys { get; set; }

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
            foreach (var statement in context.Statements)
            {
                inputContext.Statements.Add(statement);
            }
            WScalarSubquery ScalarSubquery = new WScalarSubquery()
            {
                SubQueryExpr = context.ToSelectQueryBlock()
            };
            inputContext.ResetSavedState();

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                PropertyKeys.Add("node");
                PropertyKeys.Add(ScalarSubquery);
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("local", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);

                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);

            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                PropertyKeys.Add("edge");
                PropertyKeys.Add(ScalarSubquery);
                //inputContext.IsUsedInTVF[inputContext.CurrVariable.VariableName] = true;

                var oldVariable = inputContext.GetSinkNode(inputContext.CurrVariable);
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("local", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(oldVariable, secondTableRef, Labels);
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
