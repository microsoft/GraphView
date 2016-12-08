using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinFoldOp: GremlinTranslationOperator
    {
        public GremlinFoldOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //Hack for union variable
            if (inputContext.CurrVariable is GremlinDerivedVariable
                &&
                (inputContext.CurrVariable as GremlinDerivedVariable).Type == GremlinDerivedVariable.DerivedType.UNION)
            {
                WSetVariableStatement statement = inputContext.GetOrCreateSetVariableStatement();
                inputContext.ClearAndCreateNewContextInfo();
                GremlinVariableReference newCurrVar = new GremlinVariableReference(statement);
                inputContext.AddNewVariable(newCurrVar, Labels);
                inputContext.SetCurrVariable(newCurrVar);
                inputContext.SetDefaultProjection(newCurrVar);
            }

            WScalarExpression parameter = GremlinUtil.GetStarColumnReferenceExpression(); //TODO
            inputContext.ProcessProjectWithFunctionCall(Labels, "fold", parameter);

            return inputContext;
        }
    }
}
