using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal abstract class Projection
    {
        public string VariableAlias { get; set; }

        public virtual WSelectElement ToSelectElement()
        {
            return null;
        }
    }

    internal class ColumnProjection : Projection
    {
        public string Key { get; set; }
        public string ColumnAlias { get; set; }
        public ColumnProjection(string variableAlias, string key, string columnAlias = null)
        {
            VariableAlias = variableAlias;
            ColumnAlias = columnAlias;
            Key = key;
        }

        public override WSelectElement ToSelectElement()
        {
            var multiPartIdentifier = (Key == "" || Key == null)
                ? GremlinUtil.GetMultiPartIdentifier(VariableAlias)
                : GremlinUtil.GetMultiPartIdentifier(VariableAlias, Key);
            return new WSelectScalarExpression()
            {
                ColumnName = ColumnAlias,
                SelectExpr = new WColumnReferenceExpression()
                { MultiPartIdentifier = multiPartIdentifier }
            };
        }
    }

    internal class FunctionCallProjection : Projection
    {
        public WFunctionCall FunctionCall { get; set; }

        public FunctionCallProjection(WFunctionCall functionCall)
        {
            FunctionCall = functionCall;
        }
        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression() { SelectExpr = FunctionCall };
        }
    }

    internal class StarProjection : Projection
    {
        public StarProjection() { }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectStarExpression();
        }
    }
}
