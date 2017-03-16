using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinConstantVariable: GremlinScalarTableVariable
    {
        public object ConstantValue { get; set; }

        public GremlinConstantVariable(object value)
        {
            ConstantValue = value;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            bool isList = false;
            if (GremlinUtil.IsList(ConstantValue) || GremlinUtil.IsArray(ConstantValue))
            {
                isList = true;  //1 It's a list
                foreach (var value in (IEnumerable) ConstantValue)
                {
                    parameters.Add(SqlUtil.GetValueExpr(value));
                }
            }
            else if (GremlinUtil.IsNumber(ConstantValue) || ConstantValue is string || ConstantValue is bool)
            {
                parameters.Add(SqlUtil.GetValueExpr(ConstantValue));
            }
            else
            {
                throw new ArgumentException();
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Constant, parameters, GetVariableName());
            ((WConstantReference) tableRef).IsList = isList;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
