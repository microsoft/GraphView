using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public partial class WScalarExpression
    {
        internal virtual ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            throw new NotImplementedException();
        }
    }

    public partial class WScalarSubquery
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator subQueryOp = SubQueryExpr.Compile(subContext, dbConnection);
            return new ScalarSubqueryFunction(subQueryOp, subContext.OuterContextOp);
        }
    }

    public partial class WBinaryExpression
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            ScalarFunction f1 = FirstExpr.CompileToFunction(context, dbConnection);
            ScalarFunction f2 = SecondExpr.CompileToFunction(context, dbConnection);

            return new BinaryScalarFunction(f1, f2, ExpressionType);
        }
    }

    public partial class WColumnReferenceExpression
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            int fieldIndex = context.LocateColumnReference(this);
            return new FieldValue(fieldIndex);
        }
    }

    public partial class WValueExpression
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            DateTime date_value;
            double double_value;
            float float_value;
            long long_value;
            int int_value;
            bool bool_value;

            if (SingleQuoted)
            {
                if (DateTime.TryParse(Value, out date_value))
                {
                    return new ScalarValue(Value, JsonDataType.Date);
                }
                else
                {
                    return new ScalarValue(Value, JsonDataType.String);
                }
            }
            else
            {
                if (Value.Equals("null", StringComparison.CurrentCultureIgnoreCase))
                {
                    return new ScalarValue(Value, JsonDataType.Null);
                }
                else if (bool.TryParse(Value, out bool_value))
                {
                    return new ScalarValue(Value, JsonDataType.Boolean);
                }
                else if (Value.IndexOf('.') >= 0)
                {
                    if (float.TryParse(Value, out float_value))
                    {
                        return new ScalarValue(Value, JsonDataType.Float);
                    }
                    else if (double.TryParse(Value, out double_value))
                    {
                        return new ScalarValue(Value, JsonDataType.Double);
                    }
                }
                else if (Value.StartsWith("0x"))
                {
                    return new ScalarValue(Value, JsonDataType.Bytes);
                }
                else if (int.TryParse(Value, out int_value))
                {
                    return new ScalarValue(Value, JsonDataType.Int);
                }
                else if (long.TryParse(Value, out long_value))
                {
                    return new ScalarValue(Value, JsonDataType.Long);
                }

                throw new QueryCompilationException(string.Format("Failed to interpret string \"{0}\" into any data type.", Value));
            }
        }
    }

    public partial class WFunctionCall
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            string funcName = FunctionName.ToString();

            switch (funcName)
            {
                case "WithInArray":
                    var checkField = Parameters[0] as WColumnReferenceExpression;
                    var arrayField = Parameters[1] as WColumnReferenceExpression;
                    return new WithInArray(context.LocateColumnReference(checkField), context.LocateColumnReference(arrayField));
                default:
                    throw new NotImplementedException("Function " + funcName + " hasn't been implemented.");
            }
            throw new NotImplementedException("Function " + funcName + " hasn't been implemented.");
        }
    }
}
