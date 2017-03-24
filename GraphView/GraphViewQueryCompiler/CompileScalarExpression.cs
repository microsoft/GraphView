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
            if (ColumnType == ColumnType.Wildcard)
                return null;
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
            string funcName = FunctionName.ToString().ToLowerInvariant();

            switch (funcName)
            {
                case "withinarray":
                    WColumnReferenceExpression checkField = Parameters[0] as WColumnReferenceExpression;
                    WColumnReferenceExpression arrayField = Parameters[1] as WColumnReferenceExpression;
                    return new WithInArray(context.LocateColumnReference(checkField), context.LocateColumnReference(arrayField));
                case "withoutarray":
                    checkField = Parameters[0] as WColumnReferenceExpression;
                    arrayField = Parameters[1] as WColumnReferenceExpression;
                    return new WithOutArray(context.LocateColumnReference(checkField), context.LocateColumnReference(arrayField));
                case "hasproperty":
                    checkField = Parameters[0] as WColumnReferenceExpression;
                    WValueExpression propertyName = Parameters[1] as WValueExpression;
                    return new HasProperty(context.LocateColumnReference(checkField), propertyName.Value);
                case "compose1":
                    List<Tuple<string, int>> targetFieldsAndTheirNames = new List<Tuple<string, int>>();
                    WValueExpression defaultProjectionKey = Parameters[0] as WValueExpression;
                    if (defaultProjectionKey == null)
                        throw new SyntaxErrorException("The first parameter of Compose1 has to be a WValueExpression.");

                    for (int i = 1; i < Parameters.Count; i += 2)
                    {
                        WColumnReferenceExpression columnRef = Parameters[i] as WColumnReferenceExpression;
                        WValueExpression name = Parameters[i+1] as WValueExpression;

                        if (name == null)
                            throw new SyntaxErrorException("The parameter of Compose1 at an odd position has to be a WValueExpression.");
                        if (columnRef == null)
                            throw new SyntaxErrorException("The parameter of Compose1 at an even position has to be a WColumnReference.");

                        targetFieldsAndTheirNames.Add(new Tuple<string, int>(name.Value, context.LocateColumnReference(columnRef)));
                    }

                    return new Compose1(targetFieldsAndTheirNames, defaultProjectionKey.Value);
                case "compose2":
                    List<ScalarFunction> inputOfCompose2 = new List<ScalarFunction>();

                    foreach (var parameter in Parameters)
                    {
                        inputOfCompose2.Add(parameter.CompileToFunction(context, dbConnection));
                    }

                    return new Compose2(inputOfCompose2);
                case "path":
                    List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList;
                    List<ScalarFunction> byFuncList;
                    WPathTableReference.GetPathStepListAndByFuncList(context, dbConnection, this.Parameters,
                        out pathStepList, out byFuncList);
                    return new Path(pathStepList);
                default:
                    throw new NotImplementedException("Function " + funcName + " hasn't been implemented.");
            }
            throw new NotImplementedException("Function " + funcName + " hasn't been implemented.");
        }
    }
}
