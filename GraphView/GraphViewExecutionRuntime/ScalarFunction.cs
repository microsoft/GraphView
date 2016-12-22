using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// This is a data type enumeration defined for JSON documents.  
    /// These types are not a JSON standard, but a convention used Json.NET.  
    /// 
    /// Note: the precedence follows that of SQL Server.
    internal enum JsonDataType
    {
        Bytes = 0,
        String,
        Boolean,
        Int,
        Long,
        Float,
        Double,
        Date,
        Null
    }

    /// <summary>
    /// A scalar function takes input as a raw record and outputs a scalar value.
    /// </summary>
    internal abstract class ScalarFunction
    {
        public abstract string Evaluate(RawRecord record);
        public virtual JsonDataType DataType()
        {
            return JsonDataType.String;
        }
    }

    internal class ScalarSubqueryFunction : ScalarFunction
    {
        // When a subquery is compiled, the tuple from the outer context
        // is injected into the subquery through a constant-source scan, 
        // which is in a Cartesian product with the operators compiled from the query. 
        private GraphViewExecutionOperator subqueryOp;
        private ConstantSourceOperator constantSourceOp;

        public ScalarSubqueryFunction(GraphViewExecutionOperator subqueryOp, ConstantSourceOperator constantSourceOp)
        {
            this.subqueryOp = subqueryOp;
            this.constantSourceOp = constantSourceOp;
        }

        public override string Evaluate(RawRecord record)
        {
            constantSourceOp.ConstantSource = record;
            subqueryOp.Open();
            RawRecord firstResult = subqueryOp.Next();
            subqueryOp.Close();

            return firstResult == null ? null : firstResult.RetriveData(0);
        }
    }

    internal class ScalarValue : ScalarFunction
    {
        private string value;
        private JsonDataType dataType;

        public ScalarValue(string value, JsonDataType dataType)
        {
            this.value = value;
            this.dataType = dataType;
        }

        public override string Evaluate(RawRecord record)
        {
            return value;
        }

        public override JsonDataType DataType()
        {
            return dataType;
        }
    }

    internal class BinaryScalarFunction : ScalarFunction
    {
        ScalarFunction f1;
        ScalarFunction f2;
        BinaryExpressionType binaryType;

        public BinaryScalarFunction(ScalarFunction f1, ScalarFunction f2, BinaryExpressionType binaryType)
        {
            this.f1 = f1;
            this.f2 = f2;
            this.binaryType = binaryType;
        }

        public override JsonDataType DataType()
        {
            JsonDataType dataType1 = f1.DataType();
            JsonDataType dataType2 = f2.DataType();
            return dataType1 > dataType2 ? dataType1 : dataType2;
        }

        public override string Evaluate(RawRecord record)
        {
            JsonDataType targetType = DataType();
            string value1 = f1.Evaluate(record);
            string value2 = f2.Evaluate(record);

            try
            {
                switch(targetType)
                {
                    case JsonDataType.Boolean:
                        bool bool_value1 = value1 == "true" ? true : false;
                        bool bool_value2 = value2 == "true" ? true : false;
                        bool bool_result = false;
                        switch (binaryType)
                        {
                            case BinaryExpressionType.BitwiseAnd:
                                bool_result = bool_value1 ^ bool_value2;
                                break;
                            case BinaryExpressionType.BitwiseOr:
                                bool_result = bool_value1 | bool_value2;
                                break;
                            case BinaryExpressionType.BitwiseXor:
                                bool_result = bool_value1 ^ bool_value2;
                                break;
                            default:
                                break;
                        }
                        return bool_result ? "true" : "false";
                    case JsonDataType.Bytes:
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return value1 + value2.Substring(2);    // A binary string starts with 0x
                            default:
                                return value1;
                        }
                    case JsonDataType.Int:
                        int int_value1 = int.Parse(value1);
                        int int_value2 = int.Parse(value2);
                        switch(binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return (int_value1 + int_value2).ToString();
                            case BinaryExpressionType.BitwiseAnd:
                                return (int_value1 & int_value2).ToString();
                            case BinaryExpressionType.BitwiseOr:
                                return (int_value1 | int_value2).ToString();
                            case BinaryExpressionType.BitwiseXor:
                                return (int_value1 ^ int_value2).ToString();
                            case BinaryExpressionType.Divide:
                                return (int_value1 / int_value2).ToString();
                            case BinaryExpressionType.Modulo:
                                return (int_value1 % int_value2).ToString();
                            case BinaryExpressionType.Multiply:
                                return (int_value1 * int_value2).ToString();
                            case BinaryExpressionType.Subtract:
                                return (int_value1 - int_value2).ToString();
                            default:
                                return "";
                        }
                    case JsonDataType.Long:
                        long long_value1 = long.Parse(value1);
                        long long_value2 = long.Parse(value2);
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return (long_value1 + long_value2).ToString();
                            case BinaryExpressionType.BitwiseAnd:
                                return (long_value1 & long_value2).ToString();
                            case BinaryExpressionType.BitwiseOr:
                                return (long_value1 | long_value2).ToString();
                            case BinaryExpressionType.BitwiseXor:
                                return (long_value1 ^ long_value2).ToString();
                            case BinaryExpressionType.Divide:
                                return (long_value1 / long_value2).ToString();
                            case BinaryExpressionType.Modulo:
                                return (long_value1 % long_value2).ToString();
                            case BinaryExpressionType.Multiply:
                                return (long_value1 * long_value2).ToString();
                            case BinaryExpressionType.Subtract:
                                return (long_value1 - long_value2).ToString();
                            default:
                                return "";
                        }
                    case JsonDataType.Double:
                        double double_value1 = double.Parse(value1);
                        double double_value2 = double.Parse(value2);
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return (double_value1 + double_value2).ToString();
                            case BinaryExpressionType.Divide:
                                return (double_value1 / double_value2).ToString();
                            case BinaryExpressionType.Modulo:
                                return (double_value1 % double_value2).ToString();
                            case BinaryExpressionType.Multiply:
                                return (double_value1 * double_value2).ToString();
                            case BinaryExpressionType.Subtract:
                                return (double_value1 - double_value2).ToString();
                            default:
                                throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied values of type 'double'.");
                        }
                    default:
                        return null;
                }
            }
            catch (Exception e)
            {
                throw new QueryCompilationException("An exception occurred when evaluating the scalar expression.", e);
            }
            
        }
    }
}
