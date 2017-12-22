using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace GraphView
{
    [Serializable]
    internal abstract class BooleanFunction
    {
        public abstract bool Evaluate(RawRecord r);

        public virtual HashSet<int> EvaluateInBatch(List<RawRecord> records)
        {
            HashSet<int> resultIndexes = new HashSet<int>();

            foreach (RawRecord record in records)
            {
                if (this.Evaluate(record))
                {
                    resultIndexes.Add(int.Parse(record.RetriveData(0).ToValue));
                }
            }

            return resultIndexes;
        }
    }

    [Serializable]
    internal class ComparisonFunction : BooleanFunction
    {
        ScalarFunction firstScalarFunction;
        ScalarFunction secondScalarFunction;
        BooleanComparisonType comparisonType;
        
        public ComparisonFunction(ScalarFunction f1, ScalarFunction f2, BooleanComparisonType comparisonType)
        {
            firstScalarFunction = f1;
            secondScalarFunction = f2;
            this.comparisonType = comparisonType;
        }

        public override bool Evaluate(RawRecord record)
        {
            FieldObject lhs = firstScalarFunction.Evaluate(record);
            FieldObject rhs = secondScalarFunction.Evaluate(record);

            if (lhs == null || rhs == null)
            {
                return false;
            }

            if (lhs is VertexPropertyField)
            {
                VertexPropertyField vp = (VertexPropertyField)lhs;
                foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                {
                    JsonDataType type1 = vsp.JsonDataType;
                    JsonDataType type2 = secondScalarFunction.DataType();

                    JsonDataType targetType = type1 > type2 ? type1 : type2;

                    if (Compare(vsp.ToValue, rhs.ToValue, targetType, this.comparisonType))
                    {
                        return true;
                    }
                }
                return false;
            }
            else
            {
                JsonDataType type1 = firstScalarFunction.DataType();
                JsonDataType type2 = secondScalarFunction.DataType();

                JsonDataType targetType = type1 > type2 ? type1 : type2;

                string value1 = firstScalarFunction.Evaluate(record)?.ToValue;
                string value2 = secondScalarFunction.Evaluate(record)?.ToValue;

                return Compare(value1, value2, targetType, this.comparisonType);
            }
        }

        public static bool Compare(string value1, string value2, JsonDataType targetType, BooleanComparisonType comparisonType)
        {
            switch (targetType)
            {
                case JsonDataType.Boolean:
                    bool bool_value1, bool_value2;
                    if (bool.TryParse(value1, out bool_value1) && bool.TryParse(value2, out bool_value2))
                    {
                        switch (comparisonType)
                        {
                            case BooleanComparisonType.Equals:
                                return bool_value1 == bool_value2;
                            case BooleanComparisonType.NotEqualToBrackets:
                            case BooleanComparisonType.NotEqualToExclamation:
                                return bool_value1 != bool_value2;
                            default:
                                throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"boolean\".");
                        }
                    }
                    else
                    {
                        return false;
                    }
                case JsonDataType.Bytes:
                    switch (comparisonType)
                    {
                        case BooleanComparisonType.Equals:
                            return value1 == value2;
                        case BooleanComparisonType.NotEqualToBrackets:
                        case BooleanComparisonType.NotEqualToExclamation:
                            return value1 != value2;
                        default:
                            return false;
                    }
                case JsonDataType.Int:
                    int int_value1, int_value2;
                    if (int.TryParse(value1, out int_value1) && int.TryParse(value2, out int_value2))
                    {
                        switch (comparisonType)
                        {
                            case BooleanComparisonType.Equals:
                                return int_value1 == int_value2;
                            case BooleanComparisonType.GreaterThan:
                                return int_value1 > int_value2;
                            case BooleanComparisonType.GreaterThanOrEqualTo:
                                return int_value1 >= int_value2;
                            case BooleanComparisonType.LessThan:
                                return int_value1 < int_value2;
                            case BooleanComparisonType.LessThanOrEqualTo:
                                return int_value1 <= int_value2;
                            case BooleanComparisonType.NotEqualToBrackets:
                            case BooleanComparisonType.NotEqualToExclamation:
                                return int_value1 != int_value2;
                            case BooleanComparisonType.NotGreaterThan:
                                return !(int_value1 > int_value2);
                            case BooleanComparisonType.NotLessThan:
                                return !(int_value1 < int_value2);
                            default:
                                throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"int\".");
                        }
                    }
                    else
                    {
                        return false;
                    }
                case JsonDataType.Long:
                    long long_value1, long_value2;
                    if (long.TryParse(value1, out long_value1) && long.TryParse(value2, out long_value2))
                    {
                        switch (comparisonType)
                        {
                            case BooleanComparisonType.Equals:
                                return long_value1 == long_value2;
                            case BooleanComparisonType.GreaterThan:
                                return long_value1 > long_value2;
                            case BooleanComparisonType.GreaterThanOrEqualTo:
                                return long_value1 >= long_value2;
                            case BooleanComparisonType.LessThan:
                                return long_value1 < long_value2;
                            case BooleanComparisonType.LessThanOrEqualTo:
                                return long_value1 <= long_value2;
                            case BooleanComparisonType.NotEqualToBrackets:
                            case BooleanComparisonType.NotEqualToExclamation:
                                return long_value1 != long_value2;
                            case BooleanComparisonType.NotGreaterThan:
                                return !(long_value1 > long_value2);
                            case BooleanComparisonType.NotLessThan:
                                return !(long_value1 < long_value2);
                            default:
                                throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"long\".");
                        }
                    }
                    else
                    {
                        return false;
                    }
                case JsonDataType.Double:
                    double double_value1, double_value2;
                    if (double.TryParse(value1, out double_value1) && double.TryParse(value2, out double_value2))
                    {
                        switch (comparisonType)
                        {
                            case BooleanComparisonType.Equals:
                                return double_value1 == double_value2;
                            case BooleanComparisonType.GreaterThan:
                                return double_value1 > double_value2;
                            case BooleanComparisonType.GreaterThanOrEqualTo:
                                return double_value1 >= double_value2;
                            case BooleanComparisonType.LessThan:
                                return double_value1 < double_value2;
                            case BooleanComparisonType.LessThanOrEqualTo:
                                return double_value1 <= double_value2;
                            case BooleanComparisonType.NotEqualToBrackets:
                            case BooleanComparisonType.NotEqualToExclamation:
                                return double_value1 != double_value2;
                            case BooleanComparisonType.NotGreaterThan:
                                return !(double_value1 > double_value2);
                            case BooleanComparisonType.NotLessThan:
                                return !(double_value1 < double_value2);
                            default:
                                throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"double\".");
                        }
                    }
                    else
                    {
                        return false;
                    }
                case JsonDataType.Float:
                    float float_value1, float_value2;
                    if (float.TryParse(value1, out float_value1) && float.TryParse(value2, out float_value2))
                    {
                        switch (comparisonType)
                        {
                            case BooleanComparisonType.Equals:
                                return float_value1 == float_value2;
                            case BooleanComparisonType.GreaterThan:
                                return float_value1 > float_value2;
                            case BooleanComparisonType.GreaterThanOrEqualTo:
                                return float_value1 >= float_value2;
                            case BooleanComparisonType.LessThan:
                                return float_value1 < float_value2;
                            case BooleanComparisonType.LessThanOrEqualTo:
                                return float_value1 <= float_value2;
                            case BooleanComparisonType.NotEqualToBrackets:
                            case BooleanComparisonType.NotEqualToExclamation:
                                return float_value1 != float_value2;
                            case BooleanComparisonType.NotGreaterThan:
                                return !(float_value1 > float_value2);
                            case BooleanComparisonType.NotLessThan:
                                return !(float_value1 < float_value2);
                            default:
                                throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"float\".");
                        }
                    }
                    else
                    {
                        return false;
                    }
                case JsonDataType.String:
                    switch (comparisonType)
                    {
                        case BooleanComparisonType.Equals:
                            return value1 == value2;
                        case BooleanComparisonType.GreaterThan:
                        case BooleanComparisonType.GreaterThanOrEqualTo:
                            return value1.CompareTo(value2) > 0;
                        case BooleanComparisonType.LessThan:
                        case BooleanComparisonType.LessThanOrEqualTo:
                            return value1.CompareTo(value2) > 0;
                        case BooleanComparisonType.NotEqualToBrackets:
                        case BooleanComparisonType.NotEqualToExclamation:
                            return value1 != value2;
                        case BooleanComparisonType.NotGreaterThan:
                            return value1.CompareTo(value2) <= 0;
                        case BooleanComparisonType.NotLessThan:
                            return value1.CompareTo(value2) >= 0;
                        default:
                            throw new QueryCompilationException("Operator " + comparisonType.ToString() + " cannot be applied to operands of type \"string\".");
                    }
                case JsonDataType.Date:
                    throw new NotImplementedException();
                case JsonDataType.Null:
                    return false;
                default:
                    throw new QueryCompilationException("Unsupported data type.");
            }
        }
    }

    [Serializable]
    internal class InFunction : BooleanFunction
    {
        private ScalarFunction lhsFunction;
        private List<ScalarFunction> values;
        private bool notDefined;

        public InFunction(ScalarFunction lhsFunction, List<ScalarFunction> values, bool notDefined)
        {
            this.lhsFunction = lhsFunction;
            this.values = values;
            this.notDefined = notDefined;
        }

        private static bool In(string lhsValue, JsonDataType lhsDataType, RawRecord record, List<ScalarFunction> values)
        {
            foreach (ScalarFunction valueFunction in values)
            {
                JsonDataType rhsDataType = valueFunction.DataType();
                JsonDataType targetType = lhsDataType > rhsDataType ? lhsDataType : rhsDataType;
                string rhsValue = valueFunction.Evaluate(record)?.ToValue;

                if (ComparisonFunction.Compare(lhsValue, rhsValue, targetType, BooleanComparisonType.Equals))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool NotIn(string lhsValue, JsonDataType lhsDataType, RawRecord record, List<ScalarFunction> values)
        {
            foreach (ScalarFunction valueFunction in values)
            {
                JsonDataType rhsDataType = valueFunction.DataType();
                JsonDataType targetType = lhsDataType > rhsDataType ? lhsDataType : rhsDataType;
                string rhsValue = valueFunction.Evaluate(record)?.ToValue;

                if (ComparisonFunction.Compare(lhsValue, rhsValue, targetType, BooleanComparisonType.Equals))
                {
                    return false;
                }
            }
            return true;
        }

        public override bool Evaluate(RawRecord record)
        {
            FieldObject lhs = this.lhsFunction.Evaluate(record);

            if (lhs == null)
            {
                return false;
            }

            if (lhs is VertexPropertyField)
            {
                VertexPropertyField vp = (VertexPropertyField) lhs;
                foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                {
                    JsonDataType lhsDataType = vsp.JsonDataType;
                    if (this.notDefined)
                    {
                        if (NotIn(vsp.ToValue, lhsDataType, record, this.values))
                        {
                            return true;
                        }
                    }
                    else
                    {
                        if (In(vsp.ToValue, lhsDataType, record, this.values))
                        {
                            return true;
                        }
                    }
                }

                return false;
            }
            else
            {
                string lhsValue = lhs.ToValue;
                JsonDataType lhsDataType = this.lhsFunction.DataType();
                return this.notDefined
                    ? NotIn(lhsValue, lhsDataType, record, this.values)
                    : In(lhsValue, lhsDataType, record, this.values);
            }
        }
    }

    internal enum BooleanBinaryFunctionType
    {
        And,
        Or,
    }

    [Serializable]
    internal class BooleanBinaryFunction : BooleanFunction
    {
        private BooleanBinaryFunctionType type;
        private BooleanFunction lhs;
        private BooleanFunction rhs;

        internal BooleanBinaryFunction(BooleanFunction plhs, BooleanFunction prhs, BooleanBinaryFunctionType ptype)
        {
            lhs = plhs;
            rhs = prhs;
            type = ptype;
        }

        public override bool Evaluate(RawRecord r)
        {
            if (type == BooleanBinaryFunctionType.And)
            {
                return lhs.Evaluate(r) && rhs.Evaluate(r);
            }
            if (type == BooleanBinaryFunctionType.Or)
            {
                return lhs.Evaluate(r) || rhs.Evaluate(r);
            }
            return false;
        }

        public override HashSet<int> EvaluateInBatch(List<RawRecord> records)
        {
            HashSet<int> lhsIndexes = this.lhs.EvaluateInBatch(records);

            if (this.type == BooleanBinaryFunctionType.And)
            {
                records = records.Where(i => lhsIndexes.Contains(int.Parse(i.RetriveData(0).ToValue))).ToList();
                return this.rhs.EvaluateInBatch(records);
            }

            if (this.type == BooleanBinaryFunctionType.Or)
            {
                records = records.Where(i => !lhsIndexes.Contains(int.Parse(i.RetriveData(0).ToValue))).ToList();
                HashSet<int> rhsIndexes = this.rhs.EvaluateInBatch(records);
                return new HashSet<int>(lhsIndexes.Union(rhsIndexes));
            }
            
            return new HashSet<int>();
        }
    }

    [Serializable]
    internal class ExistsFunction : BooleanFunction
    {
        // When a subquery is compiled, the tuple from the outer context
        // is injected into the subquery through a constant-source scan, 
        // which is in a Cartesian product with the operators compiled from the query. 
        private GraphViewExecutionOperator subqueryOp;
        [NonSerialized]
        private Container container;

        public ExistsFunction(GraphViewExecutionOperator subqueryOp, Container container)
        {
            this.subqueryOp = subqueryOp;
            this.container = container;
        }

        public override bool Evaluate(RawRecord r)
        {
            this.container.ResetTableCache(r);
            subqueryOp.ResetState();
            RawRecord firstResult = subqueryOp.Next();
            subqueryOp.Close();

            return firstResult != null;
        }

        public override HashSet<int> EvaluateInBatch(List<RawRecord> records)
        {
            this.container.ResetTableCache(records);
            this.subqueryOp.ResetState();

            HashSet<int> returnIndexes = new HashSet<int>();
            RawRecord rec = null;
            while (this.subqueryOp.State() && (rec = this.subqueryOp.Next()) != null)
            {
                returnIndexes.Add(int.Parse(rec.RetriveData(0).ToValue));
            }

            return returnIndexes;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.subqueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
        }
    }

    [Serializable]
    internal class BooleanNotFunction : BooleanFunction
    {
        private BooleanFunction booleanFunction;

        public BooleanNotFunction(BooleanFunction booleanFunction)
        {
            this.booleanFunction = booleanFunction;
        }

        public override bool Evaluate(RawRecord r)
        {
            return !this.booleanFunction.Evaluate(r);
        }

        public override HashSet<int> EvaluateInBatch(List<RawRecord> records)
        {
            HashSet<int> inputIndexes = new HashSet<int>(records.Select(r => int.Parse(r.RetriveData(0).ToValue)));
            HashSet<int> exceptIndexes = this.booleanFunction.EvaluateInBatch(records);

            return new HashSet<int>(inputIndexes.Except(exceptIndexes));
        }
    }
}
