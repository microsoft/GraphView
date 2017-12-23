using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    /// <summary>
    /// This is a data type enumeration defined for JSON documents.  
    /// These types are not a JSON standard, but a convention used Json.NET.  
    /// 
    /// Note: the precedence follows that of SQL Server.
    public enum JsonDataType
    {
        Bytes = 0,
        String,
        Boolean,
        Int,
        Long,
        Float,
        Double,
        Date,
        Null,
        Object,
        Array,
        Unknown
    }

    internal class JsonDataTypeHelper
    {
        internal static JsonDataType GetJsonDataType(JTokenType jTokenType)
        {
            switch (jTokenType)
            {
                case JTokenType.Bytes:
                    return JsonDataType.Bytes;
                case JTokenType.String:
                    return JsonDataType.String;
                case JTokenType.Boolean:
                    return JsonDataType.Boolean;
                case JTokenType.Integer:
                    return JsonDataType.Int;
                case JTokenType.Float:
                    return JsonDataType.Float;
                case JTokenType.Date:
                    return JsonDataType.Date;
                case JTokenType.Null:
                    return JsonDataType.Null;
                case JTokenType.Object:
                    return JsonDataType.Object;
                case JTokenType.Array:
                    return JsonDataType.Array;
                default:
                    return JsonDataType.String;
            }
        }

        internal static JValue GetStringFieldData(string value, JsonDataType type)
        {
            switch (type)
            {
                case JsonDataType.Bytes:
                    return new JValue(byte.Parse(value));
                case JsonDataType.String:
                    return new JValue(value);
                case JsonDataType.Boolean:
                    return new JValue(bool.Parse(value));
                case JsonDataType.Int:
                    return new JValue(int.Parse(value));
                case JsonDataType.Float:
                    return new JValue(float.Parse(value));
                case JsonDataType.Date:
                    throw new NotImplementedException();
                case JsonDataType.Null:
                    return null;
                case JsonDataType.Object:
                    throw new NotImplementedException();
                case JsonDataType.Array:
                    throw new NotImplementedException();
                default:
                    throw new QueryCompilationException("Unsupported data type.");
            }
        }
    }

    /// <summary>
    /// A scalar function takes input as a raw record and outputs a scalar value.
    /// </summary>
    [Serializable]
    internal abstract class ScalarFunction
    {
        public abstract FieldObject Evaluate(RawRecord record);
        public virtual JsonDataType DataType()
        {
            return JsonDataType.String;
        }
    }

    [Serializable]
    internal class ScalarSubqueryFunction : ScalarFunction
    {
        // When a subquery is compiled, the tuple from the outer context
        // is injected into the subquery through a constant-source scan, 
        // which is in a Cartesian product with the operators compiled from the query. 
        private GraphViewExecutionOperator subqueryOp;
        [NonSerialized]
        private Container container;

        public ScalarSubqueryFunction(GraphViewExecutionOperator subqueryOp, Container container)
        {
            this.subqueryOp = subqueryOp;
            this.container = container;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            this.container.ResetTableCache(record);
            subqueryOp.ResetState();
            RawRecord firstResult = subqueryOp.Next();
            subqueryOp.Close();

            return firstResult == null ? null : firstResult.RetriveData(0);
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            EnumeratorOperator enumeratorOp = subqueryOp.GetFirstOperator() as EnumeratorOperator;
            this.container = new Container();
            enumeratorOp.SetContainer(this.container);
        }
    }

    [Serializable]
    internal class ScalarValue : ScalarFunction
    {
        private string value;
        private JsonDataType dataType;

        public ScalarValue(string value, JsonDataType dataType)
        {
            this.value = value;
            this.dataType = dataType;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            if (this.dataType == JsonDataType.Null) return null;
            return new StringField(value, dataType);
        }

        public override JsonDataType DataType()
        {
            return dataType;
        }
    }

    [Serializable]
    internal class FieldValue : ScalarFunction
    {
        private int fieldIndex;
        private JsonDataType dataType;

        public FieldValue(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
            dataType = JsonDataType.String;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            FieldObject fo = record[fieldIndex];

            StringField sf = fo as StringField;
            if (sf != null)
            {
                dataType = sf.JsonDataType;
                return fo;
            }

            PropertyField pf = fo as PropertyField;
            if (pf != null)
            {
                dataType = pf.JsonDataType;
                return fo;
            }

            return fo;
        }

        public override JsonDataType DataType()
        {
            return dataType;
        }
    }

    [Serializable]
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

        public override FieldObject Evaluate(RawRecord record)
        {
            string value1 = f1.Evaluate(record)?.ToValue;
            string value2 = f2.Evaluate(record)?.ToValue;
            JsonDataType targetType = DataType();

            switch (targetType)
            {
                case JsonDataType.Boolean:
                    bool bool_value1, bool_value2;
                    if (bool.TryParse(value1, out bool_value1) && bool.TryParse(value2, out bool_value2))
                    {
                        switch (binaryType)
                        {
                            case BinaryExpressionType.BitwiseAnd:
                                return new StringField((bool_value1 ^ bool_value2).ToString(), JsonDataType.Boolean);
                            case BinaryExpressionType.BitwiseOr:
                                return new StringField((bool_value1 | bool_value2).ToString(), JsonDataType.Boolean);
                            case BinaryExpressionType.BitwiseXor:
                                return new StringField((bool_value1 ^ bool_value2).ToString(), JsonDataType.Boolean);
                            default:
                                throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied to operands of type \"boolean\".");
                        }
                    } 
                    else
                    {
                        throw new QueryCompilationException(string.Format("Cannot cast \"{0}\" or \"{1}\" to values of type \"boolean\"",
                            value1, value2));
                    }
                case JsonDataType.Bytes:
                    switch (binaryType)
                    {
                        case BinaryExpressionType.Add:
                            return new StringField(value1 + value2.Substring(2), JsonDataType.Bytes);
                        default:
                            throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied to operands of type \"bytes\".");
                    }
                case JsonDataType.Int:
                    int int_value1, int_value2;
                    if (int.TryParse(value1, out int_value1) && int.TryParse(value2, out int_value2))
                    {
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return new StringField((int_value1 + int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.BitwiseAnd:
                                return new StringField((int_value1 & int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.BitwiseOr:
                                return new StringField((int_value1 | int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.BitwiseXor:
                                return new StringField((int_value1 ^ int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.Divide:
                                return new StringField((int_value1 / int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.Modulo:
                                return new StringField((int_value1 % int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.Multiply:
                                return new StringField((int_value1 * int_value2).ToString(), JsonDataType.Int);
                            case BinaryExpressionType.Subtract:
                                return new StringField((int_value1 - int_value2).ToString(), JsonDataType.Int);
                            default:
                                return new StringField("");
                        }
                    }
                    else
                    {
                        throw new QueryCompilationException(string.Format("Cannot cast \"{0}\" or \"{1}\" to values of type \"int\"",
                            value1, value2));
                    }
                case JsonDataType.Long:
                    long long_value1, long_value2;
                    if (long.TryParse(value1, out long_value1) && long.TryParse(value2, out long_value2))
                    {
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return new StringField((long_value1 + long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.BitwiseAnd:
                                return new StringField((long_value1 & long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.BitwiseOr:
                                return new StringField((long_value1 | long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.BitwiseXor:
                                return new StringField((long_value1 ^ long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.Divide:
                                return new StringField((long_value1 / long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.Modulo:
                                return new StringField((long_value1 % long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.Multiply:
                                return new StringField((long_value1 * long_value2).ToString(), JsonDataType.Long);
                            case BinaryExpressionType.Subtract:
                                return new StringField((long_value1 - long_value2).ToString(), JsonDataType.Long);
                            default:
                                return new StringField("");
                        }
                    }
                    else
                    {
                        throw new QueryCompilationException(string.Format("Cannot cast \"{0}\" or \"{1}\" to values of type \"long\"",
                            value1, value2));
                    }
                case JsonDataType.Double:
                    double double_value1, double_value2; 
                    if (double.TryParse(value1, out double_value1) && double.TryParse(value2, out double_value2))
                    {
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return new StringField((double_value1 + double_value2).ToString(), JsonDataType.Double);
                            case BinaryExpressionType.Divide:
                                return new StringField((double_value1 / double_value2).ToString(), JsonDataType.Double);
                            case BinaryExpressionType.Modulo:
                                return new StringField((double_value1 % double_value2).ToString(), JsonDataType.Double);
                            case BinaryExpressionType.Multiply:
                                return new StringField((double_value1 * double_value2).ToString(), JsonDataType.Double);
                            case BinaryExpressionType.Subtract:
                                return new StringField((double_value1 - double_value2).ToString(), JsonDataType.Double);
                            default:
                                throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied to operands of type 'double'.");
                        }
                    }
                    else
                    {
                        throw new QueryCompilationException(string.Format("Cannot cast \"{0}\" or \"{1}\" to values of type \"double\"",
                            value1, value2));
                    }
                case JsonDataType.Float:
                    float float_value1, float_value2;
                    if (float.TryParse(value1, out float_value1) && float.TryParse(value2, out float_value2))
                    {
                        switch (binaryType)
                        {
                            case BinaryExpressionType.Add:
                                return new StringField((float_value1 + float_value2).ToString(), JsonDataType.Float);
                            case BinaryExpressionType.Divide:
                                return new StringField((float_value1 / float_value2).ToString(), JsonDataType.Float);
                            case BinaryExpressionType.Modulo:
                                return new StringField((float_value1 % float_value2).ToString(), JsonDataType.Float);
                            case BinaryExpressionType.Multiply:
                                return new StringField((float_value1 * float_value2).ToString(), JsonDataType.Float);
                            case BinaryExpressionType.Subtract:
                                return new StringField((float_value1 - float_value2).ToString(), JsonDataType.Float);
                            default:
                                throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied to operands of type 'float'.");
                        }
                    }
                    else
                    {
                        throw new QueryCompilationException(string.Format("Cannot cast \"{0}\" or \"{1}\" to values of type \"float\"",
                            value1, value2));
                    }
                case JsonDataType.String:
                    switch (binaryType)
                    {
                        case BinaryExpressionType.Add:
                            return new StringField(value1 + value2);
                        default:
                            throw new QueryCompilationException("Operator " + binaryType.ToString() + " cannot be applied to operands of type \"string\".");
                    }
                case JsonDataType.Date:
                    throw new NotImplementedException();
                case JsonDataType.Null:
                    return null;
                default:
                    throw new QueryCompilationException("Unsupported data type.");
            }
        }

    }

    [Serializable]
    internal class ComposeCompositeField : ScalarFunction, ISerializable
    {
        List<Tuple<string, int>> targetFieldsAndTheirNames;
        string defaultProjectionKey;
        
        public ComposeCompositeField(List<Tuple<string, int>> targetFieldsAndTheirNames, string defaultProjectionKey)
        {
            this.targetFieldsAndTheirNames = targetFieldsAndTheirNames;
            this.defaultProjectionKey = defaultProjectionKey;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            Dictionary<string, FieldObject> compositField = new Dictionary<string, FieldObject>(targetFieldsAndTheirNames.Count);
            foreach (Tuple<string, int> p in targetFieldsAndTheirNames) {
                compositField[p.Item1] = record[p.Item2];
            }

            return new CompositeField(compositField, defaultProjectionKey);
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Object;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            List<string> targetFieldsAndTheirNames1 = new List<string>();
            List<int> targetFieldsAndTheirNames2 = new List<int>();
            foreach (Tuple<string, int> tuple in this.targetFieldsAndTheirNames)
            {
                targetFieldsAndTheirNames1.Add(tuple.Item1);
                targetFieldsAndTheirNames2.Add(tuple.Item2);
            }
            GraphViewSerializer.SerializeList(info, "targetFieldsAndTheirNames1", targetFieldsAndTheirNames1);
            GraphViewSerializer.SerializeList(info, "targetFieldsAndTheirNames2", targetFieldsAndTheirNames2);

            info.AddValue("defaultProjectionKey", this.defaultProjectionKey, typeof(string));
        }

        protected ComposeCompositeField(SerializationInfo info, StreamingContext context)
        {
            List<string> targetFieldsAndTheirNames1 =
                GraphViewSerializer.DeserializeList<string>(info, "targetFieldsAndTheirNames1");
            List<int> targetFieldsAndTheirNames2 =
                GraphViewSerializer.DeserializeList<int>(info, "targetFieldsAndTheirNames2");
            this.targetFieldsAndTheirNames = new List<Tuple<string, int>>();
            Debug.Assert(targetFieldsAndTheirNames1.Count == targetFieldsAndTheirNames2.Count);
            for (int i = 0; i < targetFieldsAndTheirNames1.Count; i++)
            {
                this.targetFieldsAndTheirNames.Add(new Tuple<string, int>(targetFieldsAndTheirNames1[i], targetFieldsAndTheirNames2[i]));
            }

            this.defaultProjectionKey = info.GetString("defaultProjectionKey");
        }
    }

    [Serializable]
    internal class Compose2 : ScalarFunction, ISerializable
    {
        List<ScalarFunction> inputOfCompose2;

        public Compose2(List<ScalarFunction> inputOfCompose2)
        {
            this.inputOfCompose2 = inputOfCompose2;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            List<FieldObject> results = new List<FieldObject>();
            foreach (var input in inputOfCompose2)
            {
                if (input == null)
                {
                    continue;
                }
                else if (input is Compose2)
                {
                    CollectionField subCompose2 = input.Evaluate(record) as CollectionField;
                    results.AddRange(subCompose2.Collection);
                }
                else if (input is ComposeCompositeField)
                {
                    results.Add(input.Evaluate(record));
                }
                else
                {
                    var resultField = input.Evaluate(record);
                    if (resultField == null) continue;

                    CollectionField compose2ResultField = resultField as CollectionField;
                    if (compose2ResultField == null)
                        throw new GraphViewException("A WColumnReference as the parameter of Compose2 must be located to a collection field.");
                    results.AddRange(compose2ResultField.Collection);
                }
            }

            return new CollectionField(results);
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Array;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "inputOfCompose2", this.inputOfCompose2);
        }

        protected Compose2(SerializationInfo info, StreamingContext context)
        {
            this.inputOfCompose2 = GraphViewSerializer.DeserializeList<ScalarFunction>(info, "inputOfCompose2");
        }
    }

    [Serializable]
    internal class WithOutArray : ScalarFunction
    {
        private int checkFieldIndex;
        private int arrayFieldIndex;

        public WithOutArray(int checkFieldIndex, int arrayFieldIndex)
        {
            this.checkFieldIndex = checkFieldIndex;
            this.arrayFieldIndex = arrayFieldIndex;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            FieldObject checkObject = record[this.checkFieldIndex];
            if (checkObject == null) return new StringField("false", JsonDataType.Boolean);

            CollectionField arrayObject = record[this.arrayFieldIndex] as CollectionField;
            if (arrayObject != null)
            {
                foreach (FieldObject fieldObject in arrayObject.Collection)
                {
                    if (fieldObject is CompositeField)
                    {
                        CompositeField compose1Field = fieldObject as CompositeField;
                        if (checkObject.Equals(compose1Field[compose1Field.DefaultProjectionKey]))
                            return new StringField("false", JsonDataType.Boolean);
                    }
                    else if (checkObject.Equals(fieldObject))
                    {
                        return new StringField("false", JsonDataType.Boolean);
                    }
                }

                return new StringField("true", JsonDataType.Boolean);
            }

            return checkObject.Equals(record[this.arrayFieldIndex])
                ? new StringField("false", JsonDataType.Boolean)
                : new StringField("true", JsonDataType.Boolean);
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Boolean;
        }
    }

    [Serializable]
    internal class WithInArray : ScalarFunction
    {
        private int checkFieldIndex;
        private int arrayFieldIndex;

        public WithInArray(int checkFieldIndex, int arrayFieldIndex)
        {
            this.checkFieldIndex = checkFieldIndex;
            this.arrayFieldIndex = arrayFieldIndex;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            FieldObject checkObject = record[this.checkFieldIndex];
            if (checkObject == null) return new StringField("false", JsonDataType.Boolean);

            CollectionField arrayObject = record[this.arrayFieldIndex] as CollectionField;
            if (arrayObject != null)
            {
                foreach (FieldObject fieldObject in arrayObject.Collection)
                {
                    if (fieldObject is CompositeField)
                    {
                        CompositeField compose1Field = fieldObject as CompositeField;
                        if (checkObject.Equals(compose1Field[compose1Field.DefaultProjectionKey]))
                            return new StringField("true", JsonDataType.Boolean);
                    }
                    else if (checkObject.Equals(fieldObject))
                    {
                        return new StringField("true", JsonDataType.Boolean);
                    }
                }

                return new StringField("false", JsonDataType.Boolean);
            }

            return checkObject.Equals(record[this.arrayFieldIndex])
                ? new StringField("true", JsonDataType.Boolean)
                : new StringField("false", JsonDataType.Boolean);
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Boolean;
        }
    }

    [Serializable]
    internal class HasProperty : ScalarFunction
    {
        private int _checkFieldIndex;
        private string _propertyName;

        public HasProperty(int checkFieldIndex, string propertyName)
        {
            _checkFieldIndex = checkFieldIndex;
            _propertyName = propertyName;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            FieldObject checkObject = record[_checkFieldIndex];

            VertexField vertexField = checkObject as VertexField;
            EdgeField edgeField = checkObject as EdgeField;

            if (vertexField != null)
            {
                if (vertexField.AllProperties.Count(pf => pf.PropertyName ==  _propertyName) > 0)
                    return new StringField("true", JsonDataType.Boolean);
                else
                    return new StringField("false", JsonDataType.Boolean);
            }
            else if (edgeField != null)
            {
                if (edgeField.EdgeProperties.ContainsKey(_propertyName))
                    return new StringField("true", JsonDataType.Boolean);
                else
                    return new StringField("false", JsonDataType.Boolean);
            }
            else
            {
                throw new GraphViewException(
                    "HasProperty() function can only be applied to a VertexField or EdgeField but now the object is " +
                    checkObject.GetType());
            }
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Boolean;
        }
    }

    [Serializable]
    internal class Path : ScalarFunction, ISerializable
    {
        //
        // If the boolean value is true, then it's a subPath to be unfolded
        //
        private List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList;

        public Path(List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList)
        {
            this.pathStepList = pathStepList;
        }

        public override FieldObject Evaluate(RawRecord record)
        {
            List<FieldObject> path = new List<FieldObject>();

            foreach (Tuple<ScalarFunction, bool, HashSet<string>> tuple in pathStepList)
            {
                ScalarFunction accessPathStepFunc = tuple.Item1;
                bool needsUnfold = tuple.Item2;
                HashSet<string> stepLabels = tuple.Item3;

                if (accessPathStepFunc == null)
                {
                    PathStepField pathStepField = new PathStepField(null);
                    foreach (string label in stepLabels)
                    {
                        pathStepField.AddLabel(label);
                    }
                    path.Add(pathStepField);
                    continue;
                }

                FieldObject step = accessPathStepFunc.Evaluate(record);
                if (step == null)
                {
                    PathStepField lastPathStep;

                    if (path.Any())
                    {
                        lastPathStep = (PathStepField)path[path.Count - 1];
                    }
                    else
                    {
                        lastPathStep = new PathStepField(null);
                        path.Add(lastPathStep);
                    }

                    foreach (string label in stepLabels)
                    {
                        lastPathStep.AddLabel(label);
                    }
                    continue;
                }

                if (needsUnfold)
                {
                    PathField subPath = step as PathField;
                    Debug.Assert(subPath != null, "(subPath as PathField) != null");

                    foreach (PathStepField subPathStep in subPath.Path.Cast<PathStepField>())
                    {
                        if (subPathStep.StepFieldObject == null)
                        {
                            if (path.Any())
                            {
                                PathStepField lastPathStep = (PathStepField) path[path.Count - 1];
                                foreach (string label in subPathStep.Labels)
                                {
                                    lastPathStep.AddLabel(label);
                                }
                            }
                            else
                            {
                                path.Add(subPathStep);
                            }
                            continue;
                        }

                        PathStepField pathStepField = new PathStepField(subPathStep.StepFieldObject);
                        foreach (string label in subPathStep.Labels)
                        {
                            pathStepField.AddLabel(label);
                        }
                        path.Add(pathStepField);
                    }

                    PathStepField lastSubPathStep = (PathStepField) path.Last();
                    foreach (string label in stepLabels)
                    {
                        lastSubPathStep.AddLabel(label);
                    }
                }
                else
                {
                    PathStepField pathStepField = new PathStepField(step);
                    foreach (string label in stepLabels)
                    {
                        pathStepField.AddLabel(label);
                    }
                    path.Add(pathStepField);
                }
            }

            return new PathField(path);
        }

        public override JsonDataType DataType()
        {
            return JsonDataType.Array;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeListTupleHashSet(info, "pathStepList", this.pathStepList);
        }

        protected Path(SerializationInfo info, StreamingContext context)
        {
            this.pathStepList = GraphViewSerializer.DeserializeListTupleHashSet<ScalarFunction, bool, string>(info, "pathStepList");
        }
    }
}
