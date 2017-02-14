using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal abstract class TableValuedFunction : GraphViewExecutionOperator
    {
        protected GraphViewExecutionOperator inputOperator;
        protected Queue<RawRecord> outputBuffer;
        protected int outputBufferSize;

        internal TableValuedFunction(
            GraphViewExecutionOperator pInputOperator, 
            int pOutputBufferSize = 1000)
        {
            inputOperator = pInputOperator;
            outputBufferSize = pOutputBufferSize;
            outputBuffer = new Queue<RawRecord>(outputBufferSize);
            this.Open();
        }

        internal abstract IEnumerable<RawRecord> CrossApply(RawRecord record);

        public override RawRecord Next()
        {
            if (outputBuffer.Count != 0)
            {
                return outputBuffer.Dequeue();
            }

            while (outputBuffer.Count < outputBufferSize && inputOperator.State())
            {
                var srcRecord = inputOperator.Next();
                if (srcRecord == null)
                    break;

                var results = CrossApply(srcRecord);
                foreach (var rec in results)
                {
                    var resultRecord = new RawRecord(srcRecord);
                    resultRecord.Append(rec);
                    outputBuffer.Enqueue(resultRecord);
                }
            }

            if (outputBuffer.Count <= 1) this.Close();
            if (outputBuffer.Count != 0) return outputBuffer.Dequeue();
            return null;
        }

        public override void ResetState()
        {
            inputOperator.ResetState();
            outputBuffer.Clear();
            this.Open();
        }
    }

    internal class PropertiesOperator : TableValuedFunction
    {
        List<Tuple<string, int>> propertyList;
        int allPropertyIndex;

        public PropertiesOperator(
            GraphViewExecutionOperator pInputOperator, 
            List<Tuple<string, int>> pPropertiesList,
            int pAllPropertyIndex,
            int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            propertyList = pPropertiesList;
            allPropertyIndex = pAllPropertyIndex;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();

            // Extract all properties if allPropertyIndex >= 0
            if (allPropertyIndex >= 0 && record[allPropertyIndex] != null)
            {
                VertexField vertexField = record[allPropertyIndex] as VertexField;
                if (vertexField != null)
                {
                    foreach (var propertyPair in vertexField.VertexProperties)
                    {
                        string propertyName = propertyPair.Key;
                        VertexPropertyField propertyField = propertyPair.Value;

                        switch (propertyName.ToLower())
                        {
                            // Reversed properties for meta-data
                            case "_edge":
                            case "_reverse_edge":
                            case "_nextedgeoffset":
                            case "_nextreverseedgeoffset":
                            case "_rid":
                            case "_self":
                            case "_etag":
                            case "_attachments":
                            case "_ts":
                                continue;
                            default:
                                RawRecord r = new RawRecord();
                                r.Append(propertyField);
                                results.Add(r);
                                break;
                        }
                    }
                }
                else
                {
                    EdgeField edgeField = record[allPropertyIndex] as EdgeField;
                    if (edgeField == null)
                        throw new GraphViewException(
                            string.Format("The FieldObject record[{0}] should be a VertexField or EdgeField but now it is {1}.",
                                allPropertyIndex, record[allPropertyIndex].ToString()));

                    foreach (var propertyPair in edgeField.EdgeProperties)
                    {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField propertyField = propertyPair.Value;

                        switch (propertyName.ToLower())
                        {
                            // Reversed properties for meta-data
                            case "_reverse_id":
                            case "_sink":
                            case "_sinklabel":
                                continue;
                            default:
                                RawRecord r = new RawRecord();
                                r.Append(propertyField);
                                results.Add(r);
                                break;
                        }
                    }
                }
            }
            else
            {
                // TODO: Now translation code needn't to generate the key name for the operator
                foreach (var pair in propertyList)
                {
                    //string propertyName = pair.Item1;
                    int propertyValueIndex = pair.Item2;
                    var propertyValue = record[propertyValueIndex];
                    if (propertyValue == null)
                    {
                        continue;
                    }

                    var result = new RawRecord();
                    result.Append(propertyValue);
                    results.Add(result);
                }
            }

            return results;
        } 
    }

    internal class ValuesOperator : TableValuedFunction
    {
        internal List<int> ValuesIdxList;
        int allValuesIndex;

        internal ValuesOperator(GraphViewExecutionOperator pInputOperator, List<int> pValuesIdxList, int pAllValuesIndex, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            ValuesIdxList = pValuesIdxList;
            allValuesIndex = pAllValuesIndex;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();

            // Extract all values if allValuesIndex >= 0
            if (allValuesIndex >= 0 && record[allValuesIndex] != null)
            {
                VertexField vertexField = record[allValuesIndex] as VertexField;
                if (vertexField != null)
                {
                    foreach (var propertyPair in vertexField.VertexProperties)
                    {
                        string propertyName = propertyPair.Key;
                        VertexPropertyField propertyField = propertyPair.Value;

                        switch (propertyName.ToLower())
                        {
                            // Reversed properties for meta-data
                            case "_edge":
                            case "_reverse_edge":
                            case "_nextedgeoffset":
                            case "_nextreverseedgeoffset":
                            case "_rid":
                            case "_self":
                            case "_etag":
                            case "_attachments":
                            case "_ts":
                                continue;
                            default:
                                RawRecord r = new RawRecord();
                                r.Append(new StringField(propertyField.ToValue));
                                results.Add(r);
                                break;
                        }
                    }
                }
                else
                {
                    EdgeField edgeField = record[allValuesIndex] as EdgeField;
                    if (edgeField == null)
                        throw new GraphViewException(
                            string.Format("The FieldObject record[{0}] should be a VertexField or EdgeField but now it is {1}.",
                                allValuesIndex, record[allValuesIndex].ToString()));

                    foreach (var propertyPair in edgeField.EdgeProperties)
                    {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField propertyField = propertyPair.Value;

                        switch (propertyName.ToLower())
                        {
                            // Reversed properties for meta-data
                            case "_reverse_id":
                            case "_sink":
                            case "_sinklabel":
                                continue;
                            default:
                                RawRecord r = new RawRecord();
                                r.Append(new StringField(propertyField.ToValue));
                                results.Add(r);
                                break;
                        }
                    }
                }
            }
            else
            {
                foreach (var propIdx in ValuesIdxList)
                {
                    var propertyValue = record[propIdx];
                    if (propertyValue == null)
                    {
                        continue;
                    }

                    var result = new RawRecord();
                    result.Append(new StringField(propertyValue.ToValue));
                    results.Add(result);
                }
            }

            return results;
        }
    }

    internal class ConstantOperator : TableValuedFunction
    {
        private List<string> _constantValues;

        internal ConstantOperator(GraphViewExecutionOperator pInputOperator, List<string> pConstantValues, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            _constantValues = pConstantValues;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var result = new RawRecord(1);
            if (_constantValues.Count == 1)
                result.fieldValues[0] = new StringField(_constantValues[0]);
            else
            {
                List<FieldObject> cf = new List<FieldObject>();
                foreach (var value in _constantValues)
                {
                    cf.Add(new StringField(value));
                }
                
                result.fieldValues[0] = new CollectionField(cf);
            }

            return new List<RawRecord> {result};
        }
    }

    internal class PathOperator : TableValuedFunction
    {
        // <field index, whether this field is a path list needed to be unfolded>
        private List<Tuple<int, bool>> _pathFieldList;

        public PathOperator(GraphViewExecutionOperator pInputOperator,
            List<Tuple<int, bool>> pStepFieldList,
            int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            this._pathFieldList = pStepFieldList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            List<FieldObject> pathCollection = new List<FieldObject>();

            foreach (var tuple in _pathFieldList)
            {
                int index = tuple.Item1;
                bool needsUnfold = tuple.Item2;

                if (record[index] == null) continue;
                if (needsUnfold)
                {
                    CollectionField cf = record[index] as CollectionField;
                    foreach (FieldObject fo in cf.Collection)
                    {
                        pathCollection.Add(fo);
                    }
                }
                else
                {
                    pathCollection.Add(record[index]);
                }
            }

            RawRecord newRecord = new RawRecord();
            CollectionField pathResult = new CollectionField(pathCollection);
            newRecord.Append(pathResult);

            return new List<RawRecord> {newRecord};
        }
    }

    internal class UnfoldOperator : TableValuedFunction
    {
        private ScalarFunction _unfoldTarget;
        private List<string> _unfoldColumns; 

        internal UnfoldOperator(
            GraphViewExecutionOperator pInputOperator,
            ScalarFunction pUnfoldTarget,
            List<string> pUnfoldColumns,
            int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            this._unfoldTarget = pUnfoldTarget;
            this._unfoldColumns = pUnfoldColumns;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject unfoldTarget = _unfoldTarget.Evaluate(record);

            if (unfoldTarget.GetType() == typeof (CollectionField))
            {
                CollectionField cf = unfoldTarget as CollectionField;
                foreach (FieldObject fo in cf.Collection)
                {
                    if (fo == null) continue;
                    RawRecord newRecord = new RawRecord();

                    // Extract only needed columns from Compose1Field
                    if (fo.GetType() == typeof (Compose1Field))
                    {
                        Compose1Field compose1Field = fo as Compose1Field;
                        foreach (string unfoldColumn in _unfoldColumns)
                        {
                            newRecord.Append(compose1Field.Map[new StringField(unfoldColumn)]);
                        }
                    }
                    else
                    {
                        newRecord.Append(fo);
                    }
                    results.Add(newRecord);
                }
            }
            else if (unfoldTarget.GetType() == typeof(MapField))
            {
                MapField mf = unfoldTarget as MapField;
                foreach (var pair in mf.Map)
                {
                    RawRecord newRecord = new RawRecord();
                    string key = pair.Key.ToString();
                    string value = pair.Value.ToString();

                    newRecord.Append(new StringField(key + "=" + value));
                    results.Add(newRecord);
                }
            }
            else
            {
                RawRecord newRecord = new RawRecord();
                newRecord.Append(unfoldTarget);
                results.Add(newRecord);
            }

            return results;
        }
    }
}
