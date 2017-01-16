using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class TableValuedFunction : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator InputOperator;
        protected Queue<RawRecord> OutputBuffer;
        protected int OutputBufferSize;

        internal TableValuedFunction(
            GraphViewExecutionOperator pInputOperator, 
            int pOutputBufferSize = 1000)
        {
            InputOperator = pInputOperator;
            OutputBufferSize = pOutputBufferSize;
            OutputBuffer = new Queue<RawRecord>(OutputBufferSize);
            this.Open();
        }

        internal abstract IEnumerable<RawRecord> CrossApply(RawRecord record);

        public override RawRecord Next()
        {
            if (OutputBuffer.Count != 0)
            {
                return OutputBuffer.Dequeue();
            }

            while (OutputBuffer.Count < OutputBufferSize && InputOperator.State())
            {
                var srcRecord = InputOperator.Next();
                if (srcRecord == null)
                    break;

                var results = CrossApply(srcRecord);
                foreach (var rec in results)
                {
                    var resultRecord = new RawRecord(srcRecord);
                    resultRecord.Append(rec);
                    OutputBuffer.Enqueue(resultRecord);
                }
            }

            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        public override void ResetState()
        {
            InputOperator.ResetState();
            OutputBuffer.Clear();
            this.Open();
        }
    }

    internal class PropertiesOperator : TableValuedFunction
    {
        List<Tuple<string, int>> propertyList;
        int allPropertyIndex;

        public PropertiesOperator(
            GraphViewExecutionOperator pInputOperatr, 
            List<Tuple<string, int>> pPropertiesList, 
            int pOutputBufferSize = 1000)
            : base(pInputOperatr, pOutputBufferSize)
        {
            propertyList = pPropertiesList;
        }

        public PropertiesOperator(
            GraphViewExecutionOperator inputOp, 
            int allPropertyIndex,
            int bufferSize = 1000)
            : base(inputOp, bufferSize)
        {
            this.allPropertyIndex = allPropertyIndex;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var pair in propertyList)
            {
                var propName = pair.Item1;
                var propIdx = pair.Item2;
                var result = new RawRecord(1);
                var fieldValue = record[propIdx];
                if (fieldValue == null) continue;;

                result.fieldValues[0] = new StringField(propName + "->" + fieldValue);
                results.Add(result);
            }

            return results;
        } 
    }

    internal class ValuesOperator : TableValuedFunction
    {
        internal List<int> ValuesIdxList;

        internal ValuesOperator(GraphViewExecutionOperator pInputOperator, List<int> pValuesIdxList, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            ValuesIdxList = pValuesIdxList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var propIdx in ValuesIdxList)
            {
                var result = new RawRecord(1);
                var fieldValue = record[propIdx];
                if (fieldValue == null) continue;

                result.fieldValues[0] = fieldValue;
                results.Add(result);
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
                var index = tuple.Item1;
                var needsUnfold = tuple.Item2;

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
                    if (record[index].GetType() == typeof(StringField))
                        pathCollection.Add(record[index]);
                    else
                        pathCollection.Add(new StringField(record[index].ToString()));
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
        private int _collectionFieldIndex;

        internal UnfoldOperator(
            GraphViewExecutionOperator pInputOperator,
            int pCollectionFieldIndex,
            int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            this._collectionFieldIndex = pCollectionFieldIndex;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();

            if (record[_collectionFieldIndex].GetType() != typeof(CollectionField))
            {
                throw new GraphViewException("The input of unfold must be a collection.");
            }

            CollectionField cf = record[_collectionFieldIndex] as CollectionField;
            foreach (FieldObject fo in cf.Collection)
            {
                RawRecord newRecord = new RawRecord();
                newRecord.Append(fo);
                results.Add(newRecord);
            }

            return results;
        }
    }

    internal class ProjectByOperator : TableValuedFunction
    {
        internal List<Tuple<ScalarFunction, string>> ProjectList;

        internal ProjectByOperator(GraphViewExecutionOperator pInputOperatr, List<Tuple<ScalarFunction, string>> pPropertiesList, 
            int pOutputBufferSize = 1000)
            : base(pInputOperatr, pOutputBufferSize)
        {
            ProjectList = pPropertiesList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var projectMap = new Dictionary<string, FieldObject>();

            foreach (var tuple in ProjectList)
            {
                var scalarFunction = tuple.Item1;
                var value = scalarFunction.Evaluate(record);
                var key = tuple.Item2;

                if (value == null)
                    throw new GraphViewException(
                        string.Format("The provided traverser of key \"{0}\" does not map to a value.", key));

                projectMap.Add(key, value);
            }

            var result = new RawRecord(1);
            result.fieldValues[0] = new MapField(projectMap);

            return new List<RawRecord> { result };
        }
    }
}
