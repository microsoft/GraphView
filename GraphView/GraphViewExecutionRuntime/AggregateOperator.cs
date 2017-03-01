using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

namespace GraphView
{
    internal class FoldFunction : IAggregateFunction
    {
        List<FieldObject> buffer;

        public FoldFunction() { }

        public void Accumulate(params FieldObject[] values)
        {
            buffer.Add(values[0]);
        }

        public void Init()
        {
            buffer = new List<FieldObject>();
        }

        public FieldObject Terminate()
        {
            return new CollectionField(buffer);
        }
    }

    internal class CountFunction : IAggregateFunction
    {
        long count;

        public void Accumulate(params FieldObject[] values)
        {
            count++;
        }

        public void Init()
        {
            count = 0;
        }

        public FieldObject Terminate()
        {
            return new StringField(count.ToString(), JsonDataType.Long);
        }
    }

    internal class SumFunction : IAggregateFunction
    {
        double sum;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Sum cannot be cast to a number");

            sum += current;
        }

        public void Init()
        {
            sum = 0.0;
        }

        public FieldObject Terminate()
        {
            return new StringField(sum.ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    internal class MaxFunction : IAggregateFunction
    {
        double max;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Max cannot be cast to a number");

            if (max < current)
                max = current;
        }

        public void Init()
        {
            max = double.MinValue;
        }

        public FieldObject Terminate()
        {
            return new StringField(max.ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    internal class MinFunction : IAggregateFunction
    {
        double min;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Min cannot be cast to a number");

            if (current < min)
                min = current;
        }

        public void Init()
        {
            min = double.MaxValue;
        }

        public FieldObject Terminate()
        {
            return new StringField(min.ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    internal class MeanFunction : IAggregateFunction
    {
        double sum;
        long count;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Mean cannot be cast to a number");

            sum += current;
            count++;
        }

        public void Init()
        {
            sum = 0.0;
            count = 0;
        }

        public FieldObject Terminate()
        {
            return new StringField((sum / count).ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    internal class TreeFunction : IAggregateFunction
    {
        private static void ConstructTree(TreeField root, int index, CollectionField path)
        {
            if (index >= path.Collection.Count) return;
            FieldObject nodeObject = path.Collection[index++];

            TreeField child;
            if (!root.Children.TryGetValue(nodeObject, out child))
            {
                child = new TreeField(nodeObject);
                root.Children[nodeObject] = child;
            }

            ConstructTree(child, index, path);
        }

        private TreeField _root;

        void IAggregateFunction.Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

            ConstructTree(_root, 0, values[0] as CollectionField);
        }

        void IAggregateFunction.Init()
        {
            _root = new TreeField(new StringField("root"));
        }

        FieldObject IAggregateFunction.Terminate()
        {
            return _root;
        }
    }

    internal class CollectionFunction : IAggregateFunction
    {
        public CollectionField CollectionField { get; private set; }

        public CollectionFunction()
        {
            CollectionField = new CollectionField();
        }

        public void Init()
        {
            CollectionField = new CollectionField();
        }

        public void Accumulate(params FieldObject[] values)
        {
            CollectionField.Collection.Add(values[0]);
        }

        public FieldObject Terminate()
        {
            return CollectionField;
        }
    }

    internal class GroupFunction : IAggregateFunction
    {
        Dictionary<FieldObject, List<RawRecord>> groupedStates;
        GraphViewExecutionOperator aggregateOp;
        ConstantSourceOperator tempSourceOp;
        ContainerOperator groupedSourceOp;
        int elementPropertyProjectionIndex;

        public GroupFunction(ConstantSourceOperator tempSourceOp,
            ContainerOperator groupedSourceOp,
            GraphViewExecutionOperator aggregateOp,
            int elementPropertyProjectionIndex)
        {
            groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            this.tempSourceOp = tempSourceOp;
            this.groupedSourceOp = groupedSourceOp;
            this.aggregateOp = aggregateOp;
            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;
        }

        public void Init()
        {
            groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            throw new NotImplementedException();
        }

        public void Accumulate(params Object[] values)
        {
            FieldObject groupByKey = values[0] as FieldObject;
            RawRecord groupByValue = values[1] as RawRecord;

            if (!groupedStates.ContainsKey(groupByKey))
            {
                groupedStates.Add(groupByKey, new List<RawRecord>());
            }

            groupedStates[groupByKey].Add(groupByValue);
        }

        public FieldObject Terminate()
        {
            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(groupedStates.Count);

            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    foreach (var rawRecord in groupedStates[key])
                    {
                        projectFields.Add(new StringField(rawRecord[elementPropertyProjectionIndex].ToValue));
                    }
                    resultCollection[key] = new CollectionField(projectFields);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in groupedStates)
                {
                    FieldObject key = pair.Key;
                    List<RawRecord> aggregatedRecords = pair.Value;
                    groupedSourceOp.ResetState();
                    aggregateOp.ResetState();

                    foreach (RawRecord record in aggregatedRecords)
                    {
                        tempSourceOp.ConstantSource = record;
                        groupedSourceOp.Next();
                    }

                    RawRecord aggregateTraversalRecord = aggregateOp.Next();

                    FieldObject aggregateResult = aggregateTraversalRecord?.RetriveData(0);
                    if (aggregateResult == null)
                    {
                        return null;
                    }

                    resultCollection[key] = aggregateResult;
                }
            }

            return new MapField(resultCollection);
        }
    }

    internal class CapAggregate : IAggregateFunction
    {
        List<Tuple<string, IAggregateFunction>> sideEffectStates;

        public CapAggregate()
        {
            sideEffectStates = new List<Tuple<string, IAggregateFunction>>();
        }

        public void AddCapatureSideEffectState(string key, IAggregateFunction sideEffectState)
        {
            sideEffectStates.Add(new Tuple<string, IAggregateFunction>(key, sideEffectState));
        }

        public void Accumulate(params FieldObject[] values)
        {
            return;
        }

        public void Init()
        {
            return;
        }

        public FieldObject Terminate()
        {
            if (sideEffectStates.Count == 1)
            {
                Tuple<string, IAggregateFunction> tuple = sideEffectStates[0];
                IAggregateFunction sideEffectState = tuple.Item2;

                return sideEffectState.Terminate();
            }
            else
            {
                Dictionary<FieldObject, FieldObject> map = new Dictionary<FieldObject, FieldObject>();

                foreach (Tuple<string, IAggregateFunction> tuple in sideEffectStates)
                {
                    string key = tuple.Item1;
                    IAggregateFunction sideEffectState = tuple.Item2;

                    map.Add(new StringField(key), sideEffectState.Terminate());
                }

                return new MapField(map);
            }
        }
    }

    internal class GroupSideEffectOperator : GraphViewExecutionOperator
    {
        public GroupFunction GroupState { get; private set; }
        GraphViewExecutionOperator inputOp;
        ScalarFunction groupByKeyFunction;
        int groupByKeyFieldIndex;

        public GroupSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            GroupFunction groupState,
            ScalarFunction groupByKeyFunction,
            int groupByKeyFieldIndex)
        {
            this.inputOp = inputOp;
            this.GroupState = groupState;
            this.groupByKeyFunction = groupByKeyFunction;
            this.groupByKeyFieldIndex = groupByKeyFieldIndex;

            Open();
        }

        public override RawRecord Next()
        {
            if (inputOp.State())
            {
                RawRecord r = inputOp.Next();
                if (r == null)
                {
                    Close();
                    return null;
                }

                FieldObject groupByKey = groupByKeyFieldIndex >= 0 
                    ? new StringField(r[groupByKeyFieldIndex].ToValue) 
                    : groupByKeyFunction.Evaluate(r);

                GroupState.Accumulate(new Object[]{ groupByKey, r });

                if (!inputOp.State())
                {
                    Close();
                }
                return r;
            }

            return null;
        }

        public override void ResetState()
        {
            //GroupState.Init();
            inputOp.ResetState();
            Open();
        }
    }

    internal class GroupOperator : GraphViewExecutionOperator
    {
        GraphViewExecutionOperator inputOp;

        ScalarFunction groupByKeyFunction;
        int groupByKeyFieldIndex;

        GraphViewExecutionOperator aggregateOp;
        ConstantSourceOperator tempSourceOp;
        ContainerOperator groupedSourceOp;

        int elementPropertyProjectionIndex;
        int carryOnCount;

        Dictionary<FieldObject, List<RawRecord>> groupedStates;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            int groupByKeyFieldIndex,
            ConstantSourceOperator tempSourceOp,
            ContainerOperator groupedSourceOp,
            GraphViewExecutionOperator aggregateOp,
            int elementPropertyProjectionIndex,
            int carryOnCount)
        {
            this.inputOp = inputOp;

            this.groupByKeyFunction = groupByKeyFunction;
            this.groupByKeyFieldIndex = groupByKeyFieldIndex;

            this.tempSourceOp = tempSourceOp;
            this.groupedSourceOp = groupedSourceOp;
            this.aggregateOp = aggregateOp;

            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;
            this.carryOnCount = carryOnCount;

            groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            Open();
        }

        public override RawRecord Next()
        {
            if (!State()) return null;

            RawRecord r = null;
            while (inputOp.State() && (r = inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFieldIndex >= 0 
                    ? new StringField(r[groupByKeyFieldIndex].ToValue) 
                    : groupByKeyFunction.Evaluate(r);

                if (!groupedStates.ContainsKey(groupByKey))
                {
                    groupedStates.Add(groupByKey, new List<RawRecord>());
                }
                groupedStates[groupByKey].Add(r);
            }

            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(groupedStates.Count);
            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    foreach (var rawRecord in groupedStates[key])
                    {
                        projectFields.Add(new StringField(rawRecord[elementPropertyProjectionIndex].ToValue));
                    }
                    resultCollection[key] = new CollectionField(projectFields);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in groupedStates)
                {
                    FieldObject key = pair.Key;
                    List<RawRecord> aggregatedRecords = pair.Value;
                    groupedSourceOp.ResetState();
                    aggregateOp.ResetState();

                    foreach (RawRecord record in aggregatedRecords)
                    {
                        tempSourceOp.ConstantSource = record;
                        groupedSourceOp.Next();
                    }

                    RawRecord aggregateTraversalRecord = aggregateOp.Next();

                    FieldObject aggregateResult = aggregateTraversalRecord?.RetriveData(0);
                    if (aggregateResult == null)
                    {
                        Close();
                        return null;
                    }

                    resultCollection[key] = aggregateResult;
                }
            }

            RawRecord resultRecord = new RawRecord();

            for (int i = 0; i < carryOnCount; i++)
                resultRecord.Append((FieldObject)null);

            resultRecord.Append(new MapField(resultCollection));

            Close();
            return resultRecord;
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            groupedStates.Clear();
            Open();
        }
    }
}
