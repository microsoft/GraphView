using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
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
            return new StringField(count.ToString(), JsonDataType.Int);
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

    internal class StoreStateFunction : IAggregateFunction
    {
        List<FieldObject> aggregateState;

        public StoreStateFunction()
        {
            aggregateState = new List<FieldObject>();
        }

        public void Init()
        {
            aggregateState = new List<FieldObject>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            aggregateState.Add(values[0]);
        }

        public FieldObject Terminate()
        {
            return new CollectionField(aggregateState);
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
            var groupByKey = values[0] as FieldObject;
            var groupByValue = values[1] as RawRecord;

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
        List<Tuple<string, List<IAggregateFunction>>> sideEffectStates;

        public CapAggregate()
        {
            sideEffectStates = new List<Tuple<string, List<IAggregateFunction>>>();
        }

        public void AddCapatureSideEffectState(string key, List<IAggregateFunction> sideEffectList)
        {
            sideEffectStates.Add(new Tuple<string, List<IAggregateFunction>>(key, sideEffectList));
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
                List<FieldObject> collection = new List<FieldObject>();

                Tuple<string, List<IAggregateFunction>> tuple = sideEffectStates[0];
                List<IAggregateFunction> sideEffectStateList = tuple.Item2;

                int capturedStoreCount = 0;
                int capturedGroupCount = 0;
                MapField mergedMapField = null;
                foreach (var sideEffectState in sideEffectStateList)
                {
                    FieldObject capResult = sideEffectState.Terminate();

                    if (capResult is CollectionField)
                    {
                        if (capturedGroupCount > 0)
                            throw new GraphViewException("It's illegal to use the same parameter of a group(string) step and a store(string) step!");
                        capturedStoreCount++;
                        collection.AddRange((capResult as CollectionField).Collection);
                    }
                    else if (capResult is MapField)
                    {
                        if (capturedStoreCount > 0)
                            throw new GraphViewException("It's illegal to use the same parameter of a group(string) step and a store(string) step!");
                        capturedGroupCount++;

                        if (mergedMapField == null)
                            mergedMapField = capResult as MapField;
                        else
                        {
                            foreach (var pair in (capResult as MapField).Map)
                            {
                                FieldObject mapKey = pair.Key;
                                FieldObject value = pair.Value;

                                FieldObject cf;
                                if (!mergedMapField.Map.TryGetValue(mapKey, out cf))
                                {
                                    mergedMapField.Map.Add(mapKey, value);
                                }
                                else
                                {
                                    Debug.Assert(cf is CollectionField && value is CollectionField, "Group() should yield a MapField with value as CollectionField.");
                                    (cf as CollectionField).Collection.AddRange((value as CollectionField).Collection);
                                }
                            }
                        }
                    }
                }

                if (capturedStoreCount > 0)
                    return new CollectionField(collection);
                else
                    return mergedMapField;
            }
            else
            {
                Dictionary<FieldObject, FieldObject> map = new Dictionary<FieldObject, FieldObject>();

                foreach (var tuple in sideEffectStates)
                {
                    List<FieldObject> collection = new List<FieldObject>();

                    string key = tuple.Item1;
                    List<IAggregateFunction> sideEffectStateList = tuple.Item2;

                    int capturedStoreCount = 0;
                    int capturedGroupCount = 0;
                    MapField mergedMapField = null;
                    foreach (var sideEffectState in sideEffectStateList)
                    {
                        FieldObject capResult = sideEffectState.Terminate();

                        if (capResult is CollectionField)
                        {
                            if (capturedGroupCount > 0)
                                throw new GraphViewException("It's illegal to use the same parameter of a group(string) step and a store(string) step!");
                            capturedStoreCount++;
                            collection.AddRange((capResult as CollectionField).Collection);
                        }
                        else if (capResult is MapField)
                        {
                            if (capturedStoreCount > 0)
                                throw new GraphViewException("It's illegal to use the same parameter of a group(string) step and a store(string) step!");
                            capturedGroupCount++;

                            if (mergedMapField == null)
                                mergedMapField = capResult as MapField;
                            else
                            {
                                foreach (var pair in (capResult as MapField).Map)
                                {
                                    FieldObject mapKey = pair.Key;
                                    FieldObject value = pair.Value;

                                    FieldObject cf;
                                    if (!mergedMapField.Map.TryGetValue(mapKey, out cf))
                                    {
                                        mergedMapField.Map.Add(mapKey, value);
                                    }
                                    else
                                    {
                                        Debug.Assert(cf is CollectionField && value is CollectionField, "Group() should yield a MapField with value as CollectionField.");
                                        (cf as CollectionField).Collection.AddRange((value as CollectionField).Collection);
                                    }
                                }
                            }
                        }
                    }

                    if (capturedStoreCount > 0)
                        map.Add(new StringField(key), new CollectionField(collection));
                    else
                        map.Add(new StringField(key), mergedMapField);
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
            ScalarFunction groupByKeyFunction,
            int groupByKeyFieldIndex,
            ConstantSourceOperator tempSourceOp,
            ContainerOperator groupedSourceOp,
            GraphViewExecutionOperator aggregateOp,
            int elementPropertyProjectionIndex)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;
            this.groupByKeyFieldIndex = groupByKeyFieldIndex;

            GroupState = new GroupFunction(tempSourceOp, groupedSourceOp, aggregateOp, elementPropertyProjectionIndex);
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
            //groupedStates.Clear();
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
