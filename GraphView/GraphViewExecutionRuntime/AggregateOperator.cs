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
            return new StringField(count.ToString());
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
        Dictionary<FieldObject, List<RawRecord>> aggregateState;
        ScalarFunction aggregateTargetFunction;
        int elementPropertyProjectionIndex;

        public GroupFunction(ScalarFunction aggregateTargetFunction, int elementPropertyProjectionIndex)
        {
            aggregateState = new Dictionary<FieldObject, List<RawRecord>>();
            this.aggregateTargetFunction = aggregateTargetFunction;
            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;
        }

        public void Init()
        {
            aggregateState = new Dictionary<FieldObject, List<RawRecord>>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            throw new NotImplementedException();
        }

        public void Accumulate(params Object[] values)
        {
            var groupByKey = values[0] as FieldObject;
            var groupByValue = values[1] as RawRecord;

            if (!aggregateState.ContainsKey(groupByKey))
            {
                aggregateState.Add(groupByKey, new List<RawRecord>());
            }

            aggregateState[groupByKey].Add(groupByValue);
        }

        public FieldObject Terminate()
        {
            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(aggregateState.Count);

            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in aggregateState.Keys)
                {
                    List<FieldObject> fo = new List<FieldObject>();
                    foreach (var rawRecord in aggregateState[key])
                    {
                        fo.Add(rawRecord[elementPropertyProjectionIndex]);
                    }
                    resultCollection[key] = new CollectionField(fo);
                }
            }
            else
            {
                foreach (FieldObject key in aggregateState.Keys)
                {
                    RawRecord rc = aggregateState[key][0];
                    FieldObject aggregateResult = aggregateTargetFunction.Evaluate(rc);
                    if (aggregateResult == null)
                    {
                        return null;
                    }
                    CollectionField cf = new CollectionField();
                    cf.Collection.Add(aggregateResult);
                    resultCollection[key] = cf;
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

        public GroupSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ScalarFunction aggregateTargetFunction,
            int elementPropertyProjectionIndex)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;

            GroupState = new GroupFunction(aggregateTargetFunction, elementPropertyProjectionIndex);
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

                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);

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
        ScalarFunction aggregateTargetFunction;
        int elementPropertyProjectionIndex;
        int carryOnCount;

        Dictionary<FieldObject, List<RawRecord>> aggregatedState;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ScalarFunction aggregateTargetFunction,
            int elementPropertyProjectionIndex,
            int carryOnCount)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;
            this.aggregateTargetFunction = aggregateTargetFunction;
            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;
            this.carryOnCount = carryOnCount;

            aggregatedState = new Dictionary<FieldObject, List<RawRecord>>();
            Open();
            //aggregatedState.Clear();
        }

        public override RawRecord Next()
        {
            RawRecord r = null;
            while (inputOp.State() && (r = inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);
                if (!aggregatedState.ContainsKey(groupByKey))
                {
                    aggregatedState.Add(groupByKey, new List<RawRecord>());
                }
                aggregatedState[groupByKey].Add(r);
            }

            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(aggregatedState.Count);
            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in aggregatedState.Keys)
                {
                    List<FieldObject> fo = new List<FieldObject>();
                    foreach (var rawRecord in aggregatedState[key])
                    {
                        fo.Add(rawRecord[elementPropertyProjectionIndex]);
                    }
                    resultCollection[key] = new CollectionField(fo);
                }
            }
            else
            {
                foreach (FieldObject key in aggregatedState.Keys)
                {
                    RawRecord rc = aggregatedState[key][0];
                    FieldObject aggregateResult = aggregateTargetFunction.Evaluate(rc);
                    if (aggregateResult == null)
                    {
                        Close();
                        return null;
                    }

                    CollectionField cf = new CollectionField();
                    cf.Collection.Add(aggregateResult);
                    resultCollection[key] = cf;
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
            aggregatedState.Clear();
            Open();
        }
    }
}
