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
        private static void ConstructTree(TreeField root, int index, PathField pathField)
        {
            if (index >= pathField.Path.Count) return;
            PathStepField pathStepField = pathField.Path[index++] as PathStepField;
            Debug.Assert(pathStepField != null, "pathStepField != null");
            Compose1Field compose1PathStep = pathStepField.StepFieldObject as Compose1Field;
            Debug.Assert(compose1PathStep != null, "compose1PathStep != null");
            FieldObject nodeObject = compose1PathStep[compose1PathStep.DefaultProjectionKey];

            TreeField child;
            if (!root.Children.TryGetValue(nodeObject, out child))
            {
                child = new TreeField(nodeObject);
                root.Children[nodeObject] = child;
            }

            ConstructTree(child, index, pathField);
        }

        private TreeField _root;

        public TreeFunction()
        {
            _root = new TreeField(new StringField("root"));
        }

        public void Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

            ConstructTree(_root, 0, values[0] as PathField);
        }

        public void Init()
        {
            _root = new TreeField(new StringField("root"));
        }

        public FieldObject Terminate()
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
        bool isProjectingACollection;

        public GroupFunction(ConstantSourceOperator tempSourceOp,
            ContainerOperator groupedSourceOp,
            GraphViewExecutionOperator aggregateOp,
            bool isProjectingACollection)
        {
            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            this.tempSourceOp = tempSourceOp;
            this.groupedSourceOp = groupedSourceOp;
            this.aggregateOp = aggregateOp;
            this.isProjectingACollection = isProjectingACollection;
        }

        public void Init()
        {
            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            throw new NotImplementedException();
        }

        public void Accumulate(params Object[] values)
        {
            FieldObject groupByKey = values[0] as FieldObject;
            RawRecord groupByValue = values[1] as RawRecord;

            if (!this.groupedStates.ContainsKey(groupByKey)) {
                this.groupedStates.Add(groupByKey, new List<RawRecord>());
            }

            this.groupedStates[groupByKey].Add(groupByValue);
        }

        public FieldObject Terminate()
        {
            MapField result = new MapField();

            if (this.isProjectingACollection)
            {
                foreach (FieldObject key in groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    foreach (RawRecord rawRecord in groupedStates[key])
                    {
                        this.groupedSourceOp.ResetState();
                        this.aggregateOp.ResetState();
                        this.tempSourceOp.ConstantSource = rawRecord;
                        this.groupedSourceOp.Next();

                        RawRecord aggregateTraversalRecord = this.aggregateOp.Next();
                        FieldObject projectResult = aggregateTraversalRecord?.RetriveData(0);

                        if (projectResult == null) {
                            throw new GraphViewException("The property does not exist for some of the elements having been grouped.");
                        }

                        projectFields.Add(projectResult);
                    }
                    result[key] = new CollectionField(projectFields);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in this.groupedStates)
                {
                    FieldObject key = pair.Key;
                    List<RawRecord> aggregatedRecords = pair.Value;
                    this.groupedSourceOp.ResetState();
                    this.aggregateOp.ResetState();

                    foreach (RawRecord record in aggregatedRecords)
                    {
                        this.tempSourceOp.ConstantSource = record;
                        this.groupedSourceOp.Next();
                    }

                    RawRecord aggregateTraversalRecord = this.aggregateOp.Next();

                    FieldObject aggregateResult = aggregateTraversalRecord?.RetriveData(0);
                    if (aggregateResult == null) {
                        return null;
                    }

                    result[key] = aggregateResult;
                }
            }

            return result;
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
                MapField map = new MapField();

                foreach (Tuple<string, IAggregateFunction> tuple in sideEffectStates)
                {
                    string key = tuple.Item1;
                    IAggregateFunction sideEffectState = tuple.Item2;

                    map.Add(new StringField(key), sideEffectState.Terminate());
                }

                return map;
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
            GroupFunction groupState,
            ScalarFunction groupByKeyFunction)
        {
            this.inputOp = inputOp;
            this.GroupState = groupState;
            this.groupByKeyFunction = groupByKeyFunction;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.inputOp.State())
            {
                RawRecord r = this.inputOp.Next();
                if (r == null)
                {
                    this.Close();
                    return null;
                }

                FieldObject groupByKey = this.groupByKeyFunction.Evaluate(r);

                if (groupByKey == null) {
                    throw new GraphViewException("The provided property name or traversal does not map to a value for some elements.");
                }

                this.GroupState.Accumulate(new Object[]{ groupByKey, r });

                if (!this.inputOp.State()) {
                    this.Close();
                }
                return r;
            }

            return null;
        }

        public override void ResetState()
        {
            //GroupState.Init();
            this.inputOp.ResetState();
            this.Open();
        }
    }

    internal class TreeSideEffectOperator : GraphViewExecutionOperator
    {
        public TreeFunction TreeState { get; private set; }
        GraphViewExecutionOperator inputOp;
        int pathIndex;

        public TreeSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            TreeFunction treeState,
            int pathIndex)
        {
            this.inputOp = inputOp;
            this.TreeState = treeState;
            this.pathIndex = pathIndex;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.inputOp.State())
            {
                RawRecord r = this.inputOp.Next();
                if (r == null)
                {
                    this.Close();
                    return null;
                }

                PathField path = r[this.pathIndex] as PathField;

                Debug.Assert(path != null);

                this.TreeState.Accumulate(path);

                if (!this.inputOp.State()) {
                    this.Close();
                }
                return r;
            }

            return null;
        }

        public override void ResetState()
        {
            //TreeState.Init();
            this.inputOp.ResetState();
            this.Open();
        }
    }

    internal class GroupOperator : GraphViewExecutionOperator
    {
        GraphViewExecutionOperator inputOp;

        ScalarFunction groupByKeyFunction;

        GraphViewExecutionOperator aggregateOp;
        ConstantSourceOperator tempSourceOp;
        ContainerOperator groupedSourceOp;

        bool isProjectingACollection;
        int carryOnCount;

        Dictionary<FieldObject, List<RawRecord>> groupedStates;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ConstantSourceOperator tempSourceOp,
            ContainerOperator groupedSourceOp,
            GraphViewExecutionOperator aggregateOp,
            bool isProjectingACollection,
            int carryOnCount)
        {
            this.inputOp = inputOp;

            this.groupByKeyFunction = groupByKeyFunction;

            this.tempSourceOp = tempSourceOp;
            this.groupedSourceOp = groupedSourceOp;
            this.aggregateOp = aggregateOp;

            this.isProjectingACollection = isProjectingACollection;
            this.carryOnCount = carryOnCount;

            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            this.Open();
        }

        public override RawRecord Next()
        {
            if (!this.State()) return null;

            RawRecord r = null;
            while (this.inputOp.State() && (r = this.inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);

                if (groupByKey == null) {
                    throw new GraphViewException("The provided property name or traversal does not map to a value for some elements.");
                }

                if (!this.groupedStates.ContainsKey(groupByKey)) {
                    this.groupedStates.Add(groupByKey, new List<RawRecord>());
                }
                this.groupedStates[groupByKey].Add(r);
            }

            MapField result = new MapField(this.groupedStates.Count);

            if (this.isProjectingACollection)
            {
                foreach (FieldObject key in this.groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    foreach (RawRecord rawRecord in this.groupedStates[key])
                    {
                        this.groupedSourceOp.ResetState();
                        this.aggregateOp.ResetState();
                        this.tempSourceOp.ConstantSource = rawRecord;
                        this.groupedSourceOp.Next();

                        RawRecord aggregateTraversalRecord = this.aggregateOp.Next();
                        FieldObject projectResult = aggregateTraversalRecord?.RetriveData(0);

                        if (projectResult == null) {
                            throw new GraphViewException("The property does not exist for some of the elements having been grouped.");
                        }

                        projectFields.Add(projectResult);
                    }
                    result[key] = new CollectionField(projectFields);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in this.groupedStates)
                {
                    FieldObject key = pair.Key;
                    List<RawRecord> aggregatedRecords = pair.Value;
                    this.groupedSourceOp.ResetState();
                    this.aggregateOp.ResetState();

                    foreach (RawRecord record in aggregatedRecords)
                    {
                        this.tempSourceOp.ConstantSource = record;
                        this.groupedSourceOp.Next();
                    }

                    RawRecord aggregateTraversalRecord = this.aggregateOp.Next();

                    FieldObject aggregateResult = aggregateTraversalRecord?.RetriveData(0);
                    if (aggregateResult == null)
                    {
                        this.Close();
                        return null;
                    }

                    result[key] = aggregateResult;
                }
            }

            RawRecord resultRecord = new RawRecord();

            for (int i = 0; i < this.carryOnCount; i++) {
                resultRecord.Append((FieldObject)null);
            }

            resultRecord.Append(result);

            this.Close();
            return resultRecord;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.groupedStates.Clear();
            this.Open();
        }
    }
}
