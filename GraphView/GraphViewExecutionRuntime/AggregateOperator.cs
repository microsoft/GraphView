using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [DataContract]
    public abstract class AggregateState
    {
        [DataMember]
        internal readonly string tableAlias;

        protected AggregateState(string tableAlias)
        {
            this.tableAlias = tableAlias;
        }

        public virtual void Init() { }
    }

    internal interface IAggregateFunction
    {
        void Init();
        void Accumulate(params FieldObject[] values);
        FieldObject Terminate();
    }

    [DataContract]
    internal class FoldFunction : IAggregateFunction
    {
        private List<FieldObject> buffer;

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

    [DataContract]
    internal class CountFunction : IAggregateFunction
    {
        private long count;

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

    [DataContract]
    internal class SumFunction : IAggregateFunction
    {
        private double sum;

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

    [DataContract]
    internal class MaxFunction : IAggregateFunction
    {
        private double max;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Max cannot be cast to a number");

            if (max.Equals(double.NaN) || max < current)
                max = current;
        }

        public void Init()
        {
            max = double.NaN;
        }

        public FieldObject Terminate()
        {
            return new StringField(max.ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    [DataContract]
    internal class MinFunction : IAggregateFunction
    {
        private double min;

        public void Accumulate(params FieldObject[] values)
        {
            double current;
            if (!double.TryParse(values[0].ToValue, out current))
                throw new GraphViewException("The input of Min cannot be cast to a number");

            if (min.Equals(double.NaN) || current < min)
                min = current;
        }

        public void Init()
        {
            min = double.NaN;
        }

        public FieldObject Terminate()
        {
            return new StringField(min.ToString(CultureInfo.InvariantCulture), JsonDataType.Double);
        }
    }

    [DataContract]
    internal class MeanFunction : IAggregateFunction
    {
        private double sum;
        private long count;

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

    [DataContract]
    internal class CapFunction : IAggregateFunction
    {
        [DataMember]
        private List<Tuple<string, IAggregateFunction>> sideEffectFunction;

        public CapFunction()
        {
            sideEffectFunction = new List<Tuple<string, IAggregateFunction>>();
        }

        public void AddCapatureSideEffectState(string key, IAggregateFunction sideEffectState)
        {
            sideEffectFunction.Add(new Tuple<string, IAggregateFunction>(key, sideEffectState));
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
            if (sideEffectFunction.Count == 1)
            {
                Tuple<string, IAggregateFunction> tuple = sideEffectFunction[0];
                IAggregateFunction sideEffectState = tuple.Item2;

                return sideEffectState.Terminate();
            }
            else
            {
                MapField map = new MapField();

                foreach (Tuple<string, IAggregateFunction> tuple in sideEffectFunction)
                {
                    string key = tuple.Item1;
                    IAggregateFunction sideEffectState = tuple.Item2;

                    map.Add(new StringField(key), sideEffectState.Terminate());
                }

                return map;
            }
        }

        [OnDeserialized]
        private void ReconstructFunction(StreamingContext context)
        {
            List<Tuple<string, IAggregateFunction>> states = new List<Tuple<string, IAggregateFunction>>();

            foreach (Tuple<string, IAggregateFunction> tuple in this.sideEffectFunction)
            {
                states.Add(new Tuple<string, IAggregateFunction>(tuple.Item1, SerializationData.SideEffectStates[tuple.Item1]));
            }

            this.sideEffectFunction = states;
        }
    }

    [DataContract]
    internal class TreeFunction : IAggregateFunction
    {
        internal TreeState treeState;

        private static void ConstructTree(TreeField root, int index, PathField pathField)
        {
            if (index >= pathField.Path.Count)
            {
                return;
            }
            PathStepField pathStepField = pathField.Path[index++] as PathStepField;
            Debug.Assert(pathStepField != null, "pathStepField != null");
            CompositeField compose1PathStep = pathStepField.StepFieldObject as CompositeField;
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

        public TreeFunction(TreeState treeState)
        {
            this.treeState = treeState;
        }

        public void Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

            ConstructTree(this.treeState.root, 0, values[0] as PathField);
        }

        public void Init()
        {
            this.treeState.Init();
        }

        public FieldObject Terminate()
        {
            return this.treeState.root;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.treeState = new TreeState("");
        }
    }

    internal class TreeState : AggregateState
    {
        internal TreeField root;

        public TreeState(string tableAlias) : base(tableAlias)
        {
            this.root = new TreeField(new StringField("root"));
        }

        public override void Init()
        {
            this.root = new TreeField(new StringField("root"));
        }
    }

    [DataContract]
    internal class SubgraphFunction : IAggregateFunction
    {
        internal SubgraphState subgraphState;

        public SubgraphFunction(SubgraphState subgraphState)
        {
            this.subgraphState = subgraphState;
        }

        public void Init()
        {
            this.subgraphState.Init();
        }

        public void Accumulate(params FieldObject[] values)
        {
            EdgeField edge = values[0] as EdgeField;
            if (edge == null)
            {
                throw new QueryExecutionException("Only edge can be an subgraph() input.");
            }

            this.subgraphState.edgeIds.Add(edge.EdgeId);

            this.subgraphState.vertexIds.Add(edge.InV);
            this.subgraphState.vertexIds.Add(edge.OutV);

            this.subgraphState.graph = null;
        }

        public FieldObject Terminate()
        {
            if (this.subgraphState.graph != null)
            {
                return this.subgraphState.graph;
            }


            if (this.subgraphState.vertexIds.Any())
            {
                // this API would modify the parameter, so deep copy it first.
                List<VertexField> vertices = this.subgraphState.command.Connection.CreateDatabasePortal()
                    .GetVerticesByIds(new HashSet<string>(this.subgraphState.vertexIds), this.subgraphState.command, null, true);

                List<string> vertexGraphSON = new List<string>();

                foreach (VertexField vertexField in vertices)
                {
                    JObject vertex = new JObject
                    {
                        new JProperty("type", "vertex"),
                        new JProperty("id", vertexField.VertexMetaProperties[KW_DOC_ID].PropertyValue)
                    };

                    Debug.Assert(vertexField.VertexMetaProperties.ContainsKey(KW_VERTEX_LABEL));
                    if (vertexField.VertexMetaProperties[KW_VERTEX_LABEL] != null)
                    {
                        vertex.Add(new JProperty("label", vertexField.VertexMetaProperties[KW_VERTEX_LABEL].PropertyValue));
                    }

                    // Add in Edges
                    JObject inE = new JObject();
                    if (vertexField.RevAdjacencyList != null && vertexField.RevAdjacencyList.AllEdges.Any())
                    {
                        var groupByLabel = vertexField.RevAdjacencyList.AllEdges.GroupBy(e => e.Label);
                        foreach (var g in groupByLabel)
                        {
                            string edgelLabel = g.Key;
                            JArray group = new JArray();

                            foreach (EdgeField edgeField in g)
                            {
                                string edgeId = edgeField.EdgeProperties[KW_EDGE_ID].ToValue;
                                if (!this.subgraphState.edgeIds.Contains(edgeId))
                                {
                                    continue;
                                }
                                JObject edge = new JObject
                                {
                                    new JProperty("id", edgeField.EdgeProperties[KW_EDGE_ID].ToValue),
                                    new JProperty("outV", edgeField.OutV)
                                };

                                // Add edge properties
                                JObject properties = new JObject();
                                foreach (string propertyName in edgeField.EdgeProperties.Keys)
                                {
                                    switch (propertyName)
                                    {
                                        case KW_EDGE_ID:
                                        case KW_EDGE_LABEL:
                                        case KW_EDGE_SRCV:
                                        case KW_EDGE_SINKV:
                                        case KW_EDGE_SRCV_LABEL:
                                        case KW_EDGE_SINKV_LABEL:
                                        case KW_EDGE_SRCV_PARTITION:
                                        case KW_EDGE_SINKV_PARTITION:
                                            continue;
                                        default:
                                            break;
                                    }

                                    properties.Add(new JProperty(propertyName,
                                        JsonDataTypeHelper.GetStringFieldData(edgeField.EdgeProperties[propertyName].PropertyValue,
                                            edgeField.EdgeProperties[propertyName].JsonDataType)));
                                }
                                edge.Add(new JProperty("properties", properties));
                                group.Add(edge);
                            }

                            if (group.Count != 0)
                            {
                                inE.Add(edgelLabel, group);
                            }
                        }
                    }
                    if (inE.Count != 0)
                    {
                        vertex.Add(new JProperty("inE", inE));
                    }



                    // Add out Edges
                    JObject outE = new JObject();
                    if (vertexField.AdjacencyList != null && vertexField.AdjacencyList.AllEdges.Any())
                    {

                        var groupByLabel = vertexField.AdjacencyList.AllEdges.GroupBy(e => e.Label);
                        foreach (var g in groupByLabel)
                        {
                            string edgelLabel = g.Key;
                            JArray group = new JArray();

                            foreach (EdgeField edgeField in g)
                            {
                                string edgeId = edgeField.EdgeProperties[KW_EDGE_ID].ToValue;
                                if (!this.subgraphState.edgeIds.Contains(edgeId))
                                {
                                    continue;
                                }
                                JObject edge = new JObject
                                {
                                    new JProperty("id", edgeField.EdgeProperties[KW_EDGE_ID].ToValue),
                                    new JProperty("inV", edgeField.InV)
                                };

                                // Add edge properties
                                JObject properties = new JObject();
                                foreach (string propertyName in edgeField.EdgeProperties.Keys)
                                {
                                    switch (propertyName)
                                    {
                                        case KW_EDGE_ID:
                                        case KW_EDGE_LABEL:
                                        //case KW_EDGE_OFFSET:
                                        case KW_EDGE_SRCV:
                                        case KW_EDGE_SINKV:
                                        case KW_EDGE_SRCV_LABEL:
                                        case KW_EDGE_SINKV_LABEL:
                                        case KW_EDGE_SRCV_PARTITION:
                                        case KW_EDGE_SINKV_PARTITION:
                                            continue;
                                        default:
                                            break;
                                    }

                                    properties.Add(new JProperty(propertyName,
                                        JsonDataTypeHelper.GetStringFieldData(edgeField.EdgeProperties[propertyName].PropertyValue,
                                            edgeField.EdgeProperties[propertyName].JsonDataType)));
                                }
                                edge.Add(new JProperty("properties", properties));
                                group.Add(edge);
                            }

                            if (group.Count != 0)
                            {
                                outE.Add(edgelLabel, group);
                            }
                        }
                    }
                    if (outE.Count != 0)
                    {
                        vertex.Add(new JProperty("outE", outE));
                    }


                    // Add vertex properties
                    JObject vertexProperties = new JObject();
                    foreach (KeyValuePair<string, VertexPropertyField> kvp in vertexField.VertexProperties)
                    {
                        string propertyName = kvp.Key;

                        Debug.Assert(!VertexField.IsVertexMetaProperty(propertyName), "Bug!");
                        Debug.Assert(!(propertyName == KW_VERTEX_EDGE || propertyName == KW_VERTEX_REV_EDGE), "Bug!");

                        JArray propertyArray = new JArray();
                        foreach (VertexSinglePropertyField vsp in kvp.Value.Multiples.Values)
                        {
                            JObject property = new JObject
                            {
                                new JProperty("id", vsp.PropertyId),
                                new JProperty("value",
                                    JsonDataTypeHelper.GetStringFieldData(vsp.PropertyValue, vsp.JsonDataType))
                            };

                            if (vsp.MetaProperties.Count > 0)
                            {
                                JObject metaProperties = new JObject();

                                foreach (KeyValuePair<string, ValuePropertyField> metaKvp in vsp.MetaProperties)
                                {
                                    string key = metaKvp.Key;
                                    ValuePropertyField value = metaKvp.Value;

                                    metaProperties.Add(new JProperty(key, JsonDataTypeHelper.GetStringFieldData(value.PropertyValue, value.JsonDataType)));
                                }
                                property.Add(new JProperty("properties", metaProperties));
                            }

                            propertyArray.Add(property);
                        }
                        vertexProperties.Add(new JProperty(propertyName, propertyArray));

                    }

                    vertex.Add(new JProperty("properties", vertexProperties));

                    vertexGraphSON.Add(vertex.ToString(Formatting.None));
                }

                this.subgraphState.graph = new StringField("[" + string.Join(", ", vertexGraphSON) + "]");
            }
            else
            {
                this.subgraphState.graph = new StringField("[]");
            }

            return this.subgraphState.graph;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.subgraphState = new SubgraphState(SerializationData.Command, "");
        }
    }

    internal class SubgraphState : AggregateState
    {
        internal HashSet<string> edgeIds;
        internal HashSet<string> vertexIds;
        internal GraphViewCommand command;
        internal FieldObject graph;

        public SubgraphState(GraphViewCommand command, string tableAlias) : base(tableAlias)
        {
            this.edgeIds = new HashSet<string>();
            this.vertexIds = new HashSet<string>();
            this.graph = null;
            this.command = command;
        }

        public override void Init()
        {
            this.edgeIds = new HashSet<string>();
            this.vertexIds = new HashSet<string>();
            this.graph = null;
        }
    }

    [DataContract]
    internal class CollectionFunction : IAggregateFunction
    {
        internal CollectionState collectionState;

        public CollectionFunction(CollectionState collectionState)
        {
            this.collectionState = collectionState;
        }

        public void Init()
        {
            this.collectionState.Init();
        }

        public void Accumulate(params FieldObject[] values)
        {
            this.collectionState.collectionField.Collection.Add(values[0]);
        }

        public FieldObject Terminate()
        {
            return this.collectionState.collectionField;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.collectionState = new CollectionState("");
        }
    }

    internal class CollectionState : AggregateState
    {
        internal CollectionField collectionField;

        public CollectionState(string tableAlias) : base(tableAlias)
        {
            this.collectionField = new CollectionField();
        }

        public override void Init()
        {
            this.collectionField = new CollectionField();
        }
    }

    [DataContract]
    internal class GroupFunction : IAggregateFunction
    {
        internal GroupState groupState;

        [DataMember]
        private GraphViewExecutionOperator aggregateOp;
        private Container container;
        [DataMember]
        private int containerIndex;

        [DataMember]
        bool isProjectingACollection;

        public GroupFunction(GroupState groupState, GraphViewExecutionOperator aggregateOp, Container container, int containerIndex, bool isProjectingACollection)
        {
            this.groupState = groupState;
            this.aggregateOp = aggregateOp;
            this.container = container;
            this.containerIndex = containerIndex;
            this.isProjectingACollection = isProjectingACollection;
        }

        public void Init()
        {
            this.groupState.Init();
        }

        public void Accumulate(params FieldObject[] values)
        {
            throw new NotImplementedException();
        }

        public void Accumulate(params Object[] values)
        {
            FieldObject groupByKey = values[0] as FieldObject;
            RawRecord groupByValue = values[1] as RawRecord;

            if (!this.groupState.groupedStates.ContainsKey(groupByKey)) {
                this.groupState.groupedStates.Add(groupByKey, new List<RawRecord>());
            }

            this.groupState.groupedStates[groupByKey].Add(groupByValue);
        }

        public FieldObject Terminate()
        {
            MapField result = new MapField();

            if (this.isProjectingACollection)
            {
                foreach (FieldObject key in this.groupState.groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    this.container.ResetTableCache(this.groupState.groupedStates[key]);
                    this.aggregateOp.ResetState();

                    for (int i = 0; i < this.groupState.groupedStates[key].Count; i++)
                    {
                        RawRecord aggregateTraversalRecord = this.aggregateOp.Next();
                        FieldObject projectResult = aggregateTraversalRecord?.RetriveData(0);

                        if (projectResult == null)
                        {
                            throw new GraphViewException("The property does not exist for some of the elements having been grouped.");
                        }

                        projectFields.Add(projectResult);
                    }


                    Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                    compositeFieldObjects.Add(DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME, new CollectionField(projectFields));
                    result[key] = new CompositeField(compositeFieldObjects, DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in this.groupState.groupedStates)
                {
                    FieldObject key = pair.Key;
                    this.aggregateOp.ResetState();
                    this.container.ResetTableCache(pair.Value);

                    RawRecord aggregateTraversalRecord = null;
                    FieldObject aggregateResult = null;
                    while (this.aggregateOp.State() && (aggregateTraversalRecord = this.aggregateOp.Next()) != null)
                    {
                        aggregateResult = aggregateTraversalRecord.RetriveData(0) ?? aggregateResult;
                    }

                    if (aggregateResult == null) {
                        continue;
                    }

                    result[key] = aggregateResult;
                }
            }

            return result;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.groupState = new GroupState("");
            this.container = SerializationData.Containers[this.containerIndex];
        }
    }

    internal class GroupState : AggregateState
    {
        internal Dictionary<FieldObject, List<RawRecord>> groupedStates;

        public GroupState(string tableAlias) : base(tableAlias)
        {
            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
        }

        public override void Init()
        {
            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
        }
    }


    [DataContract]
    internal class GroupSideEffectOperator : GraphViewExecutionOperator
    {
        public GroupFunction groupFunction;
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private ScalarFunction groupByKeyFunction;
        [DataMember]
        private readonly string sideEffectKey;

        public GroupSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            GroupFunction groupFunction,
            string sideEffectKey,
            ScalarFunction groupByKeyFunction)
        {
            this.inputOp = inputOp;
            this.groupFunction = groupFunction;
            this.groupByKeyFunction = groupByKeyFunction;
            this.sideEffectKey = sideEffectKey;
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

                this.groupFunction.Accumulate(new Object[]{ groupByKey, r });
                return r;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.groupFunction = (GroupFunction)SerializationData.SideEffectStates[this.sideEffectKey];
        }
    }

    [DataContract]
    internal class TreeSideEffectOperator : GraphViewExecutionOperator
    {
        public TreeFunction treeFunction;
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int pathIndex;
        [DataMember]
        private readonly string sideEffectKey;

        public TreeSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            TreeFunction treeFunction,
            string sideEffectKey,
            int pathIndex)
        {
            this.inputOp = inputOp;
            this.treeFunction = treeFunction;
            this.pathIndex = pathIndex;
            this.sideEffectKey = sideEffectKey;
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

                this.treeFunction.Accumulate(path);

                if (!this.inputOp.State()) {
                    this.Close();
                }
                return r;
            }

            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }

        [OnDeserialized]
        private void ReconstructOperator(StreamingContext context)
        {
            this.treeFunction = (TreeFunction)SerializationData.SideEffectStates[this.sideEffectKey];
        }
    }

    [DataContract]
    [KnownType(typeof(GroupInBatchOperator))]
    internal class GroupOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected GraphViewExecutionOperator inputOp;
        [DataMember]
        protected ScalarFunction groupByKeyFunction;
        [DataMember]
        protected GraphViewExecutionOperator aggregateOp;
        protected Container container;
        [DataMember]
        protected int containerIndex;

        [DataMember]
        protected bool isProjectingACollection;
        [DataMember]
        protected int carryOnCount;

        protected Dictionary<FieldObject, List<RawRecord>> groupedStates;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            Container container,
            int containerIndex,
            GraphViewExecutionOperator aggregateOp,
            bool isProjectingACollection,
            int carryOnCount)
        {
            this.inputOp = inputOp;

            this.groupByKeyFunction = groupByKeyFunction;

            this.container = container;
            this.containerIndex = containerIndex;
            this.aggregateOp = aggregateOp;

            this.isProjectingACollection = isProjectingACollection;
            this.carryOnCount = carryOnCount;

            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            this.Open();
        }

        public override RawRecord Next()
        {
            if (!this.State())
            {
                return null;
            }

            RawRecord r = null;
            while (this.inputOp.State() && (r = this.inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);

                if (groupByKey == null) {
                    throw new GraphViewException("The provided property name or traversal does not map to a value for some elements.");
                }

                if (!this.groupedStates.ContainsKey(groupByKey))
                {
                    this.groupedStates.Add(groupByKey, new List<RawRecord>());
                }
                this.groupedStates[groupByKey].Add(r);
            }

            MapField result = new MapField(this.groupedStates.Count);

            if (this.isProjectingACollection)
            {
                foreach (FieldObject key in groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    this.container.ResetTableCache(groupedStates[key]);
                    this.aggregateOp.ResetState();

                    for (int i = 0; i < groupedStates[key].Count; i++)
                    {
                        RawRecord aggregateTraversalRecord = this.aggregateOp.Next();
                        FieldObject projectResult = aggregateTraversalRecord?.RetriveData(0);

                        if (projectResult == null)
                        {
                            throw new GraphViewException("The property does not exist for some of the elements having been grouped.");
                        }

                        projectFields.Add(projectResult);
                    }

                    Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                    compositeFieldObjects.Add(DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME, new CollectionField(projectFields));
                    result[key] = new CompositeField(compositeFieldObjects, DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in this.groupedStates)
                {
                    FieldObject key = pair.Key;
                    this.aggregateOp.ResetState();
                    this.container.ResetTableCache(pair.Value);

                    RawRecord aggregateTraversalRecord = null;
                    FieldObject aggregateResult = null;
                    while (this.aggregateOp.State() && (aggregateTraversalRecord = this.aggregateOp.Next()) != null)
                    {
                        aggregateResult = aggregateTraversalRecord.RetriveData(0) ?? aggregateResult;
                    }

                    if (aggregateResult == null)
                    {
                        continue;
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
            this.container.Clear();
            this.aggregateOp.ResetState();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.groupedStates = new Dictionary<FieldObject, List<RawRecord>>();
            this.container = SerializationData.Containers[this.containerIndex];
        }
    }

    [DataContract]
    internal class GroupInBatchOperator : GroupOperator
    {
        private RawRecord firstRecordInGroup;
        private int processedGroupIndex;

        public GroupInBatchOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            Container container,
            int containerIndex,
            GraphViewExecutionOperator aggregateOp,
            bool isProjectingACollection,
            int carryOnCount)
            : base(inputOp, groupByKeyFunction, container, containerIndex, aggregateOp,
                isProjectingACollection, carryOnCount)
        {
            this.processedGroupIndex = 0;
        }

        public override RawRecord Next()
        {
            if (this.firstRecordInGroup == null && this.State())
            {
                this.firstRecordInGroup = this.inputOp.Next();
            }

            if (this.firstRecordInGroup == null)
            {
                this.Close();
                return null;
            }

            while (this.processedGroupIndex < int.Parse(this.firstRecordInGroup[0].ToValue))
            {
                MapField emptyMap = new MapField(this.groupedStates.Count);
                RawRecord noGroupRecord = new RawRecord();

                for (int i = 0; i < this.carryOnCount; i++)
                {
                    noGroupRecord.Append((FieldObject)null);
                }
                noGroupRecord.Append(emptyMap);
                noGroupRecord.fieldValues[0] = new StringField(this.processedGroupIndex.ToString(), JsonDataType.Int);
                this.processedGroupIndex++;

                return noGroupRecord;
            }

            FieldObject groupByKey = this.groupByKeyFunction.Evaluate(this.firstRecordInGroup);

            if (groupByKey == null)
            {
                throw new GraphViewException("The provided property name or traversal does not map to a value for some elements.");
            }

            if (!this.groupedStates.ContainsKey(groupByKey))
            {
                this.groupedStates.Add(groupByKey, new List<RawRecord>());
            }
            this.groupedStates[groupByKey].Add(this.firstRecordInGroup);

            RawRecord rec = null;
            while (this.inputOp.State() && 
                (rec = this.inputOp.Next()) != null && 
                rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
            {
                groupByKey = groupByKeyFunction.Evaluate(rec);

                if (groupByKey == null)
                {
                    throw new GraphViewException("The provided property name or traversal does not map to a value for some elements.");
                }

                if (!this.groupedStates.ContainsKey(groupByKey))
                {
                    this.groupedStates.Add(groupByKey, new List<RawRecord>());
                }
                this.groupedStates[groupByKey].Add(rec);
            }

            MapField result = new MapField(this.groupedStates.Count);

            if (this.isProjectingACollection)
            {
                foreach (FieldObject key in groupedStates.Keys)
                {
                    List<FieldObject> projectFields = new List<FieldObject>();
                    this.container.ResetTableCache(groupedStates[key]);
                    this.aggregateOp.ResetState();

                    for (int i = 0; i < groupedStates[key].Count; i++)
                    {
                        RawRecord aggregateTraversalRecord = this.aggregateOp.Next();
                        FieldObject projectResult = aggregateTraversalRecord?.RetriveData(0);

                        if (projectResult == null)
                        {
                            throw new GraphViewException("The property does not exist for some of the elements having been grouped.");
                        }

                        projectFields.Add(projectResult);
                    }

                    Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                    compositeFieldObjects.Add(DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME, new CollectionField(projectFields));
                    result[key] = new CompositeField(compositeFieldObjects, DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME);
                }
            }
            else
            {
                foreach (KeyValuePair<FieldObject, List<RawRecord>> pair in this.groupedStates)
                {
                    FieldObject key = pair.Key;
                    this.aggregateOp.ResetState();
                    this.container.ResetTableCache(pair.Value);

                    RawRecord aggregateTraversalRecord = null;
                    FieldObject aggregateResult = null;
                    while (this.aggregateOp.State() && (aggregateTraversalRecord = this.aggregateOp.Next()) != null)
                    {
                        aggregateResult = aggregateTraversalRecord.RetriveData(0) ?? aggregateResult;
                    }

                    if (aggregateResult == null)
                    {
                        continue;
                    }

                    result[key] = aggregateResult;
                }
            }

            RawRecord resultRecord = new RawRecord();

            for (int i = 0; i < this.carryOnCount; i++)
            {
                resultRecord.Append((FieldObject)null);
            }
            resultRecord.Append(result);
            resultRecord.fieldValues[0] = this.firstRecordInGroup[0];
            
            this.firstRecordInGroup = rec;
            this.groupedStates.Clear();
            this.processedGroupIndex++;

            return resultRecord;
        }

        public override void ResetState()
        {
            this.processedGroupIndex = 0;
            base.ResetState();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.processedGroupIndex = 0;
        }
    }



}
