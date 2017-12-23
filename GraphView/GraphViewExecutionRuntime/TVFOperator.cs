using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [Serializable]
    internal abstract class TableValuedFunction : GraphViewExecutionOperator, ISerializable
    {
        protected GraphViewExecutionOperator inputOperator;

        protected Queue<RawRecord> outputBuffer;
        protected RawRecord currentRecord = null;

        internal TableValuedFunction(GraphViewExecutionOperator pInputOperator)
        {
            this.inputOperator = pInputOperator;
            this.outputBuffer = new Queue<RawRecord>();
            this.Open();
        }

        internal abstract List<RawRecord> CrossApply(RawRecord record);

        public override RawRecord Next()
        {
            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }

            while (inputOperator.State())
            {
                currentRecord = inputOperator.Next();
                if (currentRecord == null)
                {
                    Close();
                    return null;
                }

                List<RawRecord> results = CrossApply(currentRecord);

                foreach (RawRecord rec in results)
                {
                    outputBuffer.Enqueue(rec);
                }

                if (outputBuffer.Count > 0)
                {
                    RawRecord r = new RawRecord(currentRecord);
                    RawRecord toAppend = outputBuffer.Dequeue();
                    r.Append(toAppend);

                    return r;
                }
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            currentRecord = null;
            inputOperator.ResetState();
            outputBuffer.Clear();
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOperator.GetFirstOperator();
        }

        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("inputOp", this.inputOperator, typeof(GraphViewExecutionOperator));
        }

        protected TableValuedFunction(SerializationInfo info, StreamingContext context)
        {
            this.inputOperator = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.outputBuffer = new Queue<RawRecord>();
            this.Open();
        }
    }

    [Serializable]
    internal class PropertiesOperator : TableValuedFunction
    {
        List<int> propertiesIndex;
        List<string> populateMetaproperties; 

        public PropertiesOperator(
            GraphViewExecutionOperator inputOp,
            List<int> propertiesIndex,
            List<string> populateMetaproperties) : base(inputOp)
        {
            this.propertiesIndex = propertiesIndex;
            this.populateMetaproperties = populateMetaproperties;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            foreach (int propertyIndex in this.propertiesIndex)
            {
                FieldObject propertyObject = record[propertyIndex];
                if (propertyObject == null)
                {
                    continue;
                }

                VertexPropertyField vp = propertyObject as VertexPropertyField;
                if (vp != null)
                {
                    foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                    {
                        RawRecord r = new RawRecord();
                        r.Append(new VertexSinglePropertyField(vsp));
                        foreach (string metapropertyName in this.populateMetaproperties)
                        {
                            r.Append(vsp[metapropertyName]);
                        }

                        results.Add(r);
                    }
                    continue;
                }

                VertexSinglePropertyField singleVp = propertyObject as VertexSinglePropertyField;
                if (singleVp != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new VertexSinglePropertyField(singleVp));
                    foreach (string metapropertyName in this.populateMetaproperties)
                    {
                        r.Append(singleVp[metapropertyName]);
                    }
                    results.Add(r);
                    continue;
                }

                EdgePropertyField edgePf = propertyObject as EdgePropertyField;
                if (edgePf != null)
                {
                    if (this.populateMetaproperties.Count > 0)
                    {
                        throw new GraphViewException("An edge property cannot contain meta properties.");
                    }

                    RawRecord r = new RawRecord();
                    r.Append(new EdgePropertyField(edgePf));
                    results.Add(r);
                    continue;
                }

                ValuePropertyField metaPf = propertyObject as ValuePropertyField;
                if (metaPf != null)
                {
                    if (this.populateMetaproperties.Count > 0)
                    {
                        throw new GraphViewException("A meta property cannot contain meta properties.");
                    }

                    RawRecord r = new RawRecord();
                    r.Append(new ValuePropertyField(metaPf));
                    results.Add(r);
                    continue;
                }

                Debug.Assert(false, "Should not get here.");
            }

            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            GraphViewSerializer.SerializeList(info, "propertiesIndex", this.propertiesIndex);
            GraphViewSerializer.SerializeList(info, "populateMetaproperties", this.populateMetaproperties);
        }

        protected PropertiesOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.propertiesIndex = GraphViewSerializer.DeserializeList<int>(info, "propertiesIndex");
            this.populateMetaproperties = GraphViewSerializer.DeserializeList<string>(info, "populateMetaproperties");
        }
    }

    [Serializable]
    internal class ValuesOperator : TableValuedFunction
    {
        List<int> propertiesIndex;

        public ValuesOperator(GraphViewExecutionOperator inputOp, List<int> propertiesIndex) : base(inputOp)
        {
            this.propertiesIndex = propertiesIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            foreach (int propertyIndex in this.propertiesIndex)
            {
                FieldObject propertyObject = record[propertyIndex];
                if (propertyObject == null)
                {
                    continue;
                }

                VertexPropertyField vp = propertyObject as VertexPropertyField;
                if (vp != null)
                {
                    foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                    {
                        RawRecord r = new RawRecord();
                        r.Append(new StringField(vsp.PropertyValue, vsp.JsonDataType));
                        results.Add(r);
                    }
                    continue;
                }

                VertexSinglePropertyField singleVp = propertyObject as VertexSinglePropertyField;
                if (singleVp != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(singleVp.PropertyValue, singleVp.JsonDataType));
                    results.Add(r);
                    continue;
                }

                EdgePropertyField edgePf = propertyObject as EdgePropertyField;
                if (edgePf != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(edgePf.PropertyValue, edgePf.JsonDataType));
                    results.Add(r);
                    continue;
                }

                ValuePropertyField metaPf = propertyObject as ValuePropertyField;
                if (metaPf != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(metaPf.PropertyValue, metaPf.JsonDataType));
                    results.Add(r);
                    continue;
                }

                StringField strf = propertyObject as StringField;
                if (strf != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(strf.Value, strf.JsonDataType));
                    results.Add(r);
                    continue;
                }

                Debug.Assert(false, "Should not get here.");
            }

            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            GraphViewSerializer.SerializeList(info, "propertiesIndex", this.propertiesIndex);
        }

        protected ValuesOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.propertiesIndex = GraphViewSerializer.DeserializeList<int>(info, "propertiesIndex");
        }
    }

    [Serializable]
    internal class LabelOperator : TableValuedFunction
    {
        private readonly int targetIndex;

        public LabelOperator(GraphViewExecutionOperator inputOp, int targetIndex)
            : base(inputOp)
        {
            this.targetIndex = targetIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject target = record[this.targetIndex];
            if (target != null)
            {
                VertexField vertex = target as VertexField;
                if (vertex != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(vertex.VertexMetaProperties[KW_VERTEX_LABEL].ToValue));
                    results.Add(r);
                }

                VertexSinglePropertyField vertexSingleProperty = target as VertexSinglePropertyField;
                if (vertexSingleProperty != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(vertexSingleProperty.PropertyName));
                    results.Add(r);
                }

                EdgeField edge = target as EdgeField;
                if (edge != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(edge.Label));
                    results.Add(r);
                }

            }

            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("targetIndex", this.targetIndex);
        }

        protected LabelOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.targetIndex = info.GetInt32("targetIndex");
        }
    }

    [Serializable]
    internal class IdOperator : TableValuedFunction
    {
        private readonly int targetIndex;

        public IdOperator(GraphViewExecutionOperator inputOp, int targetIndex)
            : base(inputOp)
        {
            this.targetIndex = targetIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject target = record[this.targetIndex];
            if (target != null)
            {
                VertexField vertex = target as VertexField;
                if (vertex != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(vertex.VertexMetaProperties[KW_DOC_ID].ToValue));
                    results.Add(r);
                }

                VertexSinglePropertyField vertexSingleProperty = target as VertexSinglePropertyField;
                if (vertexSingleProperty != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(vertexSingleProperty.PropertyId));
                    results.Add(r);
                }

                EdgeField edge = target as EdgeField;
                if (edge != null)
                {
                    RawRecord r = new RawRecord();
                    r.Append(new StringField(edge.EdgeId));
                    results.Add(r);
                }

            }

            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("targetIndex", this.targetIndex);
        }

        protected IdOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.targetIndex = info.GetInt32("targetIndex");
        }
    }

    [Serializable]
    internal class AllPropertiesOperator : TableValuedFunction
    {
        private readonly int inputTargetIndex;
        private readonly List<string> populateMetaProperties;

        internal AllPropertiesOperator(
            GraphViewExecutionOperator inputOp,
            int inputTargetIndex,
            List<string> populateMetaProperties) : base(inputOp)
        {
            this.inputTargetIndex = inputTargetIndex;
            this.populateMetaProperties = populateMetaProperties;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject inputTarget = record[this.inputTargetIndex];

            if (inputTarget is VertexField)
            {
                VertexField vertexField = (VertexField)inputTarget;
                foreach (VertexPropertyField property in vertexField.VertexProperties.Values)
                {
                    string propertyName = property.PropertyName;
                    Debug.Assert(!VertexField.IsVertexMetaProperty(propertyName));
                    Debug.Assert(!propertyName.Equals(KW_VERTEX_EDGE));
                    Debug.Assert(!propertyName.Equals(KW_VERTEX_REV_EDGE));

                    switch (propertyName)
                    {
                        case "_rid":
                        case "_self":
                        case "_etag":
                        case "_attachments":
                        case "_ts":
                            continue;
                        default:
                            foreach (VertexSinglePropertyField singleVp in property.Multiples.Values)
                            {
                                RawRecord r = new RawRecord();
                                r.Append(new VertexSinglePropertyField(singleVp));
                                foreach (string metaPropertyName in this.populateMetaProperties)
                                {
                                    r.Append(singleVp[metaPropertyName]);
                                }
                                results.Add(r);
                            }
                            break;
                    }
                }
            }
            else if (inputTarget is EdgeField)
            {
                EdgeField edgeField = (EdgeField)inputTarget;
                foreach (KeyValuePair<string, EdgePropertyField> propertyPair in edgeField.EdgeProperties)
                {
                    string propertyName = propertyPair.Key;
                    EdgePropertyField edgePropertyField = propertyPair.Value;

                    switch (propertyName)
                    {
                        // Reserved properties for meta-data
                        case KW_EDGE_LABEL:
                        case KW_EDGE_ID:
                        case KW_EDGE_SRCV:
                        case KW_EDGE_SINKV:
                        case KW_EDGE_SRCV_LABEL:
                        case KW_EDGE_SINKV_LABEL:
                        case KW_EDGE_SRCV_PARTITION:
                        case KW_EDGE_SINKV_PARTITION:
                            continue;
                        default:
                            RawRecord r = new RawRecord();
                            r.Append(new EdgePropertyField(edgePropertyField));
                            results.Add(r);
                            break;
                    }
                }

                if (this.populateMetaProperties.Count > 0 && results.Count > 0)
                {
                    throw new GraphViewException("An edge property cannot contain meta properties.");
                }
            }
            else if (inputTarget is VertexSinglePropertyField)
            {
                VertexSinglePropertyField singleVp = (VertexSinglePropertyField)inputTarget;
                foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties)
                {
                    RawRecord r = new RawRecord();
                    ValuePropertyField metaPropertyField = kvp.Value;
                    r.Append(new ValuePropertyField(metaPropertyField));
                    results.Add(r);
                }

                if (this.populateMetaProperties.Count > 0 && results.Count > 0)
                {
                    throw new GraphViewException("An edge property cannot contain meta properties.");
                }
            }
            else
            {
                throw new GraphViewException("The input of properties() cannot be a meta or edge property.");
            }
            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("inputTargetIndex", this.inputTargetIndex);
            GraphViewSerializer.SerializeList(info, "populateMetaProperties", this.populateMetaProperties);
        }

        protected AllPropertiesOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.inputTargetIndex = info.GetInt32("inputTargetIndex");
            this.populateMetaProperties = GraphViewSerializer.DeserializeList<string>(info, "populateMetaProperties");
        }
    }

    [Serializable]
    internal class ValueMapOperator : TableValuedFunction
    {
        private readonly int inputTargetIndex;
        private readonly bool includingMetaValue;
        private readonly List<string> propertyNameList;

        internal ValueMapOperator(
            GraphViewExecutionOperator inputOp, 
            int inputTargetIndex, 
            bool includingMetaValue, 
            List<string> propertyNameList) 
            : base(inputOp)
        {
            this.inputTargetIndex = inputTargetIndex;
            this.includingMetaValue = includingMetaValue;
            this.propertyNameList = propertyNameList;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            MapField valueMap = new MapField();

            FieldObject inputTarget = record[this.inputTargetIndex];

            if (inputTarget is VertexField)
            {
                VertexField vertexField = (VertexField)inputTarget;
                if (this.includingMetaValue)
                {
                    valueMap.Add(new StringField(GremlinKeyword.NodeID), new StringField(vertexField[KW_DOC_ID].ToValue));
                    valueMap.Add(new StringField(GremlinKeyword.Label), new StringField(vertexField[KW_VERTEX_LABEL].ToValue));
                }

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = vertexField[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        List<FieldObject> values = new List<FieldObject>();
                        VertexPropertyField vp = property as VertexPropertyField;
                        if (vp != null)
                        {
                            foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                            {
                                values.Add(new StringField(vsp.PropertyValue, vsp.JsonDataType));
                            }
                        }

                        valueMap.Add(new StringField(propertyName), new CollectionField(values));
                    }
                }
                else
                {
                    foreach (VertexPropertyField property in vertexField.VertexProperties.Values)
                    {
                        string propertyName = property.PropertyName;
                        Debug.Assert(!VertexField.IsVertexMetaProperty(propertyName));
                        Debug.Assert(!propertyName.Equals(KW_VERTEX_EDGE));
                        Debug.Assert(!propertyName.Equals(KW_VERTEX_REV_EDGE));

                        switch (propertyName)
                        {
                            case "_rid":
                            case "_self":
                            case "_etag":
                            case "_attachments":
                            case "_ts":
                                continue;
                            default:
                                List<FieldObject> values = new List<FieldObject>();
                                foreach (VertexSinglePropertyField singleVp in property.Multiples.Values)
                                {
                                    values.Add(new StringField(singleVp.PropertyValue, singleVp.JsonDataType));
                                }
                                valueMap.Add(new StringField(propertyName), new CollectionField(values));
                                break;
                        }
                    }
                }
            }
            else if (inputTarget is EdgeField)
            {
                EdgeField edgeField = (EdgeField)inputTarget;

                if (this.includingMetaValue)
                {
                    valueMap.Add(new StringField(GremlinKeyword.EdgeID), new StringField(edgeField[KW_EDGE_ID].ToValue));
                    valueMap.Add(new StringField(GremlinKeyword.Label), new StringField(edgeField.Label));
                }

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = edgeField[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        EdgePropertyField edgePf = property as EdgePropertyField;
                        if (edgePf != null)
                        {
                            valueMap.Add(new StringField(propertyName),
                                new StringField(edgePf.PropertyValue, edgePf.JsonDataType));
                        }
                    }
                }
                else
                {
                    foreach (KeyValuePair<string, EdgePropertyField> propertyPair in edgeField.EdgeProperties)
                    {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField edgePropertyField = propertyPair.Value;

                        switch (propertyName)
                        {
                            // Reserved properties for meta-data
                            case KW_EDGE_ID:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
                            case KW_EDGE_SRCV_PARTITION:
                            case KW_EDGE_SINKV_PARTITION:
                                continue;
                            default:
                                valueMap.Add(new StringField(propertyName),
                                    new StringField(edgePropertyField.PropertyValue, edgePropertyField.JsonDataType));
                                break;
                        }
                    }
                }
            }
            else if (inputTarget is VertexSinglePropertyField)
            {
                VertexSinglePropertyField singleVp = inputTarget as VertexSinglePropertyField;

                if (this.includingMetaValue)
                {
                    valueMap.Add(new StringField("id"), new StringField(singleVp.PropertyId));
                    valueMap.Add(new StringField("key"), new StringField(singleVp.PropertyName));
                    valueMap.Add(new StringField("value"), new StringField(singleVp.PropertyValue));
                }

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = singleVp[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        ValuePropertyField metaPf = property as ValuePropertyField;
                        if (metaPf != null)
                        {
                            valueMap.Add(new StringField(propertyName),
                                new StringField(metaPf.PropertyValue, metaPf.JsonDataType));
                        }
                    }
                }
                else
                {
                    foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties)
                    {
                        valueMap.Add(new StringField(kvp.Key), new StringField(kvp.Value.PropertyValue, kvp.Value.JsonDataType));
                    }
                }
            }
            else
            {
                throw new GraphViewException("The input of valueMap() cannot be a meta or edge property.");
            }

            RawRecord result = new RawRecord();
            result.Append(valueMap);
            return new List<RawRecord> { result };
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("inputTargetIndex", this.inputTargetIndex);
            info.AddValue("includingMetaValue", this.includingMetaValue);
            GraphViewSerializer.SerializeList(info, "propertyNameList", this.propertyNameList);
        }

        protected ValueMapOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.inputTargetIndex = info.GetInt32("inputTargetIndex");
            this.includingMetaValue = info.GetBoolean("includingMetaValue");
            this.propertyNameList = GraphViewSerializer.DeserializeList<string>(info, "propertyNameList");
        }
    }

    [Serializable]
    internal class PropertyMapOperator : TableValuedFunction
    {
        private readonly int inputTargetIndex;
        private readonly List<string> propertyNameList;

        internal PropertyMapOperator(
            GraphViewExecutionOperator inputOp,
            int inputTargetIndex,
            List<string> propertyNameList)
            : base(inputOp)
        {
            this.inputTargetIndex = inputTargetIndex;
            this.propertyNameList = propertyNameList;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            MapField valueMap = new MapField();

            FieldObject inputTarget = record[this.inputTargetIndex];

            if (inputTarget is VertexField)
            {
                VertexField vertexField = (VertexField)inputTarget;

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = vertexField[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        List<FieldObject> values = new List<FieldObject>();
                        VertexPropertyField vp = property as VertexPropertyField;
                        if (vp != null)
                        {
                            foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                            {
                                values.Add(vsp);
                            }
                        }

                        valueMap.Add(new StringField(propertyName), new CollectionField(values));
                    }
                }
                else
                {
                    foreach (VertexPropertyField property in vertexField.VertexProperties.Values)
                    {
                        string propertyName = property.PropertyName;
                        Debug.Assert(!VertexField.IsVertexMetaProperty(propertyName));
                        Debug.Assert(!propertyName.Equals(KW_VERTEX_EDGE));
                        Debug.Assert(!propertyName.Equals(KW_VERTEX_REV_EDGE));

                        switch (propertyName)
                        {
                            case "_rid":
                            case "_self":
                            case "_etag":
                            case "_attachments":
                            case "_ts":
                                continue;
                            default:
                                List<FieldObject> values = new List<FieldObject>();
                                foreach (VertexSinglePropertyField singleVp in property.Multiples.Values)
                                {
                                    values.Add(singleVp);
                                }
                                valueMap.Add(new StringField(propertyName), new CollectionField(values));
                                break;
                        }
                    }
                }
            }
            else if (inputTarget is EdgeField)
            {
                EdgeField edgeField = (EdgeField)inputTarget;

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = edgeField[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        EdgePropertyField edgePf = property as EdgePropertyField;
                        if (edgePf != null)
                        {
                            valueMap.Add(new StringField(propertyName), edgePf);
                        }
                    }
                }
                else
                {
                    foreach (KeyValuePair<string, EdgePropertyField> propertyPair in edgeField.EdgeProperties)
                    {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField edgePropertyField = propertyPair.Value;

                        switch (propertyName)
                        {
                            // Reserved properties for meta-data
                            case KW_EDGE_ID:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
                            case KW_EDGE_SRCV_PARTITION:
                            case KW_EDGE_SINKV_PARTITION:
                                continue;
                            default:
                                valueMap.Add(new StringField(propertyName), edgePropertyField);
                                break;
                        }
                    }
                }
            }
            else if (inputTarget is VertexSinglePropertyField)
            {
                VertexSinglePropertyField singleVp = inputTarget as VertexSinglePropertyField;

                if (this.propertyNameList.Any())
                {
                    foreach (string propertyName in this.propertyNameList)
                    {
                        FieldObject property = singleVp[propertyName];
                        if (property == null)
                        {
                            continue;
                        }

                        ValuePropertyField metaPf = property as ValuePropertyField;
                        if (metaPf != null)
                        {
                            valueMap.Add(new StringField(propertyName), metaPf);
                        }
                    }
                }
                else
                {
                    foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties)
                    {
                        valueMap.Add(new StringField(kvp.Key), kvp.Value);
                    }
                }
            }
            else
            {
                throw new GraphViewException("The input of valueMap() cannot be a meta or edge property.");
            }

            RawRecord result = new RawRecord();
            result.Append(valueMap);
            return new List<RawRecord> { result };
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("inputTargetIndex", this.inputTargetIndex);
            GraphViewSerializer.SerializeList(info, "propertyNameList", this.propertyNameList);
        }

        protected PropertyMapOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.inputTargetIndex = info.GetInt32("inputTargetIndex");
            this.propertyNameList = GraphViewSerializer.DeserializeList<string>(info, "propertyNameList");
        }
    }

    [Serializable]
    internal class AllValuesOperator : TableValuedFunction
    {
        private readonly int inputTargetIndex;

        internal AllValuesOperator(GraphViewExecutionOperator inputOp, int inputTargetIndex) : base(inputOp)
        {
            this.inputTargetIndex = inputTargetIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject inputTarget = record[this.inputTargetIndex];

            if (inputTarget is VertexField)
            {
                VertexField vertexField = (VertexField)inputTarget;
                foreach (VertexPropertyField property in vertexField.VertexProperties.Values)
                {
                    string propertyName = property.PropertyName;
                    Debug.Assert(!VertexField.IsVertexMetaProperty(propertyName));
                    Debug.Assert(!propertyName.Equals(KW_VERTEX_EDGE));
                    Debug.Assert(!propertyName.Equals(KW_VERTEX_REV_EDGE));

                    switch (propertyName)
                    {
                        case "_rid":
                        case "_self":
                        case "_etag":
                        case "_attachments":
                        case "_ts":
                            continue;
                        default:
                            foreach (VertexSinglePropertyField singleVp in property.Multiples.Values)
                            {
                                RawRecord r = new RawRecord();
                                r.Append(new StringField(singleVp.PropertyValue, singleVp.JsonDataType));
                                results.Add(r);
                            }
                            break;
                    }
                }
            }
            else if (inputTarget is EdgeField)
            {
                EdgeField edgeField = (EdgeField)inputTarget;

                foreach (KeyValuePair<string, EdgePropertyField> propertyPair in edgeField.EdgeProperties)
                {
                    string propertyName = propertyPair.Key;
                    EdgePropertyField edgePropertyField = propertyPair.Value;

                    switch (propertyName)
                    {
                        // Reserved properties for meta-data
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
                            RawRecord r = new RawRecord();
                            r.Append(new StringField(edgePropertyField.PropertyValue, edgePropertyField.JsonDataType));
                            results.Add(r);
                            break;
                    }
                }
            }
            else if (inputTarget is VertexSinglePropertyField)
            {
                VertexSinglePropertyField singleVp = inputTarget as VertexSinglePropertyField;
                foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties)
                {
                    RawRecord r = new RawRecord();
                    ValuePropertyField metaPropertyField = kvp.Value;
                    r.Append(new StringField(metaPropertyField.PropertyValue, metaPropertyField.JsonDataType));
                    results.Add(r);
                }
            }
            else
            {
                throw new GraphViewException("The input of values() cannot be a meta or edge property.");
            }
            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("inputTargetIndex", this.inputTargetIndex);
        }

        protected AllValuesOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.inputTargetIndex = info.GetInt32("inputTargetIndex");
        }
    }

    [Serializable]
    internal class ConstantOperator : TableValuedFunction
    {
        private List<ScalarFunction> constantValues;
        private bool isList;
        private readonly string defaultProjectionKey;

        internal ConstantOperator(
            GraphViewExecutionOperator inputOp,
            List<ScalarFunction> constantValues,
            bool isList,
            string defaultProjectionKey)
            : base(inputOp)
        {
            this.constantValues = constantValues;
            this.isList = isList;
            this.defaultProjectionKey = defaultProjectionKey;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            RawRecord result = new RawRecord();

            if (this.constantValues.Count == 0 && !this.isList)
            {
                return new List<RawRecord>();
            }

            if (isList)
            {
                List<FieldObject> collection = new List<FieldObject>();
                foreach (ScalarFunction constantValueFunc in this.constantValues)
                {
                    Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                    compositeFieldObjects.Add(defaultProjectionKey, constantValueFunc.Evaluate(null));
                    collection.Add(new CompositeField(compositeFieldObjects, defaultProjectionKey));
                }
                result.Append(new CollectionField(collection));
            }
            else
            {
                result.Append(this.constantValues[0].Evaluate(null));
            }

            return new List<RawRecord> { result };
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            GraphViewSerializer.SerializeList(info, "constantValues", this.constantValues);
            info.AddValue("isList", this.isList);
            info.AddValue("defaultProjectionKey", this.defaultProjectionKey);
            
        }

        protected ConstantOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.constantValues = GraphViewSerializer.DeserializeList<ScalarFunction>(info, "constantValues");
            this.isList = info.GetBoolean("isList");
            this.defaultProjectionKey = info.GetString("defaultProjectionKey");
            
        }
    }

    [Serializable]
    internal class PathOperator : GraphViewExecutionOperator, ISerializable
    {
        private GraphViewExecutionOperator inputOp;
        // The scalar function in a path step is either a ComposeCompositeField function 
        // or a column reference. The former wrapps all fields of a single step into one column 
        // that are needed by the By functions. The latter refers to a sub-path produced by a 
        // Gremlin step (or TVF) that has one or more steps.  
        // The boolean value indicates a path step is a sub-path or not. 
        private List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList;
        private List<ScalarFunction> byFuncList;

        public PathOperator(GraphViewExecutionOperator inputOp,
            List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList,
            List<ScalarFunction> byFuncList)
        {
            this.inputOp = inputOp;
            this.pathStepList = pathStepList;
            this.byFuncList = byFuncList;

            this.Open();
        }

        private FieldObject GetStepProjectionResult(FieldObject step, ref int activeByFuncIndex)
        {
            FieldObject stepProjectionResult;

            if (this.byFuncList.Count == 0)
            {
                stepProjectionResult = step;
            }
            else
            {
                RawRecord initCompose1Record = new RawRecord();
                initCompose1Record.Append(step);
                stepProjectionResult = this.byFuncList[activeByFuncIndex++ % this.byFuncList.Count].Evaluate(initCompose1Record);

                if (stepProjectionResult == null)
                {
                    throw new GraphViewException("The provided traversal or property name of path() does not map to a value.");
                }
            }

            return stepProjectionResult;
        }

        public override RawRecord Next()
        {
            RawRecord inputRec;
            while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
            {
                List<FieldObject> path = new List<FieldObject>();
                int activeByFuncIndex = 0;

                foreach (Tuple<ScalarFunction, bool, HashSet<string>> tuple in pathStepList)
                {
                    ScalarFunction accessPathStepFunc = tuple.Item1;
                    bool needsUnfold = tuple.Item2;
                    HashSet<string> stepLabels = tuple.Item3;

                    // g.V().FlatMap(__.As('a'))
                    // For a path starting from a context variable (__), the first step is null. 
                    // The labels of the first step will be passed to whatever proceeds the context variable.
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

                    FieldObject step = accessPathStepFunc.Evaluate(inputRec);
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

                            FieldObject pathStep = this.GetStepProjectionResult(subPathStep.StepFieldObject, ref activeByFuncIndex);
                            PathStepField pathStepField = new PathStepField(pathStep);
                            foreach (string label in subPathStep.Labels)
                            {
                                pathStepField.AddLabel(label);
                            }
                            path.Add(pathStepField);
                        }

                        PathStepField lastSubPathStep = (PathStepField)path.Last();
                        foreach (string label in stepLabels)
                        {
                            lastSubPathStep.AddLabel(label);
                        }
                    }
                    else
                    {
                        CompositeField basicStep = step as CompositeField;
                        Debug.Assert(basicStep != null, "basicStep != null");

                        // g.V().optional(__.count().V()).path()
                        // When records in a pipeline go through an aggregation operator,
                        // this operator produces only record and resets all the fields populated 
                        // by prior steps to null. By path() semantics, all prior steps do not
                        // appear in the path expression either.   
                        if (basicStep[basicStep.DefaultProjectionKey] == null)
                        {
                            continue;
                        }

                        FieldObject pathStep = this.GetStepProjectionResult(step, ref activeByFuncIndex);

                        PathStepField pathStepField = new PathStepField(pathStep);
                        foreach (string label in stepLabels)
                        {
                            pathStepField.AddLabel(label);
                        }
                        path.Add(pathStepField);
                    }
                }

                RawRecord r = new RawRecord(inputRec);
                r.Append(new PathField(path));
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

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("inputOp", this.inputOp, typeof(GraphViewExecutionOperator));
            GraphViewSerializer.SerializeListTupleHashSet(info, "pathStepList", this.pathStepList);
            GraphViewSerializer.SerializeList(info, "byFuncList", this.byFuncList);
        }

        protected PathOperator(SerializationInfo info, StreamingContext context)
        {
            this.inputOp = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.pathStepList = GraphViewSerializer.DeserializeListTupleHashSet<ScalarFunction, bool, string>(info, "pathStepList");
            this.byFuncList = GraphViewSerializer.DeserializeList<ScalarFunction>(info, "byFuncList");
        }
    }

    [Serializable]
    internal class PathOperator2 : GraphViewExecutionOperator, ISerializable
    {
        private GraphViewExecutionOperator inputOp;
        //
        // If the boolean value is true, then it's a subPath to be unfolded
        //
        private List<Tuple<ScalarFunction, bool>> pathStepList;
        private List<ScalarFunction> byFuncList;

        public PathOperator2(GraphViewExecutionOperator inputOp,
            List<Tuple<ScalarFunction, bool>> pathStepList,
            List<ScalarFunction> byFuncList)
        {
            this.inputOp = inputOp;
            this.pathStepList = pathStepList;
            this.byFuncList = byFuncList;

            this.Open();
        }

        private FieldObject GetStepProjectionResult(FieldObject step, ref int activeByFuncIndex)
        {
            FieldObject stepProjectionResult;

            if (this.byFuncList.Count == 0)
            {
                stepProjectionResult = step;
            }
            else
            {
                RawRecord initCompose1Record = new RawRecord();
                initCompose1Record.Append(step);
                stepProjectionResult = this.byFuncList[activeByFuncIndex++ % this.byFuncList.Count].Evaluate(initCompose1Record);

                if (stepProjectionResult == null)
                {
                    throw new GraphViewException("The provided traversal or property name of path() does not map to a value.");
                }
            }

            return stepProjectionResult;
        }

        public override RawRecord Next()
        {
            RawRecord inputRec;
            while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
            {
                List<FieldObject> path = new List<FieldObject>();
                int activeByFuncIndex = 0;

                foreach (Tuple<ScalarFunction, bool> tuple in pathStepList)
                {
                    ScalarFunction accessPathStepFunc = tuple.Item1;
                    bool needsUnfold = tuple.Item2;

                    FieldObject step = accessPathStepFunc.Evaluate(inputRec);
                    if (step == null) continue;

                    if (needsUnfold)
                    {
                        CollectionField subPath = step as CollectionField;
                        Debug.Assert(subPath != null, "(subPath as CollectionField) != null");

                        foreach (FieldObject subPathStep in subPath.Collection)
                        {
                            path.Add(GetStepProjectionResult(subPathStep, ref activeByFuncIndex));
                        }
                    }
                    else
                    {
                        path.Add(GetStepProjectionResult(step, ref activeByFuncIndex));
                    }
                }

                RawRecord r = new RawRecord(inputRec);
                r.Append(new CollectionField(path));
                return r;
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("inputOp", this.inputOp, typeof(GraphViewExecutionOperator));
            GraphViewSerializer.SerializeListTuple(info, "pathStepList", this.pathStepList);
            GraphViewSerializer.SerializeList(info, "byFuncList", this.byFuncList);
        }

        protected PathOperator2(SerializationInfo info, StreamingContext context)
        {
            this.inputOp = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.pathStepList = GraphViewSerializer.DeserializeListTuple<ScalarFunction, bool>(info, "pathStepList");
            this.byFuncList = GraphViewSerializer.DeserializeList<ScalarFunction>(info, "byFuncList");
        }
    }

    [Serializable]
    internal class UnfoldOperator : TableValuedFunction
    {
        private ScalarFunction getUnfoldTargetFunc;
        private List<string> populateColumns;

        internal UnfoldOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction getUnfoldTargetFunc,
            List<string> populateColumns)
            : base(inputOp)
        {
            this.getUnfoldTargetFunc = getUnfoldTargetFunc;
            this.populateColumns = populateColumns;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject unfoldTarget = getUnfoldTargetFunc.Evaluate(record);

            if (unfoldTarget is PathField)
            {
                PathField path = (PathField)unfoldTarget;
                foreach (PathStepField pathStep in path.Path.Cast<PathStepField>())
                {
                    if (pathStep == null) continue;
                    RawRecord flatRecord = new RawRecord();

                    CompositeField compose1StepField = pathStep.StepFieldObject as CompositeField;
                    Debug.Assert(compose1StepField != null, "compose1StepField != null");
                    //
                    // Extract only needed columns from Compose1Field
                    //
                    foreach (string unfoldColumn in populateColumns)
                    {
                        flatRecord.Append(compose1StepField[unfoldColumn]);
                    }

                    results.Add(flatRecord);
                }
            }
            else if (unfoldTarget is CollectionField)
            {
                CollectionField inputCollection = (CollectionField)unfoldTarget;
                foreach (FieldObject singleObj in inputCollection.Collection)
                {
                    if (singleObj == null)
                    {
                        continue;
                    }
                    RawRecord flatRecord = new RawRecord();

                    CompositeField compose1ObjField = singleObj as CompositeField;
                    Debug.Assert(compose1ObjField != null, "compose1ObjField != null");
                    //
                    // Extract only needed columns from Compose1Field
                    //
                    foreach (string unfoldColumn in populateColumns)
                    {
                        flatRecord.Append(compose1ObjField[unfoldColumn]);
                    }

                    results.Add(flatRecord);
                }
            }
            else if (unfoldTarget is MapField)
            {
                MapField inputMap = (MapField)unfoldTarget;
                foreach (EntryField entry in inputMap)
                {
                    RawRecord entryRecord = new RawRecord();

                    foreach (string columnName in this.populateColumns)
                    {
                        entryRecord.Append(columnName.Equals(DocumentDBKeywords.KW_TABLE_DEFAULT_COLUMN_NAME)
                            ? entry
                            : (FieldObject) null);
                    }

                    results.Add(entryRecord);
                }
            }
            else
            {
                RawRecord flatRecord = unfoldTarget.FlatToRawRecord(this.populateColumns);
                results.Add(flatRecord);
            }

            return results;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("getUnfoldTargetFunc", this.getUnfoldTargetFunc);
            GraphViewSerializer.SerializeList(info, "populateColumns", this.populateColumns);
        }

        protected UnfoldOperator(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.getUnfoldTargetFunc = (ScalarFunction)info.GetValue("getUnfoldTargetFunc", typeof(ScalarFunction));
            this.populateColumns = GraphViewSerializer.DeserializeList<string>(info, "populateColumns");
        }
    }
}
