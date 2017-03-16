using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using static GraphView.GraphViewKeywords;

namespace GraphView
{
    internal abstract class TableValuedFunction : GraphViewExecutionOperator
    {
        protected GraphViewExecutionOperator inputOperator;
        protected Queue<RawRecord> outputBuffer;

        protected RawRecord currentRecord = null;

        internal TableValuedFunction(GraphViewExecutionOperator pInputOperator)
        {
            inputOperator = pInputOperator;
            outputBuffer = new Queue<RawRecord>();
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
                    outputBuffer.Enqueue(rec);

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
    }

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
                if (propertyObject == null) {
                    continue;
                }

                VertexPropertyField vp = propertyObject as VertexPropertyField;
                if (vp != null)
                {
                    foreach (VertexSinglePropertyField vsp in vp.Multiples.Values)
                    {
                        RawRecord r = new RawRecord();
                        r.Append(new VertexSinglePropertyField(vsp));
                        foreach (string metapropertyName in this.populateMetaproperties) {
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
                    foreach (string metapropertyName in this.populateMetaproperties) {
                        r.Append(singleVp[metapropertyName]);
                    }
                    results.Add(r);
                    continue;
                }

                EdgePropertyField edgePf = propertyObject as EdgePropertyField;
                if (edgePf != null)
                {
                    if (this.populateMetaproperties.Count > 0) {
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
                    if (this.populateMetaproperties.Count > 0) {
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
    }

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
                if (propertyObject == null) {
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

                Debug.Assert(false, "Should not get here.");
            }

            return results;
        }
    }

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
                    Debug.Assert(!propertyName.Equals("_edge"));
                    Debug.Assert(!propertyName.Equals("_reverse_edge"));

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
                                foreach (string metaPropertyName in this.populateMetaProperties) {
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
                        case KW_EDGE_OFFSET:
                        case KW_EDGE_SRCV:
                        case KW_EDGE_SINKV:
                        case KW_EDGE_SRCV_LABEL:
                        case KW_EDGE_SINKV_LABEL:
                            continue;
                        default:
                            RawRecord r = new RawRecord();
                            r.Append(new EdgePropertyField(edgePropertyField));
                            results.Add(r);
                            break;
                    }
                }

                if (this.populateMetaProperties.Count > 0 && results.Count > 0) {
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

                if (this.populateMetaProperties.Count > 0 && results.Count > 0) {
                    throw new GraphViewException("An edge property cannot contain meta properties.");
                }
            }
            else {
                throw new GraphViewException("The input of properties() cannot be a meta or edge property.");
            }
            return results;
        }
    }

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
                        if (property == null) {
                            continue;
                        }

                        List<FieldObject> values = new List<FieldObject>();
                        VertexPropertyField vp = property as VertexPropertyField;
                        if (vp != null) {
                            foreach (VertexSinglePropertyField vsp in vp.Multiples.Values) {
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
                        Debug.Assert(!propertyName.Equals("_edge"));
                        Debug.Assert(!propertyName.Equals("_reverse_edge"));

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
                                foreach (VertexSinglePropertyField singleVp in property.Multiples.Values) {
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
                        if (property == null) {
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
                            case GraphViewKeywords.KW_EDGE_ID:
                            case KW_EDGE_OFFSET:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
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
                        if (property == null) {
                            continue;
                        }

                        ValuePropertyField metaPf = property as ValuePropertyField;
                        if (metaPf != null)
                        {
                            valueMap.Add(new StringField(propertyName),
                                new StringField(metaPf.PropertyValue, metaPf.JsonDataType));
                        }
                    }
                } else {
                    foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties) {
                        valueMap.Add(new StringField(kvp.Key), new StringField(kvp.Value.PropertyValue, kvp.Value.JsonDataType));
                    }
                }
            }
            else {
                throw new GraphViewException("The input of valueMap() cannot be a meta or edge property.");
            }

            RawRecord result = new RawRecord();
            result.Append(valueMap);
            return new List<RawRecord> { result };
        }
    }

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
                        if (property == null) {
                            continue;
                        }

                        List<FieldObject> values = new List<FieldObject>();
                        VertexPropertyField vp = property as VertexPropertyField;
                        if (vp != null) {
                            foreach (VertexSinglePropertyField vsp in vp.Multiples.Values) {
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
                        Debug.Assert(!propertyName.Equals("_edge"));
                        Debug.Assert(!propertyName.Equals("_reverse_edge"));

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
                                foreach (VertexSinglePropertyField singleVp in property.Multiples.Values) {
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
                        if (property == null) {
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
                            case GraphViewKeywords.KW_EDGE_ID:
                            case KW_EDGE_OFFSET:
                            case KW_EDGE_SRCV:
                            case KW_EDGE_SINKV:
                            case KW_EDGE_SRCV_LABEL:
                            case KW_EDGE_SINKV_LABEL:
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
                        if (property == null) {
                            continue;
                        }

                        ValuePropertyField metaPf = property as ValuePropertyField;
                        if (metaPf != null) {
                            valueMap.Add(new StringField(propertyName), metaPf);
                        }
                    }
                } else {
                    foreach (KeyValuePair<string, ValuePropertyField> kvp in singleVp.MetaProperties) {
                        valueMap.Add(new StringField(kvp.Key), kvp.Value);
                    }
                }
            }
            else {
                throw new GraphViewException("The input of valueMap() cannot be a meta or edge property.");
            }

            RawRecord result = new RawRecord();
            result.Append(valueMap);
            return new List<RawRecord> { result };
        }
    }

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
                    Debug.Assert(!propertyName.Equals("_edge"));
                    Debug.Assert(!propertyName.Equals("_reverse_edge"));

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
                        case KW_EDGE_OFFSET:
                        case KW_EDGE_SRCV:
                        case KW_EDGE_SINKV:
                        case KW_EDGE_SRCV_LABEL:
                        case KW_EDGE_SINKV_LABEL:
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
            else {
                throw new GraphViewException("The input of values() cannot be a meta or edge property.");
            }
            return results;
        }
    }

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

            if (this.constantValues.Count == 0 && !this.isList) {
                return new List<RawRecord>();
            }

            if (isList)
            {
                List<FieldObject> collection = new List<FieldObject>();
                foreach (ScalarFunction constantValueFunc in this.constantValues)
                {
                    Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                    compositeFieldObjects.Add(defaultProjectionKey, constantValueFunc.Evaluate(null));
                    collection.Add(new Compose1Field(compositeFieldObjects, defaultProjectionKey));
                }
                result.Append(new CollectionField(collection));
            }
            else
            {
                Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                compositeFieldObjects.Add(defaultProjectionKey, this.constantValues[0].Evaluate(null));
                result.Append(new Compose1Field(compositeFieldObjects, defaultProjectionKey));
            }

            return new List<RawRecord> { result };
        }
    }

    internal class PathOperator : TableValuedFunction
    {
        // <field index, whether this field is a path list needed to be unfolded>
        private List<Tuple<int, bool>> _pathFieldList;

        public PathOperator(GraphViewExecutionOperator pInputOperator,
            List<Tuple<int, bool>> pStepFieldList) : base(pInputOperator)
        {
            this._pathFieldList = pStepFieldList;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
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

    internal class PathOperator2 : GraphViewExecutionOperator
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

            if (this.byFuncList.Count == 0) {
                stepProjectionResult = step;
            }
            else
            {
                RawRecord initCompose1Record = new RawRecord();
                initCompose1Record.Append(step);
                stepProjectionResult = this.byFuncList[activeByFuncIndex++ % this.byFuncList.Count].Evaluate(initCompose1Record);

                if (stepProjectionResult == null) {
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

                        foreach (FieldObject subPathStep in subPath.Collection) {
                            path.Add(GetStepProjectionResult(subPathStep, ref activeByFuncIndex));
                        }
                    }
                    else {
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
    }

    internal class UnfoldOperator : TableValuedFunction
    {
        private ScalarFunction getUnfoldTargetFunc;
        private List<string> unfoldCompose1Columns;
        private readonly string tableDefaultColumnName;

        internal UnfoldOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction getUnfoldTargetFunc,
            List<string> unfoldCompose1Columns,
            string tableDefaultColumnName)
            : base(inputOp)
        {
            this.getUnfoldTargetFunc = getUnfoldTargetFunc;
            this.unfoldCompose1Columns = unfoldCompose1Columns;
            this.tableDefaultColumnName = tableDefaultColumnName;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            FieldObject unfoldTarget = getUnfoldTargetFunc.Evaluate(record);

            if (unfoldTarget is CollectionField)
            {
                CollectionField cf = unfoldTarget as CollectionField;
                foreach (FieldObject singleObj in cf.Collection)
                {
                    if (singleObj == null) continue;
                    RawRecord newRecord = new RawRecord();

                    // Extract only needed columns from Compose1Field
                    if (singleObj is Compose1Field)
                    {
                        Compose1Field compose1Field = singleObj as Compose1Field;
                        foreach (string unfoldColumn in unfoldCompose1Columns) {
                            newRecord.Append(compose1Field.CompositeFieldObject[unfoldColumn]);
                        }
                    }
                    else
                    {
                        foreach (string columnName in this.unfoldCompose1Columns)
                        {
                            if (columnName.Equals(this.tableDefaultColumnName)) {
                                newRecord.Append(singleObj);
                            }
                            else {
                                newRecord.Append((FieldObject)null);
                            }     
                        } 
                    }

                    results.Add(newRecord);
                }
            }
            else if (unfoldTarget is MapField)
            {
                MapField mf = unfoldTarget as MapField;
                foreach (EntryField entry in mf)
                {
                    RawRecord newRecord = new RawRecord();
                    string key = entry.Key.ToString();
                    string value = entry.Value.ToString();

                    foreach (string columnName in this.unfoldCompose1Columns)
                    {
                        if (columnName.Equals(GremlinKeyword.TableDefaultColumnName)) {
                            newRecord.Append(new StringField(key + "=" + value));
                        }
                        else {
                            newRecord.Append((FieldObject)null);
                        }
                    }

                    results.Add(newRecord);
                }
            }
            else
            {
                RawRecord newRecord = new RawRecord();
                foreach (string columnName in this.unfoldCompose1Columns)
                {
                    if (columnName.Equals(GremlinKeyword.TableDefaultColumnName)) {
                        newRecord.Append(unfoldTarget);
                    }
                    else {
                        newRecord.Append((FieldObject)null);
                    }
                }
                results.Add(newRecord);
            }

            return results;
        }
    }
}
