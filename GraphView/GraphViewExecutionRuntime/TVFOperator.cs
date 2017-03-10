using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        List<Tuple<string, int>> propertyList;
        int allPropertyIndex;

        public PropertiesOperator(
            GraphViewExecutionOperator pInputOperator, 
            List<Tuple<string, int>> pPropertiesList,
            int pAllPropertyIndex) : base(pInputOperator)
        {
            propertyList = pPropertiesList;
            allPropertyIndex = pAllPropertyIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();

            // Extract all properties if allPropertyIndex >= 0
            if (allPropertyIndex >= 0 && record[allPropertyIndex] != null) {
                VertexField vertexField = record[allPropertyIndex] as VertexField;
                if (vertexField != null) {
                    foreach (PropertyField property in vertexField.AllProperties) {
                        string propertyName = property.PropertyName;

                        switch (propertyName) {
                        // Reversed properties for meta-data
                        case "_edge":
                        case "_partition":
                        case "_reverse_edge":
                        case "_nextEdgeOffset":

                        case "_rid":
                        case "_self":
                        case "_etag":
                        case "_attachments":
                        case "_ts":
                            continue;
                        default:
                            RawRecord r = new RawRecord();
                            if (property is VertexSinglePropertyField || property is ValuePropertyField) {
                                r.Append(property);
                            }
                            else if (property is VertexPropertyField) {
                                foreach (VertexSinglePropertyField p in ((VertexPropertyField)property).Multiples.Values) {
                                    r.Append(p);
                                }
                            }
                            else {
                                Debug.Assert(false, $"[PropertiesOperator.CrossApply] property type error: {property.GetType()}");
                            }

                            results.Add(r);
                            break;
                        }
                    }
                }
                else {
                    EdgeField edgeField = record[allPropertyIndex] as EdgeField;
                    if (edgeField == null)
                        throw new GraphViewException(
                            string.Format("The FieldObject record[{0}] should be a VertexField or EdgeField but now it is {1}.",
                                          allPropertyIndex, record[allPropertyIndex].ToString()));

                    foreach (var propertyPair in edgeField.EdgeProperties) {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField propertyField = propertyPair.Value;

                        switch (propertyName) {
                        // Reversed properties for meta-data
                        case "_offset":
                        case "_srcV":
                        case "_sinkV":
                        case "_srcVLabel":
                        case "_sinkVLabel":
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
            else {
                // TODO: Now translation code needn't to generate the key name for the operator
                foreach (var pair in propertyList) {
                    //string propertyName = pair.Item1;
                    int propertyValueIndex = pair.Item2;
                    var propertyValue = record[propertyValueIndex];
                    if (propertyValue == null) {
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

    internal class PropertiesOperator2 : TableValuedFunction
    {
        List<int> propertiesIndex;
        List<string> populateMetaproperties; 

        public PropertiesOperator2(
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

    internal class ValuesOperator2 : TableValuedFunction
    {
        List<int> propertiesIndex;

        public ValuesOperator2(GraphViewExecutionOperator inputOp, List<int> propertiesIndex) : base(inputOp)
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
                    Debug.Assert(propertyName != "_edge");
                    Debug.Assert(propertyName != "_reverse_edge");

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
                        case "label":
                        case "_edgeId":
                        case "_offset":
                        case "_srcV":
                        case "_sinkV":
                        case "_srcVLabel":
                        case "_sinkVLabel":
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
                    Debug.Assert(propertyName == "_edge");
                    Debug.Assert(propertyName == "_reverse_edge");

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
                        case "_edgeId":
                        case "_offset":
                        case "_srcV":
                        case "_sinkV":
                        case "_srcVLabel":
                        case "_sinkVLabel":
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

    internal class ValuesOperator : TableValuedFunction
    {
        internal List<int> ValuesIdxList;
        int allValuesIndex;

        internal ValuesOperator(GraphViewExecutionOperator pInputOperator, List<int> pValuesIdxList, int pAllValuesIndex)
            : base(pInputOperator)
        {
            ValuesIdxList = pValuesIdxList;
            allValuesIndex = pAllValuesIndex;
        }

        internal override List<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();

            // Extract all values if allValuesIndex >= 0
            if (allValuesIndex >= 0 && record[allValuesIndex] != null)
            {
                VertexField vertexField = record[allValuesIndex] as VertexField;
                if (vertexField != null) {
                    foreach (PropertyField property in vertexField.AllProperties) {
                        string propertyName = property.PropertyName;

                        switch (propertyName) {
                        // Reversed properties for meta-data
                        case "_edge":
                        case "_reverse_edge":
                        case "_partition":
                        case "_nextEdgeOffset":

                        case "_rid":
                        case "_self":
                        case "_etag":
                        case "_attachments":
                        case "_ts":
                            continue;
                        default:
                            RawRecord r = new RawRecord();
                            r.Append(new StringField(property.PropertyValue, property.JsonDataType));
                            results.Add(r);
                            break;
                        }
                    }
                }
                else {
                    EdgeField edgeField = record[allValuesIndex] as EdgeField;
                    if (edgeField == null)
                        throw new GraphViewException(
                            string.Format("The FieldObject record[{0}] should be a VertexField or EdgeField but now it is {1}.",
                                          allValuesIndex, record[allValuesIndex].ToString()));

                    foreach (var propertyPair in edgeField.EdgeProperties) {
                        string propertyName = propertyPair.Key;
                        EdgePropertyField propertyField = propertyPair.Value;

                        switch (propertyName) {
                        // Reversed properties for meta-data
                        case "_offset":
                        case "_srcV":
                        case "_srcVLabel":
                        case "_sinkV":
                        case "_sinkVLabel":
                            continue;
                        default:
                            RawRecord r = new RawRecord();
                            r.Append(new StringField(propertyField.ToValue, propertyField.JsonDataType));
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
                    PropertyField propertyValue = record[propIdx] as PropertyField;
                    if (propertyValue == null)
                    {
                        continue;
                    }

                    var result = new RawRecord();
                    result.Append(new StringField(propertyValue.ToValue, propertyValue.JsonDataType));
                    results.Add(result);
                }
            }

            return results;
        }
    }

    internal class ConstantOperator : TableValuedFunction
    {
        private List<ScalarFunction> constantValues;
        private bool isList;

        internal ConstantOperator(
            GraphViewExecutionOperator inputOp,
            List<ScalarFunction> constantValues,
            bool isList)
            : base(inputOp)
        {
            this.constantValues = constantValues;
            this.isList = isList;
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
                foreach (ScalarFunction constantValueFunc in this.constantValues) {
                    collection.Add(constantValueFunc.Evaluate(null));
                }
                result.Append(new CollectionField(collection));
            }
            else {
                result.Append(this.constantValues[0].Evaluate(null));
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

        internal UnfoldOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction getUnfoldTargetFunc,
            List<string> unfoldCompose1Columns)
            : base(inputOp)
        {
            this.getUnfoldTargetFunc = getUnfoldTargetFunc;
            this.unfoldCompose1Columns = unfoldCompose1Columns;
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
                            if (columnName.Equals(GremlinKeyword.TableDefaultColumnName)) {
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
                foreach (KeyValuePair<FieldObject, FieldObject> pair in mf)
                {
                    RawRecord newRecord = new RawRecord();
                    string key = pair.Key.ToString();
                    string value = pair.Value.ToString();

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
