#define TEST_ON_DOCUMENT_DB
//#define TEST_ON_JSONSERVER

// !!! Important, change the same define in AbstractGremlinTest.cs at the same time.

using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.CSharp;

namespace GraphView
{
    public enum OutputFormat
    {
        Regular = 0,
        GraphSON
    }

    public class GraphSONProjector
    {
        internal static string ToGraphSON(List<RawRecord> results, GraphViewCommand command)
        {
            StringBuilder finalGraphSonResult = new StringBuilder("[");
            HashSet<string> batchIdSet = new HashSet<string>();
            HashSet<string> batchPartitionKeySet = new HashSet<string>();
            Dictionary<int, VertexField> batchGraphSonDict = new Dictionary<int, VertexField>();

            StringBuilder notBatchedGraphSonResult = new StringBuilder();
            bool firstEntry = true;
            EdgeType edgeType = 0;
            foreach (RawRecord record in results)
            {
                if (firstEntry) {
                    firstEntry = false;
                }
                else {
                    notBatchedGraphSonResult.Append(", ");
                }
                FieldObject field = record[0];

                VertexField vertexField = field as VertexField;
                if (vertexField != null &&
                    (!vertexField.AdjacencyList.HasBeenFetched || !vertexField.RevAdjacencyList.HasBeenFetched))
                {
                    if (!vertexField.AdjacencyList.HasBeenFetched) {
                        edgeType |= EdgeType.Outgoing;
                    }
                    if (!vertexField.RevAdjacencyList.HasBeenFetched) {
                        edgeType |= EdgeType.Incoming;
                    }

                    string vertexId = vertexField[DocumentDBKeywords.KW_DOC_ID].ToValue;
                    batchIdSet.Add(vertexId);
                    if (vertexField.Partition != null) {
                        batchPartitionKeySet.Add(vertexField.Partition);
                    }
                    batchGraphSonDict.Add(notBatchedGraphSonResult.Length, vertexField);
                    continue;
                }

                notBatchedGraphSonResult.Append(field.ToGraphSON());
            }

            if (batchIdSet.Any())
            {
                EdgeDocumentHelper.ConstructLazyAdjacencyList(command, edgeType, batchIdSet, batchPartitionKeySet);

                int startIndex = 0;
                foreach (KeyValuePair<int, VertexField> kvp in batchGraphSonDict)
                {
                    int insertedPosition = kvp.Key;
                    int length = insertedPosition - startIndex;
                    VertexField vertexField = kvp.Value;

                    finalGraphSonResult.Append(notBatchedGraphSonResult.ToString(startIndex, length));
                    finalGraphSonResult.Append(vertexField.ToGraphSON());
                    startIndex = insertedPosition;
                }

                finalGraphSonResult.Append(notBatchedGraphSonResult.ToString(startIndex,
                    notBatchedGraphSonResult.Length - startIndex));

            }
            else {
                finalGraphSonResult.Append(notBatchedGraphSonResult.ToString());
            }

            finalGraphSonResult.Append("]");
            return finalGraphSonResult.ToString();
        }
    }

    public class GraphTraversal : IEnumerable<string>
    {
        public class GraphTraversalIterator : IEnumerator<string>
        {
            private readonly GraphViewCommand command;
            private string currentRecord;
            private readonly GraphViewExecutionOperator currentOperator;
            private readonly OutputFormat outputFormat;
            private bool firstCall;

            internal GraphTraversalIterator(GraphViewExecutionOperator pCurrentOperator, 
                GraphViewCommand command, OutputFormat outputFormat)
            {
                this.command = command;
                this.currentOperator = pCurrentOperator;
                this.outputFormat = outputFormat;
                this.firstCall = true;
            }

            public bool MoveNext()
            {
                if (currentOperator == null) return false;

                if (outputFormat == OutputFormat.GraphSON)
                {
                    List<RawRecord> rawRecordResults = new List<RawRecord>();

                    RawRecord outputRec = null;
                    bool firstEntry = true;
                    while ((outputRec = currentOperator.Next()) != null) {
                        rawRecordResults.Add(outputRec);
                        firstEntry = false;
                    }

                    if (firstEntry && !firstCall) {
                        return false;
                    }
                    else
                    {
                        firstCall = false;
                        currentRecord = GraphSONProjector.ToGraphSON(rawRecordResults, this.command);
                        return true;
                    }
                }
                else
                {
                    RawRecord outputRec = null;
                    if ((outputRec = currentOperator.Next()) != null)
                    {
                        currentRecord = outputRec[0].ToString();
                        return currentRecord != null;
                    }
                    else return false;
                }
            }

            public void Reset() {}

            object IEnumerator.Current
            {
                get
                {
                    return currentRecord;
                }
            }

            public string Current
            {
                get
                {
                    return currentRecord;
                }
            }

            public void Dispose()
            {

            }
        }

        public IEnumerator<string> GetEnumerator()
        {
            GremlinUtil.ClearCounters();
            var sqlScript = GetEndOp().ToSqlScript();
            SqlScript = sqlScript.ToString();
            it = new GraphTraversalIterator(sqlScript.Batches[0].Compile(null, this.Command), this.Command, outputFormat);
            return it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public string SqlScript { get; set; }
        private GraphTraversalIterator it;
        public GraphViewCommand Command { get; set; }
        internal List<GremlinTranslationOperator> GremlinTranslationOpList { get; set; }

        OutputFormat outputFormat;

        public GraphTraversal()
        {
            GremlinTranslationOpList = new List<GremlinTranslationOperator>();
        }

        public GraphTraversal(GraphViewCommand command)
        {
            GremlinTranslationOpList = new List<GremlinTranslationOperator>();
            Command = command;
            outputFormat = OutputFormat.Regular;
        }

        public GraphTraversal(GraphViewCommand command, OutputFormat outputFormat)
        {
            GremlinTranslationOpList = new List<GremlinTranslationOperator>();
            this.Command = command;
            this.outputFormat = outputFormat;
        }

        public List<string> Next()
        {
            GremlinUtil.ClearCounters();
            WSqlScript sqlScript = GetEndOp().ToSqlScript();
            SqlScript = sqlScript.ToString();

            GraphViewExecutionOperator op = sqlScript.Batches[0].Compile(null, this.Command);
            List<RawRecord> rawRecordResults = new List<RawRecord>();
            RawRecord outputRec = null;

            while ((outputRec = op.Next()) != null) {
                rawRecordResults.Add(outputRec);
            }

            List<string> results = new List<string>();

            switch (outputFormat)
            {
                case OutputFormat.GraphSON:
                    results.Add(GraphSONProjector.ToGraphSON(rawRecordResults, this.Command));
                    break;
                default:
                    foreach (var record in rawRecordResults) {
                        FieldObject field = record[0];
                        results.Add(field.ToString());
                    }
                    break;
            }

            return results;
        }

        internal void InsertGremlinOperator(int index, GremlinTranslationOperator newGremlinTranslationOp)
        {
            if (index > GremlinTranslationOpList.Count || index < 0) 
                throw new QueryCompilationException();
            GremlinTranslationOpList.Insert(index, newGremlinTranslationOp);
            if (index > 0)
            {
                newGremlinTranslationOp.InputOperator = GremlinTranslationOpList[index - 1];
            }
            if (index + 1 < GremlinTranslationOpList.Count)
            {
                GremlinTranslationOpList[index + 1].InputOperator = newGremlinTranslationOp;
            }
        }

        internal GremlinTranslationOperator PopGremlinOperator()
        {
            return this.RemoveGremlinOperator(GremlinTranslationOpList.Count - 1);
        }
        
        internal GremlinTranslationOperator RemoveGremlinOperator(int index)
        {
            if (index >= GremlinTranslationOpList.Count || index < 0)
                throw new QueryCompilationException();

            GremlinTranslationOperator removedOp = GremlinTranslationOpList[index].Copy();

            GremlinTranslationOpList.RemoveAt(index);

            if (index != GremlinTranslationOpList.Count && index >= 0)
            {
                if (index > 0)
                {
                    GremlinTranslationOpList[index].InputOperator = GremlinTranslationOpList[index - 1];
                }
                else
                {
                    GremlinTranslationOpList[index].InputOperator = null;
                }
            }

            return removedOp;
        }

        internal void ReplaceGremlinOperator(int index, GremlinTranslationOperator newGremlinTranslationOp)
        {
            this.RemoveGremlinOperator(index);
            this.InsertGremlinOperator(index, newGremlinTranslationOp);
        }

        internal void AddGremlinOperator(GremlinTranslationOperator newGremlinTranslationOp)
        {
            GremlinTranslationOperator LastGremlinTranslationOp = this.GetEndOp();
            if (LastGremlinTranslationOp is GremlinAndOp && (LastGremlinTranslationOp as GremlinAndOp).IsInfix)
            {
                (LastGremlinTranslationOp as GremlinAndOp).SecondTraversal.AddGremlinOperator(newGremlinTranslationOp);
            }
            else if (LastGremlinTranslationOp is GremlinOrOp && (LastGremlinTranslationOp as GremlinOrOp).IsInfix)
            {
                (LastGremlinTranslationOp as GremlinOrOp).SecondTraversal.AddGremlinOperator(newGremlinTranslationOp);
            }
            else
            {
                GremlinTranslationOpList.Add(newGremlinTranslationOp);
                newGremlinTranslationOp.InputOperator = LastGremlinTranslationOp;
            }
        }

        internal GremlinTranslationOperator GetStartOp()
        {
            return GremlinTranslationOpList.Count == 0 ? null : GremlinTranslationOpList.First();
        }

        internal GremlinTranslationOperator GetEndOp()
        {
            return GremlinTranslationOpList.Count == 0 ? null : GremlinTranslationOpList.Last();
        }

        // get operator by index, return null if out of range
        internal GremlinTranslationOperator GetOp(int index)
        {
            return GremlinTranslationOpList.Count <= index ? null : GremlinTranslationOpList[index];
        }

        public GraphTraversal AddE()
        {
            throw new SyntaxErrorException("AddE must have a label");
            return this;
        }

        public GraphTraversal AddE(string edgeLabel)
        {
            AddGremlinOperator(new GremlinAddEOp(edgeLabel));
            return this;
        }

        public GraphTraversal AddV()
        {
            AddGremlinOperator(new GremlinAddVOp());
            return this;
        }

        public GraphTraversal AddV(params object[] propertyKeyValues)
        {
            AddGremlinOperator(new GremlinAddVOp(propertyKeyValues));
            return this;
        }

        public GraphTraversal AddV(string vertexLabel)
        {
            AddGremlinOperator(new GremlinAddVOp(vertexLabel));
            return this;
        }

        public GraphTraversal Aggregate(string sideEffectKey)
        {
            AddGremlinOperator(new GremlinAggregateOp(sideEffectKey));
            return this;
        }

        public GraphTraversal And(params GraphTraversal[] andTraversals)
        {
            if (andTraversals.Length == 0)
            {
                //Infix And step
                GraphTraversal firstTraversal = GraphTraversal.__();
                GraphTraversal sencondTraversal = GraphTraversal.__();
                for (var i = 1; i < GremlinTranslationOpList.Count; i++)
                {
                    firstTraversal.AddGremlinOperator(GremlinTranslationOpList[i]);
                }
                GremlinTranslationOpList.RemoveRange(1, GremlinTranslationOpList.Count - 1);
                GremlinAndOp newAndOp = new GremlinAndOp(firstTraversal, sencondTraversal);
                AddGremlinOperator(newAndOp);
            }
            else
            {
                AddGremlinOperator(new GremlinAndOp(andTraversals));
            }
            return this;
        }

        public GraphTraversal As(params string[] labels) {
            AddGremlinOperator(new GremlinAsOp(labels));
            return this;    
        }

        public GraphTraversal Barrier()
        {
            AddGremlinOperator(new GremlinBarrierOp());
            return this;
        }

        public GraphTraversal Barrier(int maxBarrierSize)
        {
            AddGremlinOperator(new GremlinBarrierOp(maxBarrierSize));
            return this;
        }

        public GraphTraversal Both(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinBothOp(edgeLabels));
            return this;
        }

        public GraphTraversal BothE(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinBothEOp(edgeLabels));
            return this;
        }

        public GraphTraversal BothV()
        {
            AddGremlinOperator(new GremlinBothVOp());
            return this;
        }

        public GraphTraversal By()
        {
            GetEndOp().ModulateBy();
            return this;
        }

        public GraphTraversal By(GremlinKeyword.Order order)
        {
            GetEndOp().ModulateBy(order);
            return this;
        }

        public GraphTraversal By(IComparer comparer)
        {
            GetEndOp().ModulateBy(comparer);
            return this;
        }

        public GraphTraversal By(GremlinKeyword.Column column)
        {
            GetEndOp().ModulateBy(column);
            return this;
        }

        public GraphTraversal By(GremlinKeyword.Column column, GremlinKeyword.Order order)
        {
            GetEndOp().ModulateBy(column, order);
            return this;
        }

        public GraphTraversal By(GremlinKeyword.Column column, IComparer comparer)
        {
            GetEndOp().ModulateBy(column, comparer);
            return this;
        }

        public GraphTraversal By(string key)
        {
            GetEndOp().ModulateBy(key);
            return this;
        }

        public GraphTraversal By(string key, GremlinKeyword.Order order)
        {
            GetEndOp().ModulateBy(key, order);
            return this;
        }

        public GraphTraversal By(string key, IComparer order)
        {
            GetEndOp().ModulateBy(key, order);
            return this;
        }

        public GraphTraversal By(GraphTraversal traversal)
        {
            GetEndOp().ModulateBy(traversal);
            return this;
        }

        public GraphTraversal By(GraphTraversal traversal, GremlinKeyword.Order order)
        {
            GetEndOp().ModulateBy(traversal, order);
            return this;
        }

        public GraphTraversal By(GraphTraversal traversal, IComparer order)
        {
            GetEndOp().ModulateBy(traversal, order);
            return this;
        }

        public GraphTraversal By(GremlinKeyword.T token)
        {
            GetEndOp().ModulateBy(token);
            return this;
        }

        public GraphTraversal Cap(params string[] sideEffectKeys)
        {
            AddGremlinOperator(new GremlinCapOp(sideEffectKeys));
            return this;
        }

        public GraphTraversal Choose(Predicate choosePredicate, GraphTraversal trueChoice, GraphTraversal falseChoice = null)
        {
            if (falseChoice == null) falseChoice = __();
            AddGremlinOperator(new GremlinChooseOp(__().Is(choosePredicate), trueChoice, falseChoice));
            return this;
        }

        public GraphTraversal Choose(GraphTraversal traversalPredicate, GraphTraversal trueChoice, GraphTraversal falseChoice = null)
        {
            if (falseChoice == null) falseChoice = __();
            AddGremlinOperator(new GremlinChooseOp(traversalPredicate, trueChoice, falseChoice));
            return this;
        }

        public GraphTraversal Choose(GraphTraversal choiceTraversal)
        {
            AddGremlinOperator(new GremlinChooseOp(choiceTraversal));
            return this;
        }

        public GraphTraversal Coalesce(params GraphTraversal[] coalesceTraversals)
        {
            AddGremlinOperator(new GremlinCoalesceOp(coalesceTraversals));
            return this;
        }

        public GraphTraversal Coin(double probability)
        {
            AddGremlinOperator(new GremlinCoinOp(probability));
            return this;
        }

        public GraphTraversal Constant()
        {
            AddGremlinOperator(new GremlinConstantOp(new List<object>()));
            return this;
        }

        public GraphTraversal Constant(object value)
        {
            if (GremlinUtil.IsList(value)
                || GremlinUtil.IsArray(value)
                || GremlinUtil.IsNumber(value)
                || value is string
                || value is bool)
            {
                AddGremlinOperator(new GremlinConstantOp(value));
            }
            else
            {
                throw new ArgumentException();
            }
            return this;
        }

        public GraphTraversal Count()
        {
            AddGremlinOperator(new GremlinCountOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Count(GremlinKeyword.Scope scope)
        {
            if (scope == GremlinKeyword.Scope.Global)
            {
                AddGremlinOperator(new GremlinCountOp(scope));
            }
            else
            {
                AddGremlinOperator(new GremlinCountOp(scope));
            }
            return this;
        }

        public GraphTraversal CyclicPath()
        {
            AddGremlinOperator(new GremlinCyclicPathOp());
            return this;
        }

        public GraphTraversal Dedup(GremlinKeyword.Scope scope, params string[] dedupLabels)
        {
            AddGremlinOperator(new GremlinDedupOp(scope, dedupLabels));
            return this;
        }

        public GraphTraversal Dedup(params string[] dedupLabels)
        {
            AddGremlinOperator(new GremlinDedupOp(GremlinKeyword.Scope.Global, dedupLabels));
            return this;
        }

        public GraphTraversal Drop()
        {
            AddGremlinOperator(new GremlinDropOp());
            return this;
        }

        public GraphTraversal E()
        {
            AddGremlinOperator(new GremlinEOp());
            return this;
        }

        public GraphTraversal E(params object[] edgeIdsOrElements)
        {
            AddGremlinOperator(new GremlinEOp(edgeIdsOrElements));
            return this;
        }

        public GraphTraversal E(List<object> edgeIdsOrElements)
        {
            AddGremlinOperator(new GremlinEOp(edgeIdsOrElements));
            return this;
        }

        public GraphTraversal Emit()
        {
            GremlinRepeatOp lastOp = GetEndOp() as GremlinRepeatOp;
            if (lastOp != null && lastOp.IsEmit == false)
            {
                lastOp.IsEmit = true;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).IsEmit = true;
                (GetEndOp() as GremlinRepeatOp).EmitContext = true;
            }
            return this;
        }

        public GraphTraversal Emit(Predicate emitPredicate)
        {
            GremlinRepeatOp lastOp = GetEndOp() as GremlinRepeatOp;
            if (lastOp != null && lastOp.IsEmit == false)
            {
                lastOp.IsEmit = true;
                lastOp.EmitPredicate = emitPredicate;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).IsEmit = true;
                (GetEndOp() as GremlinRepeatOp).EmitPredicate = emitPredicate;
                (GetEndOp() as GremlinRepeatOp).EmitContext = true;
            }
            return this;
        }

        public GraphTraversal Emit(GraphTraversal emitTraversal)
        {
            GremlinRepeatOp lastOp = GetEndOp() as GremlinRepeatOp;
            if (lastOp != null && lastOp.IsEmit == false)
            {
                lastOp.IsEmit = true;
                lastOp.EmitTraversal = emitTraversal;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).IsEmit = true;
                (GetEndOp() as GremlinRepeatOp).EmitTraversal = emitTraversal;
                (GetEndOp() as GremlinRepeatOp).EmitContext = true;
            }
            return this;
        }

        public GraphTraversal FlatMap(GraphTraversal flatMapTraversal)
        {
            AddGremlinOperator(new GremlinFlatMapOp(flatMapTraversal));
            return this;
        }

        public GraphTraversal Fold()
        {
            AddGremlinOperator(new GremlinFoldOp());
            return this;
        }

        public GraphTraversal From(string fromLabel)
        {
            GremlinAddEOp addEOp = this.GetEndOp() as GremlinAddEOp;
            GremlinPathOp pathOp = this.GetEndOp() as GremlinPathOp;
            GremlinSimplePathOp simplePathOp = this.GetEndOp() as GremlinSimplePathOp;
            GremlinCyclicPathOp cyclicPathOp = this.GetEndOp() as GremlinCyclicPathOp;
            if (addEOp != null)
            {
                addEOp.FromVertexTraversal = GraphTraversal.__().Select(fromLabel);
            }
            else if (pathOp != null)
            {
                pathOp.FromLabel = fromLabel;
            }
            else if (simplePathOp != null)
            {
                simplePathOp.FromLabel = fromLabel;
            }
            else if (cyclicPathOp != null)
            {
                cyclicPathOp.FromLabel = fromLabel;
            }
            else
            {
                throw new SyntaxErrorException($"{this.GetEndOp()} cannot be cast to GremlinAddEOp");
            }
            return this;
        }

        public GraphTraversal From(GraphTraversal fromVertexTraversal)
        {
            GremlinAddEOp addEOp = this.GetEndOp() as GremlinAddEOp;
            if (addEOp != null)
            {
                addEOp.FromVertexTraversal = fromVertexTraversal;
            }
            else
            {
                throw new SyntaxErrorException($"{this.GetEndOp()} cannot be cast to GremlinAddEOp");
            }
            return this;
        }

        public GraphTraversal Group()
        {
            AddGremlinOperator(new GremlinGroupOp());
            return this;
        }

        public GraphTraversal Group(string sideEffectKey)
        {
            AddGremlinOperator(new GremlinGroupOp(sideEffectKey));
            return this;
        }

        public GraphTraversal GroupCount()
        {
            GremlinGroupOp newGroupOp = new GremlinGroupOp();
            newGroupOp.ProjectBy = __().Count();
            newGroupOp.IsProjectingACollection = false;
            AddGremlinOperator(newGroupOp);
            return this;
        }

        public GraphTraversal GroupCount(string sideEffectKey)
        {
            GremlinGroupOp newGroupOp = new GremlinGroupOp(sideEffectKey);
            newGroupOp.ProjectBy = __().Count();
            newGroupOp.IsProjectingACollection = false;
            AddGremlinOperator(newGroupOp);
            return this;
        }

        public GraphTraversal Has(string propertyKey)
        {
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasProperty, propertyKey));
            return this;
        }

        public GraphTraversal HasNot(string propertyKey)
        {
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasNotProperty, propertyKey));
            return this;
        }

        public GraphTraversal Has(string propertyKey, object predicateOrValue)
        {
            GremlinUtil.CheckIsValueOrPredicate(predicateOrValue);
            AddGremlinOperator(new GremlinHasOp(propertyKey, predicateOrValue));
            return this;
        }

        public GraphTraversal Has(string propertyKey, GraphTraversal propertyTraversal)
        {
            AddGremlinOperator(new GremlinHasOp(propertyKey, propertyTraversal));
            return this;
        }

        public GraphTraversal Has(string label, string propertyKey, object predicateOrValue)
        {
            GremlinUtil.CheckIsValueOrPredicate(predicateOrValue);
            AddGremlinOperator(new GremlinHasOp(label, propertyKey, predicateOrValue));
            return this;
        }

        public GraphTraversal Has(GremlinKeyword.T token, object predicateOrValue)
        {
            GremlinUtil.CheckIsValueOrPredicate(predicateOrValue);
            switch (token)
            {
                case GremlinKeyword.T.Id:
                    AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasId, predicateOrValue));
                    break;
                case GremlinKeyword.T.Label:
                    AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasLabel, predicateOrValue));
                    break;
                case GremlinKeyword.T.Key:
                    AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasKey, predicateOrValue));
                    break;
                case GremlinKeyword.T.Value:
                    AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasValue, predicateOrValue));
                    break;
                default:
                    throw new TranslationException("Unknown GremlinKeyword.T");
            }
            return this;
        }

        public GraphTraversal HasId(params object[] valuesOrPredicates)
        {
            GremlinUtil.CheckIsValueOrPredicate(valuesOrPredicates);
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasId, valuesOrPredicates));
            return this;
        }

        public GraphTraversal HasLabel(params object[] valuesOrPredicates)
        {
            GremlinUtil.CheckIsValueOrPredicate(valuesOrPredicates);
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasLabel, valuesOrPredicates));
            return this;
        }

        public GraphTraversal HasKey(params string[] valuesOrPredicates)
        {
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasKey, valuesOrPredicates));
            return this;
        }

        public GraphTraversal HasValue(params object[] valuesOrPredicates)
        {
            GremlinUtil.CheckIsValueOrPredicate(valuesOrPredicates);
            AddGremlinOperator(new GremlinHasOp(GremlinHasType.HasValue, valuesOrPredicates));
            return this;
        }

        public GraphTraversal Id()
        {
            AddGremlinOperator(new GremlinIdOp());
            return this;
        }

        public GraphTraversal Identity()
        {
            return this;
        }

        public GraphTraversal In(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinInOp(edgeLabels));
            return this;
        }

        public GraphTraversal InE(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinInEOp(edgeLabels));
            return this;
        }

        public GraphTraversal Inject()
        {
            //Do nothing
            return this;
        }

        public GraphTraversal Inject(params object[] injections)
        {
            foreach (var injection in injections)
            {
                if (GremlinUtil.IsList(injection)
                    || GremlinUtil.IsArray(injection)
                    || GremlinUtil.IsNumber(injection)
                    || injection is string
                    || injection is bool)
                {
                    AddGremlinOperator(new GremlinInjectOp(injection));
                }
                else
                {
                    throw new ArgumentException();
                }
            }
            return this;
        }

        public GraphTraversal InV()
        {
            AddGremlinOperator(new GremlinInVOp());
            return this;
        }

        public GraphTraversal Is(object value)
        {
            AddGremlinOperator(new GremlinIsOp(value));
            return this;
        }

        public GraphTraversal Is(Predicate predicate)
        {
            AddGremlinOperator(new GremlinIsOp(predicate));
            return this;
        }

        public GraphTraversal Key()
        {
            AddGremlinOperator(new GremlinKeyOp());
            return this;
        }

        public GraphTraversal Label()
        {
            AddGremlinOperator(new GremlinLabelOp());
            return this;
        }

        public GraphTraversal Limit(int limit)
        {
            AddGremlinOperator(new GremlinRangeOp(0, limit, GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Limit(GremlinKeyword.Scope scope, int limit)
        {
            AddGremlinOperator(new GremlinRangeOp(0, limit, scope));
            return this;
        }

        public GraphTraversal Local(GraphTraversal localTraversal)
        {
            AddGremlinOperator(new GremlinLocalOp(localTraversal));
            return this;
        }

        public GraphTraversal Map(GraphTraversal mapTraversal)
        {
            AddGremlinOperator(new GremlinMapOp(mapTraversal));
            return this;   
        }

        public GraphTraversal Match(params GraphTraversal[] matchTraversals)
        {
            // AddGremlinOperator(new GremlinMatchOp(matchTraversals));
            // Polyfill-Match: Implement Match by `Choose`, `Where` and `Select`, but do not support the Infix-And and Infix-Or.
            (new PolyfillHelper.Match(PolyfillHelper.Match.Connective.AND, matchTraversals)).Polyfill(this);
            return this;
        }

        public GraphTraversal Max()
        {
            AddGremlinOperator(new GremlinMaxOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Max(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinMaxOp(scope));
            return this;
        }

        public GraphTraversal Mean()
        {
            AddGremlinOperator(new GremlinMeanOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Mean(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinMeanOp(scope));
            return this;
        }

        public GraphTraversal Min()
        {
            AddGremlinOperator(new GremlinMinOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Min(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinMinOp(scope));
            return this;
        }

        public GraphTraversal Not(GraphTraversal notTraversal)
        {
           AddGremlinOperator(new GremlinNotOp(notTraversal));
            return this;
        }
        public GraphTraversal Option(object pickToken, GraphTraversal traversalOption)
        {
            if (!(GremlinUtil.IsNumber(pickToken) || pickToken is string || pickToken is GremlinKeyword.Pick || pickToken is bool))
            {
                throw new ArgumentException();
            }
            var op = this.GetEndOp() as GremlinChooseOp;
            if (op != null)
            {
                if (op.Options.ContainsKey(pickToken))
                {
                    throw new SyntaxErrorException($"Choose step can only have one traversal per pick token: {pickToken}");
                }
                op.Options[pickToken] = traversalOption;
                return this;
            }
            throw new Exception("Option step only can follow by choose step.");
        }

        public GraphTraversal Optional(GraphTraversal traversalOption)
        {
            AddGremlinOperator(new GremlinOptionalOp(traversalOption));
            return this;
        }

        public GraphTraversal Or(params GraphTraversal[] orTraversals)
        {
            if (orTraversals.Length == 0)
            {
                //Infix And step
                GraphTraversal firstTraversal = GraphTraversal.__();
                GraphTraversal sencondTraversal = GraphTraversal.__();
                for (var i = 1; i < GremlinTranslationOpList.Count; i++)
                {
                    firstTraversal.AddGremlinOperator(GremlinTranslationOpList[i].Copy());
                }
                GremlinTranslationOpList.RemoveRange(1, GremlinTranslationOpList.Count - 1);
                GremlinOrOp newOrOp = new GremlinOrOp(firstTraversal, sencondTraversal);
                AddGremlinOperator(newOrOp);
            }
            else
            {
                AddGremlinOperator(new GremlinOrOp(orTraversals));
            }
            return this;
        }

        public GraphTraversal Order()
        {
            AddGremlinOperator(new GremlinOrderOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Order(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinOrderOp(scope));
            return this;
        }

        public GraphTraversal OtherV()
        {
            AddGremlinOperator(new GremlinOtherVOp());
            return this;
        }

        public GraphTraversal Out(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinOutOp(edgeLabels));
            return this;
        }

        public GraphTraversal OutE(params string[] edgeLabels)
        {
            AddGremlinOperator(new GremlinOutEOp(edgeLabels));
            return this;
        }

        public GraphTraversal OutV()
        {
            AddGremlinOperator(new GremlinOutVOp());
            return this;
        }

        public GraphTraversal Path()
        {
            AddGremlinOperator(new GremlinPathOp());
            return this;
        }

        public GraphTraversal Project(params string[] projectKeys)
        {
            AddGremlinOperator(new GremlinProjectOp(projectKeys));
            return this;
        }

        public GraphTraversal Properties(params string[] propertyKeys)
        {
            AddGremlinOperator(new GremlinPropertiesOp(propertyKeys));
            return this;
        }

        public GraphTraversal Property(string key, object value, params object[] keyValues)
        {
            return Property(GremlinKeyword.PropertyCardinality.Single, key, value, keyValues);
        }

        public GraphTraversal Property(GremlinKeyword.PropertyCardinality cardinality, string key, object value,
            params object[] keyValues)
        {
            if (keyValues.Length % 2 != 0) throw new Exception("The parameter of property should be even");

            var lastOp = this.GetEndOp() as GremlinAddEOp;
            if (lastOp != null)
            {
                if (keyValues.Length > 0) throw new SyntaxErrorException("Only vertex can use PropertyCardinality.List and have meta properties");
                GremlinProperty property = new GremlinProperty(cardinality, key, value, null);
                lastOp.EdgeProperties.Add(property);
            }
            else
            {
                Dictionary<string, object> metaProperties = new Dictionary<string, object>();
                for (var i = 0; i < keyValues.Length; i += 2)
                {
                    metaProperties[keyValues[i] as string] = keyValues[i + 1];
                }
                GremlinProperty property = new GremlinProperty(cardinality, key, value, metaProperties);
                AddGremlinOperator(new GremlinPropertyOp(property));
            }
            return this;
        }

        public GraphTraversal PropertyMap(params string[] propertyKeys)
        {
            AddGremlinOperator(new GremlinPropertyMapOp(propertyKeys));
            return this;
        }

        public GraphTraversal Range(int low, int high)
        {
            AddGremlinOperator(new GremlinRangeOp(low, high, GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Range(GremlinKeyword.Scope scope, int low, int high)
        {
            AddGremlinOperator(new GremlinRangeOp(low, high, scope));
            return this;
        }

        public GraphTraversal Repeat(GraphTraversal repeatTraversal)
        {
            if (GetEndOp() is GremlinRepeatOp && (GetEndOp() as GremlinRepeatOp).IsFake)
            {
                // Repeat after emit/until/times
                (GetEndOp() as GremlinRepeatOp).RepeatTraversal = repeatTraversal;
                (GetEndOp() as GremlinRepeatOp).IsFake = false;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp(repeatTraversal));
            }
            return this;
        }

        public GraphTraversal Sample(int amountToSample)
        {
            AddGremlinOperator(new GremlinSampleOp(GremlinKeyword.Scope.Global, amountToSample));
            return this;
        }

        public GraphTraversal Sample(GremlinKeyword.Scope scope, int amountToSample)
        {
            AddGremlinOperator(new GremlinSampleOp(scope, amountToSample));
            return this;
        }

        public GraphTraversal Select(GremlinKeyword.Column column)
        {
            AddGremlinOperator(new GremlinSelectColumnOp(column));
            return this;
        }

        public GraphTraversal Select(GremlinKeyword.Pop pop, params string[] selectKeys)
        {
            AddGremlinOperator(new GremlinSelectOp(pop, selectKeys));
            return this;
        }

        public GraphTraversal Select(params string[] selectKeys)
        {
            AddGremlinOperator(new GremlinSelectOp(GremlinKeyword.Pop.All, selectKeys));
            return this;
        }

        public GraphTraversal SideEffect(GraphTraversal sideEffectTraversal)
        {
            AddGremlinOperator(new GremlinSideEffectOp(sideEffectTraversal));
            return this;    
        }

        public GraphTraversal SimplePath()
        {
            AddGremlinOperator(new GremlinSimplePathOp());
            return this;
        }

        public GraphTraversal Store(string sideEffectKey)
        {
            AddGremlinOperator(new GremlinStoreOp(sideEffectKey));
            return this;
        }

        public GraphTraversal Sum()
        {
            AddGremlinOperator(new GremlinSumOp(GremlinKeyword.Scope.Global));
            return this;
        }

        public GraphTraversal Subgraph(string sideEffectKey)
        {
            AddGremlinOperator(new GremlinSubgraphOp(sideEffectKey));
            return this;
        }

        public GraphTraversal Sum(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinSumOp(scope));
            return this;
        }

        public GraphTraversal Tail()
        {
            AddGremlinOperator(new GremlinRangeOp(0, 1, GremlinKeyword.Scope.Global, true));
            return this;
        }

        public GraphTraversal Tail(int limit)
        {
            AddGremlinOperator(new GremlinRangeOp(0, limit, GremlinKeyword.Scope.Global, true));
            return this;
        }

        public GraphTraversal Tail(GremlinKeyword.Scope scope)
        {
            AddGremlinOperator(new GremlinRangeOp(0, 1, scope, true));
            return this;
        }

        public GraphTraversal Tail(GremlinKeyword.Scope scope, int limit)
        {
            AddGremlinOperator(new GremlinRangeOp(0, limit, scope, true));
            return this;
        }

        public GraphTraversal TimeLimit(long timeLimit)
        {
            throw new NotImplementedException();
        }

        public GraphTraversal Times(int maxLoops)
        {
            maxLoops = Math.Max(maxLoops, 1);
            GremlinRepeatOp lastOp = (GetEndOp() as GremlinRepeatOp);
            if (lastOp != null && lastOp.RepeatTimes == -1 && lastOp.TerminationTraversal == null)
            {
                lastOp.RepeatTimes = maxLoops;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).RepeatTimes = maxLoops;
            }
            return this;
        }

        public GraphTraversal To(string toLabel)
        {
            GremlinAddEOp addEOp = this.GetEndOp() as GremlinAddEOp;
            GremlinPathOp pathOp = this.GetEndOp() as GremlinPathOp;
            GremlinSimplePathOp simplePathOp = this.GetEndOp() as GremlinSimplePathOp;
            GremlinCyclicPathOp cyclicPathOp = this.GetEndOp() as GremlinCyclicPathOp;
            if (addEOp != null)
            {
                addEOp.ToVertexTraversal = GraphTraversal.__().Select(toLabel);
            }
            else if (pathOp != null)
            {
                pathOp.ToLabel = toLabel;
            }
            else if (simplePathOp != null)
            {
                simplePathOp.ToLabel = toLabel;
            }
            else if (cyclicPathOp != null)
            {
                cyclicPathOp.ToLabel = toLabel;
            }
            else
            {
                throw new SyntaxErrorException($"{this.GetEndOp()} cannot be cast to GremlinAddEOp");
            }
            return this;
        }

        public GraphTraversal To(GraphTraversal toVertex)
        {
            GremlinAddEOp addEOp = this.GetEndOp() as GremlinAddEOp;
            if (addEOp != null)
            {
                addEOp.ToVertexTraversal = toVertex;
            }
            else
            {
                throw new SyntaxErrorException($"{this.GetEndOp()} cannot be cast to GremlinAddEOp");
            }
            return this;
        }

        public GraphTraversal Tree()
        {
            AddGremlinOperator(new GremlinTreeOp());
            return this;
        }

        public GraphTraversal Tree(string sideEffectKey)
        {
            AddGremlinOperator(new GremlinTreeOp(sideEffectKey));
            return this;
        }

        public GraphTraversal Unfold()
        {
            AddGremlinOperator(new GremlinUnfoldOp());
            return this;
        }

        public GraphTraversal Union(params GraphTraversal[] unionTraversals)
        {
            AddGremlinOperator(new GremlinUnionOp(unionTraversals));
            return this;
        }

        public GraphTraversal Until(Predicate untilPredicate)
        {
            GremlinRepeatOp lastOp = GetEndOp() as GremlinRepeatOp;
            if (lastOp != null && lastOp.RepeatTimes == -1 && lastOp.TerminationTraversal == null)
            {
                lastOp.TerminationPredicate = untilPredicate;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).TerminationPredicate = untilPredicate;
                (GetEndOp() as GremlinRepeatOp).StartFromContext = true;
            }
            return this;
        }

        public GraphTraversal Until(GraphTraversal untilTraversal)
        {
            GremlinRepeatOp lastOp = GetEndOp() as GremlinRepeatOp;
            if (lastOp != null && lastOp.RepeatTimes == -1 && lastOp.TerminationTraversal == null)
            {
                lastOp.TerminationTraversal = untilTraversal;
            }
            else
            {
                AddGremlinOperator(new GremlinRepeatOp());
                (GetEndOp() as GremlinRepeatOp).TerminationTraversal = untilTraversal;
                (GetEndOp() as GremlinRepeatOp).StartFromContext = true;
            }
            return this;
        }

        public GraphTraversal V(params object[] vertexIdsOrElements)
        {
            AddGremlinOperator(new GremlinVOp(vertexIdsOrElements));
            return this;
        }

        public GraphTraversal V(List<object> vertexIdsOrElements)
        {
            AddGremlinOperator(new GremlinVOp(vertexIdsOrElements));
            return this;
        }

        public GraphTraversal Value()
        {
            AddGremlinOperator(new GremlinValueOp());
            return this;
        }

        public GraphTraversal ValueMap(params string[] propertyKeys)
        {
            AddGremlinOperator(new GremlinValueMapOp(false, propertyKeys));
            return this;
        }

        public GraphTraversal ValueMap(bool isIncludeTokens, params string[] propertyKeys)
        {
            AddGremlinOperator(new GremlinValueMapOp(isIncludeTokens, propertyKeys));
            return this;
        }

        public GraphTraversal Values(params string[] propertyKeys)
        {
            AddGremlinOperator(new GremlinValuesOp(propertyKeys));
            return this;
        }

        public GraphTraversal Where(Predicate predicate)
        {
            AddGremlinOperator(new GremlinWherePredicateOp(predicate));
            return this;
        }

        public GraphTraversal Where(string startKey, Predicate predicate)
        {
            AddGremlinOperator(new GremlinWherePredicateOp(startKey, predicate));
            return this;
        }

        public GraphTraversal Where(GraphTraversal whereTraversal)
        {
            AddGremlinOperator(new GremlinWhereTraversalOp(whereTraversal));
            return this;
        }

        public static GraphTraversal __()
        {
            GraphTraversal newGraphTraversal = new GraphTraversal();
            newGraphTraversal.AddGremlinOperator(new GremlinParentContextOp());
            return newGraphTraversal;
        }

        public List<object> ToList()
        {
            //TODO
            var str = this.GetEndOp().ToSqlScript().ToString();
            return new List<object>() {1};
        }

        public IEnumerable<string> EvalGremlinTraversal(string sCSCode)
        {
            return EvalGraphTraversal(ConvertGremlinToGraphTraversalCode(sCSCode));    
        }

        public string ConvertGremlinToGraphTraversalCode(string sCSCode)
        {
            // transform all the quotes to escape quotes in string in gremlin(groovy).
            //     then take all the strings off, save in queue, and restore them in the end.
            //        i.e. : `g.inject('I say:"hello".').inject("I'm Blackjack.")` => 
            //               `g.inject("").inject("")` + Queue<string> strings { "I say:\"hello\".", "I\'m Blackjack." }
            Queue<string> strings = new Queue<string>();
            Regex reForString = new Regex(@"'(([^\\']|\\[tbnrf\\'""])*)'|""(([^\\""]|\\[tbnrf\\'""])*)""");
            sCSCode = reForString.Replace(sCSCode, match =>
            {
                strings.Enqueue((new Regex(@"(?<!\\)['""]")).Replace(
                    (match.Groups[1].Success ? match.Groups[1].Value : match.Groups[3].Value),
                    (m => m.Value == "\"" ? "\\\"" : "\\\'")));
                return "\"\"";
            });

            //replace gremlin steps with uppercase
            Regex reForStep = new Regex(@"\.\s*(\w+)\s*\(", RegexOptions.Compiled);
            sCSCode = reForStep.Replace(sCSCode,
                match => $".{ GremlinKeyword.GremlinStepToGraphTraversalDict[match.Groups[1].Value] }(");

            //replace with GraphTraversal FunctionName
            Regex reForFunction = new Regex(@"(\w+)\s*\.\s*(\w+)\s*\(", RegexOptions.Compiled);
            sCSCode = reForFunction.Replace(sCSCode, match =>
                $"{ GremlinKeyword.GremlinMainStepToGraphTraversalDict[match.Groups[1].Value] }.{ match.Groups[2].Value }(");
            
            //replace gremlin predicate with GraphTraversal predicate
            Regex reForPredicate = new Regex(@"\s*(\w+)\s*\(", RegexOptions.Compiled);
            sCSCode = reForPredicate.Replace(sCSCode, match => 
                (GremlinKeyword.GremlinPredicateToGraphTraversalDict.ContainsKey(match.Groups[1].Value) ? 
                    GremlinKeyword.GremlinPredicateToGraphTraversalDict[match.Groups[1].Value] : 
                    match.Groups[1].Value) + "(");

            //replace gremlin keyword
            Regex reForKeyword = new Regex(@"(?<=[,\(])\s*(\w+(\s*\.\s*\w+)*)\s*(?=[,\)])", RegexOptions.Compiled);
            sCSCode = reForKeyword.Replace(sCSCode, match => 
                (GremlinKeyword.GremlinKeywordToGraphTraversalDict.ContainsKey(match.Groups[1].Value) ? 
                    GremlinKeyword.GremlinKeywordToGraphTraversalDict[match.Groups[1].Value] :
                    match.Groups[1].Value));

            //replace gremlin array with C# array, nested array is not allowed
            Regex reForArray = new Regex(@"(\[\s*(([+-]?(0|[1-9][0-9]*)(\.[0-9]+)?|true|false|""(([^\\""]|\\[tbnrf\\'""])*)"")
                                          (\s*,\s*([+-]?(0|[1-9][0-9]*)(\.[0-9]+)?|true|false|""(([^\\""]|\\[tbnrf\\'""])*)""))*)?\s*\])", 
                                         RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);
            sCSCode = reForArray.Replace(sCSCode, match => $"new List<object> {{ { match.Groups[2].Value } }}");

            // restore all the strings
            sCSCode = (new Regex(@"""""")).Replace(sCSCode, match => $"\"{ strings.Dequeue() }\"");

            return sCSCode;
        }

        public IEnumerable<string> EvalGraphTraversal(string sCSCode)
        {
            CompilerParameters cp = new CompilerParameters();
            cp.ReferencedAssemblies.Add("GraphView.dll");
            cp.ReferencedAssemblies.Add("System.dll");
            cp.GenerateInMemory = true;

            StringBuilder sb = new StringBuilder("");
            sb.Append("using GraphView;\n");
            sb.Append("using System;\n");
            sb.Append("using System.Collections.Generic;\n");

            sb.Append("namespace GraphView { \n");
            sb.Append("public class Program { \n");
            sb.Append("public object Main() {\n");
            sb.Append("GraphViewConnection connection = new GraphViewConnection("+ getConnectionInfo() +");");
            sb.Append("GraphViewCommand graph = new GraphViewCommand(connection);\n");
            switch(outputFormat)
            {
                case OutputFormat.GraphSON:
                    sb.Append("graph.OutputFormat = OutputFormat.GraphSON;\r\n");
                    break;
            }
            sb.Append("return " + sCSCode + ";\n");
            sb.Append("}\n");
            sb.Append("}\n");
            sb.Append("}\n");

            CodeDomProvider icc = CodeDomProvider.CreateProvider("CSharp");
            CompilerResults cr = icc.CompileAssemblyFromSource(cp, sb.ToString());
            if (cr.Errors.Count > 0)
            {
                throw new Exception("ERROR: " + cr.Errors[0].ErrorText + "Error evaluating cs code");
            }

            System.Reflection.Assembly a = cr.CompiledAssembly;
            object o = a.CreateInstance("GraphView.Program");

            Type t = o.GetType();
            MethodInfo mi = t.GetMethod("Main");

            return (IEnumerable<string>)mi.Invoke(o, null);
        }

        private string addDoubleQuotes(string str)
        {
            return "\"" + str + "\"";
        }
#if TEST_ON_DOCUMENT_DB
        private string getConnectionInfo()
        {
            List<string> connectionList = new List<string>();
            connectionList.Add(addDoubleQuotes(Command.Connection.DocDBUrl));
            connectionList.Add(addDoubleQuotes(Command.Connection.DocDBPrimaryKey));
            connectionList.Add(addDoubleQuotes(Command.Connection.DocDBDatabaseId));
            connectionList.Add(addDoubleQuotes(Command.Connection.DocDBCollectionId));
            connectionList.Add($"{nameof(GraphType)}.{this.Command.Connection.GraphType}");
            connectionList.Add(Command.Connection.UseReverseEdges.ToString().ToLower());
            connectionList.Add(Command.Connection.EdgeSpillThreshold.ToString());
            connectionList.Add(Command.Connection.RealPartitionKey != null ? addDoubleQuotes(Command.Connection.RealPartitionKey) : "null");
            return string.Join(",", connectionList);
        }
#elif TEST_ON_JSONSERVER
        private string getConnectionInfo()
        {
            const string CONNECTION_STRING = "Data Source = (local); Initial Catalog = JsonTesting; Integrated Security = true;";
            const string COLLECTION_NAME = "GraphViewCollection";
            List<string> connectionList = new List<string>
            {
                this.addDoubleQuotes(CONNECTION_STRING),
                this.addDoubleQuotes(COLLECTION_NAME),
                "GraphType.GraphAPIOnly",
                "true",
                "1",
                "null",
                "null"
            };
            return string.Join(", ", connectionList);
        }
#endif

        public class PolyfillHelper
        {
            public class Match
            {
                public enum Connective
                {
                    AND,
                    OR
                };

                public Connective MatchConnective { get; set; }
                public List<GraphTraversal> MatchTraversals { get; set; }
                public List<Tuple<string, string>> StartAndEndLabelsPairList;
                public HashSet<string> Labels;

                public Match(Connective connective, params GraphTraversal[] matchTraversals)
                {
                    this.MatchConnective = connective;
                    this.MatchTraversals = new List<GraphTraversal>();
                    this.StartAndEndLabelsPairList = new List<Tuple<string, string>>();
                    this.Labels = new HashSet<string>();

                    foreach (GraphTraversal traversal in matchTraversals)
                    {
                        this.MatchTraversals.Add(this.ConfigureStartAndEndOperators(traversal));
                    }
                }

                internal void Polyfill(GraphTraversal traversal)
                {
                    if (this.StartAndEndLabelsPairList.Count == 0)
                    {
                        throw new TranslationException("the number of match-traversal should not be zero.");
                    }

                    if (this.MatchConnective == Connective.AND)
                    {
                        this.SortMatchTraversals();
                        var computedStartLabel = this.StartAndEndLabelsPairList[0].Item1;

                        GraphTraversal flatMapTraversal = __();

                        // tag the traverser by computedStartLabel
                        flatMapTraversal.Choose(__().Select(computedStartLabel), __().Identity(), __().As(computedStartLabel));
                        JoinMatchTraversals(flatMapTraversal, this.MatchTraversals);

                        // match(...) which include 'a', 'b', 'c' means it will select('a', 'b', 'c') and generate a map
                        flatMapTraversal.Select(GremlinKeyword.Pop.Last, this.Labels.ToArray());

                        traversal.FlatMap(flatMapTraversal);
                    }
                    else if (this.MatchConnective == Connective.OR)
                    {
                        traversal.Or(this.MatchTraversals.ToArray());
                    }
                    else
                    {
                        throw new TranslationException("MatchConnective should be AND or OR.");
                    }
                }

                internal GraphTraversal ConfigureStartAndEndOperators(GraphTraversal traversal)
                {
                    GraphTraversal configuredTraversal = __();

                    if ((traversal.GetStartOp() as GremlinParentContextOp) != null)
                    {
                        traversal.RemoveGremlinOperator(0);
                    }

                    // -------- Match-OR --------
                    GremlinOrOp orOperator = traversal.GetStartOp() as GremlinOrOp;
                    
                    if (orOperator != null)
                    {
                        if (orOperator.IsInfix)
                        {
                            (new PolyfillHelper.Match(Connective.OR, orOperator.FirstTraversal,
                                orOperator.SecondTraversal)).Polyfill(configuredTraversal);
                        }
                        else
                        {
                            (new PolyfillHelper.Match(Connective.OR, orOperator.OrTraversals.ToArray())).Polyfill(configuredTraversal);
                        }
                        this.StartAndEndLabelsPairList.Add(new Tuple<string, string>(null, null));
                        return configuredTraversal;
                    }

                    // -------- Match-AND-------
                    GremlinAndOp andOperator = traversal.GetStartOp() as GremlinAndOp;

                    if (andOperator != null)
                    {
                        if (andOperator.IsInfix)
                        {
                            (new PolyfillHelper.Match(Connective.AND, andOperator.FirstTraversal,
                                andOperator.SecondTraversal)).Polyfill(configuredTraversal);
                        }
                        else
                        {
                            (new PolyfillHelper.Match(Connective.AND, andOperator.AndTraversals.ToArray())).Polyfill(configuredTraversal);
                        }
                        
                        this.StartAndEndLabelsPairList.Add(new Tuple<string, string>(null, null));
                        return configuredTraversal;
                    }


                    // --------- Where()-Traversal ---------
                    GremlinWherePredicateOp wherePredicateOperator = traversal.GetStartOp() as GremlinWherePredicateOp;

                    if (wherePredicateOperator != null)
                    {
                        configuredTraversal.AddGremlinOperator(wherePredicateOperator);
                        this.StartAndEndLabelsPairList.Add(new Tuple<string, string>(null, null));
                        return configuredTraversal;
                    }


                    GremlinNotOp notOperator = traversal.GetStartOp() as GremlinNotOp;
                    GremlinWhereTraversalOp whereTraversalOperator = traversal.GetStartOp() as GremlinWhereTraversalOp;

                    if (notOperator != null)
                    {
                        if (traversal.GremlinTranslationOpList.Count != 1)
                        {
                            throw new TranslationException(
                                "It is not allowed to put other steps after the not() step in not()-traversal in match().");
                        }

                        GraphTraversal notTraversal = notOperator.NotTraversal;
                        GremlinAsOp innerAsOperator = notTraversal.GetOp(1) as GremlinAsOp;
                        if (innerAsOperator == null)
                        {
                            throw new TranslationException("not()-traversal in match() must have a single start label.");
                        }

                        // take the inner first 'as' operator out
                        traversal.InsertGremlinOperator(0, innerAsOperator);
                        notTraversal.RemoveGremlinOperator(1);

                        GremlinAsOp innerEndOperator = notTraversal.GetEndOp() as GremlinAsOp;
                        if (innerEndOperator != null)
                        {
                            List<string> innerEndLabels = innerEndOperator.Labels;

                            if (innerEndLabels.Count > 1)
                            {
                                throw new TranslationException(
                                    "The end operator of a match()-traversal as not()-subtraversal can have at most one label.");
                            }

                            notTraversal.PopGremlinOperator(); // remove the last 'as' step
                            if (innerEndLabels.Count == 1)
                            {
                                notTraversal.Where(Predicate.eq(innerEndLabels[0]));
                            }
                        }
                    }
                    else if (whereTraversalOperator != null)
                    {
                        GraphTraversal whereTraversal = whereTraversalOperator.WhereTraversal;
                        if ((whereTraversal.GetStartOp() as GremlinParentContextOp) != null)
                        {
                            whereTraversal.RemoveGremlinOperator(0);
                        }
                        traversal = whereTraversal;
                    }

                    // --------- Normal Match()-Traversal ---------
                    GremlinAsOp asOperator = traversal.GetStartOp() as GremlinAsOp;

                    if (asOperator == null || asOperator.Labels.Count != 1)
                    {
                        throw new TranslationException("All match()-traversals must have a single start label.");
                    }

                    string startLabel = asOperator.Labels[0], endLabel = null;

                    traversal.RemoveGremlinOperator(0); // remove first 'as' operator
                    configuredTraversal.Select(GremlinKeyword.Pop.Last, startLabel);

                    GremlinAsOp endOperator = traversal.GetEndOp() as GremlinAsOp;
                    if (endOperator != null)
                    {
                        // as('a')...as('b'): both the start and end of the traversal have a declared variable.
                        List<string> endLabels = endOperator.Labels;
                        if (endLabels.Count <= 1)
                        {
                            traversal.PopGremlinOperator();
                            if (endLabels.Count == 1)
                            {
                                endLabel = endLabels[0];

                                if (MatchConnective == Connective.AND)
                                {
                                    configuredTraversal.Choose(
                                        __().Select(GremlinKeyword.Pop.Last, endLabel),
                                        __().Where(Predicate.eq(endLabel)).As(endLabel),
                                        __().As(endLabel)
                                    );
                                }
                                else if (MatchConnective == Connective.OR)
                                {
                                    configuredTraversal.Where(Predicate.eq(endLabel));
                                }
                                else
                                {
                                    throw new TranslationException("MatchConnective should be AND or OR.");
                                }
                            }
                        }
                        else
                        {
                            throw new TranslationException(
                                "The end operator of a match()-traversal can have at most one label.");
                        }
                    }

                    this.StartAndEndLabelsPairList.Add(new Tuple<string, string>(startLabel, endLabel));

                    if (startLabel != null)
                    {
                        this.Labels.Add(startLabel);
                    }

                    if (endLabel != null)
                    {
                        this.Labels.Add(endLabel);
                    }

                    GraphTraversal flatMapTraversal = __();
                    traversal.GremlinTranslationOpList.ForEach(flatMapTraversal.AddGremlinOperator); // construct the flatMapTraversal by appending the operators

                    configuredTraversal.InsertGremlinOperator(2, new GremlinFlatMapOp(flatMapTraversal));

                    return configuredTraversal;
                }

                internal static GraphTraversal JoinMatchTraversals(GraphTraversal headTraversal, List<GraphTraversal> matchTraversals)
                {
                    foreach (GraphTraversal matchTraversal in matchTraversals)
                    {
                        List<GremlinTranslationOperator> opList = matchTraversal.GremlinTranslationOpList.Copy();
                        if ((opList.First() as GremlinParentContextOp) != null)
                        {
                            opList.RemoveAt(0);
                        }
                        foreach (GremlinTranslationOperator op in opList)
                        {
                            headTraversal.AddGremlinOperator(op);
                        }
                    }
                    return headTraversal;
                }

                // similar topological sorting, MatchTraversals and StartAndEndLabelsPairList will be sorted synchronously
                internal void SortMatchTraversals()
                {
                    Dictionary<string, List<string>> edges = new Dictionary<string, List<string>>();
                    // find all the valid edges which have a source vertex and sink vertex
                    foreach (Tuple<string, string> pair in this.StartAndEndLabelsPairList)
                    {
                        if (pair.Item1 != null && pair.Item2 != null)
                        {
                            if (edges.ContainsKey(pair.Item1))
                            {
                                edges[pair.Item1].Add(pair.Item2);
                            }
                            else
                            {
                                edges.Add(pair.Item1, new List<string> {pair.Item2});
                            }
                        }
                    }

                    // We have a graph consisting of many edges storing in 'edges'.
                    // Each time, We will cut the graph to a subgraph and generate a list which is composed of the vertexs in the cut part meanwhile.
                    // And then put this list at the front of the final sorted list which will include all vertexs(labels) finally.
                    // We will not stop until the graph has no vertex.

                    HashSet<string> vertexs = this.Labels.Copy();

                    List<string> totalSortedList = new List<string>();

                    while (vertexs.Count > 0)
                    {
                        List<string> longestPartialSortedList = new List<string>();
                        foreach (string vertex in vertexs)
                        {
                            List<string> partialSortedList = this.BreadthFirstSearch(edges, vertex);
                            if (partialSortedList.Count > longestPartialSortedList.Count)
                            {
                                longestPartialSortedList = partialSortedList;
                            }
                        }
                        // cut it off from the graph
                        foreach (string vertex in longestPartialSortedList)
                        {
                            vertexs.Remove(vertex);
                            foreach (KeyValuePair<string, List<string>> pair in edges)
                            {
                                pair.Value.Remove(vertex);
                            }
                            edges.Remove(vertex);
                        }
                        totalSortedList.InsertRange(0, longestPartialSortedList);
                    }

                    Debug.Assert(this.MatchTraversals.Count == this.StartAndEndLabelsPairList.Count);

                    List<GraphTraversal> sortedMatchTraversals = new List<GraphTraversal>();
                    List<Tuple<string, string>> sortedStartAndEndLabelsPairList = new List<Tuple<string, string>>();

                    foreach (string label in totalSortedList)
                    {
                        for (int i = 0; i < this.MatchTraversals.Count; i++)
                        {
                            string startLabel = this.StartAndEndLabelsPairList[i].Item1;
                            ;
                            if (startLabel == label)
                            {
                                sortedMatchTraversals.Add(this.MatchTraversals[i]);
                                sortedStartAndEndLabelsPairList.Add(this.StartAndEndLabelsPairList[i]);
                            }
                        }
                    }

                    for (int i = 0; i < this.MatchTraversals.Count; i++)
                    {
                        string startLabel = this.StartAndEndLabelsPairList[i].Item1;
                        if (startLabel == null)
                        {
                            sortedMatchTraversals.Add(this.MatchTraversals[i]);
                            sortedStartAndEndLabelsPairList.Add(this.StartAndEndLabelsPairList[i]);
                        }
                    }

                    // could be remove when stable
                    Debug.Assert(this.MatchTraversals.Count == sortedMatchTraversals.Count);
                    Debug.Assert(this.StartAndEndLabelsPairList.Count == sortedStartAndEndLabelsPairList.Count);
                    Debug.Assert(sortedMatchTraversals.TrueForAll(t => this.MatchTraversals.Contains(t)));
                    Debug.Assert(this.MatchTraversals.TrueForAll(t => sortedMatchTraversals.Contains(t)));
                    Debug.Assert(
                        sortedStartAndEndLabelsPairList.TrueForAll(t => this.StartAndEndLabelsPairList.Contains(t)));
                    Debug.Assert(
                        this.StartAndEndLabelsPairList.TrueForAll(t => sortedStartAndEndLabelsPairList.Contains(t)));

                    this.MatchTraversals = sortedMatchTraversals;
                    this.StartAndEndLabelsPairList = sortedStartAndEndLabelsPairList;
                }

                internal List<string> BreadthFirstSearch(Dictionary<string, List<string>> edges, string start)
                {
                    List<string> record = new List<string>();

                    Queue<string> queue = new Queue<string>();
                    queue.Enqueue(start);
                    while (queue.Count > 0)
                    {
                        string current = queue.Dequeue();
                        record.Add(current);

                        if (!edges.ContainsKey(current))
                        {
                            continue; // no next possible vertex
                        }

                        foreach (string nextVertex in edges[current])
                        {
                            if (!record.Contains(nextVertex))
                            {
                                queue.Enqueue(nextVertex);
                            }
                        }
                    }

                    return record;
                }
            }
        }
    }
}