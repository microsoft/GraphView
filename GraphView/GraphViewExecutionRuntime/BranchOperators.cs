using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class OptionalOperator : GraphViewExecutionOperator
    {
        // A list of record fields (identified by field indexes) from the input 
        // operator are to be returned when the optional traversal produces no results.
        // When a field index is less than 0, it means that this field value is always null. 
        private readonly List<int> inputIndexes;

        private readonly GraphViewExecutionOperator inputOp;

        // use this target traversal to determine which input records will have output
        // and put them together into optionalTraversal
        private ContainerEnumerator targetSource;

        private GraphViewExecutionOperator targetSubQueryOp;

        private ContainerEnumerator sourceEnumerator;
        // The traversal inside the optional function. 
        // The records returned by this operator should have the same number of fields
        // as the records produced by the input operator, i.e., inputIndexes.Count 
        private readonly GraphViewExecutionOperator optionalTraversal;

        private List<RawRecord> evaluatedTrueRecords;
        private Queue<RawRecord> evaluatedFalseRecords;

        private bool needInitialize;

        public OptionalOperator(
            GraphViewExecutionOperator inputOp,
            List<int> inputIndexes,
            ContainerEnumerator targetSource,
            GraphViewExecutionOperator targetSubQueryOp,
            ContainerEnumerator sourceEnumerator,
            GraphViewExecutionOperator optionalTraversalOp)
        {
            this.inputOp = inputOp;
            this.inputIndexes = inputIndexes;

            this.targetSource = targetSource;
            this.targetSubQueryOp = targetSubQueryOp;

            this.optionalTraversal = optionalTraversalOp;
            this.sourceEnumerator = sourceEnumerator;

            this.evaluatedTrueRecords = new List<RawRecord>();
            this.evaluatedFalseRecords = new Queue<RawRecord>();

            this.needInitialize = true;

            this.Open();
        }

        private RawRecord ConstructForwardingRecord(RawRecord inputRecord)
        {
            RawRecord rawRecord = new RawRecord(inputRecord);
            foreach (int index in this.inputIndexes)
            {
                if (index < 0)
                {
                    rawRecord.Append((FieldObject)null);
                }
                else
                {
                    rawRecord.Append(inputRecord[index]);
                }
            }

            return rawRecord;
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                // read inputs and set sub-traversal sources
                List<RawRecord> inputBuffer = new List<RawRecord>();
                RawRecord currentRecord = null;
                while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputBuffer.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(currentRecord);
                    inputBuffer.Add(batchRawRecord);
                }

                if (!inputBuffer.Any())
                {
                    this.Close();
                    this.needInitialize = false;
                    return null;
                }

                // only send one query
                this.targetSource.ResetTableCache(inputBuffer);
                this.targetSubQueryOp.ResetState();
                HashSet<int> haveOutput = new HashSet<int>();
                RawRecord targetOutput;
                while (this.targetSubQueryOp.State() && (targetOutput = this.targetSubQueryOp.Next()) != null)
                {
                    haveOutput.Add(int.Parse(targetOutput.RetriveData(0).ToValue));
                }

                // determine which branch should the records apply
                foreach (RawRecord record in inputBuffer)
                {
                    if (haveOutput.Contains(int.Parse(record.RetriveData(0).ToValue)))
                    {
                        this.evaluatedTrueRecords.Add(record.GetRange(1));
                    }
                    else
                    {
                        this.evaluatedFalseRecords.Enqueue(record.GetRange(1));
                    }
                }

                this.sourceEnumerator.ResetTableCache(this.evaluatedTrueRecords);
                this.optionalTraversal.ResetState();

                this.needInitialize = false;
            }

            RawRecord subOutput;
            if (this.optionalTraversal.State() && (subOutput = this.optionalTraversal.Next()) != null)
            {
                return subOutput;
            }

            if (this.evaluatedFalseRecords.Any())
            {
                return this.ConstructForwardingRecord(this.evaluatedFalseRecords.Dequeue());
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.targetSource.Reset();
            this.targetSubQueryOp.ResetState();

            this.sourceEnumerator.ResetState();
            this.optionalTraversal.ResetState();

            this.evaluatedTrueRecords.Clear();
            this.evaluatedFalseRecords.Clear();

            this.needInitialize = true;
            this.Open();
        }
    }

    internal class UnionOperator : GraphViewExecutionOperator
    {
        // traversal Op and hasAggregate flag.
        private List<Tuple<GraphViewExecutionOperator, ContainerEnumerator>> traversalList;


        private List<RawRecord> inputBuffer;
        private GraphViewExecutionOperator inputOp;

        private bool needInitialize;

        public UnionOperator(
            GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;

            this.inputBuffer = new List<RawRecord>();
            this.traversalList = new List<Tuple<GraphViewExecutionOperator, ContainerEnumerator>>();

            this.needInitialize = true;
            this.Open();
        }

        public void AddTraversal(GraphViewExecutionOperator traversal, ContainerEnumerator sourceEnumerator)
        {
            traversalList.Add(new Tuple<GraphViewExecutionOperator, ContainerEnumerator>(traversal, sourceEnumerator));
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                // read inputs
                RawRecord inputRecord;
                while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    this.inputBuffer.Add(inputRecord);
                }

                foreach (Tuple<GraphViewExecutionOperator, ContainerEnumerator> tuple in this.traversalList)
                {
                    tuple.Item1.ResetState();
                    tuple.Item2.ResetTableCache(inputBuffer);
                }

                this.needInitialize = false;
            }

            foreach (Tuple<GraphViewExecutionOperator, ContainerEnumerator> tuple in this.traversalList)
            {
                RawRecord result;
                if (tuple.Item1.State() && (result = tuple.Item1.Next()) != null)
                {
                    return result;
                }
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();

            foreach (Tuple<GraphViewExecutionOperator, ContainerEnumerator> tuple in this.traversalList)
            {
                tuple.Item1.ResetState();
                tuple.Item2.ResetState(); // or reset?
            }

            this.needInitialize = true;

            this.Open();
        }
    }

    internal class RepeatOperator : GraphViewExecutionOperator
    {
        private readonly GraphViewExecutionOperator inputOp;

        // Number of times the inner operator repeats itself.
        // If this number is less than 0, the termination condition 
        // is specified by a boolean function. 
        private readonly int repeatTimes;
        private int currentRepeatTimes;

        // The termination condition of iterations
        private readonly BooleanFunction untilCondition;
        // If this variable is true, the iteration starts with the context record. 
        // This corresponds to the while-do loop semantics. 
        // Otherwise, the iteration starts with the the output of the first execution of the inner operator,
        // which corresponds to the do-while loop semantics.
        // i.e. .until().repeat()
        private readonly bool isUntilFront;

        // The condition determining whether or not an intermediate state is emitted
        private readonly BooleanFunction emitCondition;
        // This variable specifies whether or not the context record is considered 
        // to be emitted when the iteration does not start with the context record,
        // i.e., .emit().repeat()
        private readonly bool isEmitFront;

        // initialOp recieves records from the input operator
        // and extracts needed columns to generate records that are fed as the initial input into the inner operator.
        private readonly ContainerEnumerator initialSource;
        private readonly GraphViewExecutionOperator initialOp;

        // loop body
        private readonly ContainerEnumerator repeatTraversalSource;
        private readonly GraphViewExecutionOperator repeatTraversalOp;

        // After initialization, input records will become repeat rocords,
        // For each loop, this records will be the inputs of repeatTraversalOp(the loop body),
        // and the output of the loop body will replace this, to be new repeat records.
        private List<RawRecord> repeatRecords;

        private readonly Queue<RawRecord> repeatResultBuffer;

        private bool needInitialize;

        public RepeatOperator(
            GraphViewExecutionOperator inputOp,
            ContainerEnumerator initialSource,
            GraphViewExecutionOperator initialOp,
            ContainerEnumerator repeatTraversalSource,
            GraphViewExecutionOperator repeatTraversalOp,
            BooleanFunction emitCondition,
            bool isEmitFront,
            BooleanFunction untilCondition = null,
            bool isUntilFront = false,
            int repeatTimes = -1)
        {
            this.inputOp = inputOp;
            this.initialSource = initialSource;
            this.initialOp = initialOp;

            this.repeatTraversalSource = repeatTraversalSource;
            this.repeatTraversalOp = repeatTraversalOp;

            this.emitCondition = emitCondition;
            this.isEmitFront = isEmitFront;

            this.untilCondition = untilCondition;
            this.isUntilFront = isUntilFront;

            // By current implementation of Gremlin, when repeat time is set to 0,
            // it is reset to 1.
            this.repeatTimes = repeatTimes == 0 ? 1 : repeatTimes;
            this.currentRepeatTimes = 0;

            this.repeatRecords = new List<RawRecord>();
            this.repeatResultBuffer = new Queue<RawRecord>();
            this.needInitialize = true;

            this.Open();
        }

        // Emit to repeatResultBuffer if need;
        private void Emit(List<RawRecord> records)
        {
            if (this.emitCondition == null || (this.repeatTimes != -1 && this.currentRepeatTimes >= this.repeatTimes))
            {
                return;
            }

            List<RawRecord> batchRecords = new List<RawRecord>();
            foreach (RawRecord record in records)
            {
                RawRecord batchRecord = new RawRecord();
                batchRecord.Append(new StringField(batchRecords.Count.ToString(), JsonDataType.Int));
                batchRecord.Append(record);
                batchRecords.Add(batchRecord);
            }

            HashSet<int> haveOutput = this.emitCondition.EvaluateInBatch(batchRecords);
            haveOutput.Select(x => records[x]).ToList().ForEach(this.repeatResultBuffer.Enqueue);
        }

        // for each of the input records, 
        //  if it satisfy the until condition
        //  then enqueue it to repeatResultBuffer
        //  else keep it for next loop
        private List<RawRecord> Until(List<RawRecord> records)
        {
            // when query has no .until() (neither .times())
            if (this.untilCondition == null)
            {
                return records;
            }

            List<RawRecord> batchRecords = new List<RawRecord>();
            foreach (RawRecord record in records)
            {
                RawRecord batchRecord = new RawRecord();
                batchRecord.Append(new StringField(batchRecords.Count.ToString(), JsonDataType.Int));
                batchRecord.Append(record);
                batchRecords.Add(batchRecord);
            }

            HashSet<int> haveOutput = this.untilCondition.EvaluateInBatch(batchRecords);
            haveOutput.Select(x => records[x]).ToList().ForEach(this.repeatResultBuffer.Enqueue);

            List<RawRecord> result = new List<RawRecord>();
            batchRecords.ForEach(x => {
                if (!haveOutput.Contains(int.Parse(x.RetriveData(0).ToValue)))
                {
                    result.Add(x.GetRange(1));
                }
            });
            return result;
        }


        // apply inputs records to repeatTraversal, and return the results of it
        private List<RawRecord> RepeatOnce(List<RawRecord> records)
        {
            this.repeatTraversalSource.ResetTableCache(records);
            this.repeatTraversalOp.ResetState();

            List<RawRecord> result = new List<RawRecord>();
            RawRecord innerOutput;
            while (this.repeatTraversalOp.State() && (innerOutput = this.repeatTraversalOp.Next()) != null)
            {
                result.Add(innerOutput);
            }
            return result;
        }


        private bool ShouldClose()
        {
            if (this.repeatResultBuffer.Any() || (this.repeatTimes == -1 && this.repeatRecords.Any()) || this.currentRepeatTimes < this.repeatTimes)
            {
                return false;
            }
            return true;
        }


        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                RawRecord inputRecord;
                List<RawRecord> inputs = new List<RawRecord>();
                while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    inputs.Add(inputRecord);
                }

                // Project to same length
                this.initialSource.ResetTableCache(inputs);
                while (this.initialOp.State() && (inputRecord = this.initialOp.Next()) != null)
                {
                    this.repeatRecords.Add(inputRecord);
                }

                // invoke Until if needs
                if (this.isUntilFront)
                {
                    // until step will update repeatRecords
                    this.repeatRecords = this.Until(this.repeatRecords);
                }

                // invoke Emit after Until, if there is a emit()
                if (this.isEmitFront)
                {
                    this.Emit(this.repeatRecords);
                }

                this.needInitialize = false;
            }

            while (this.State())
            {
                // only return records from here
                if (this.repeatResultBuffer.Any())
                {
                    return repeatResultBuffer.Dequeue();
                }

                // using .times()
                if (this.repeatTimes > 0)
                {
                    if (this.currentRepeatTimes < this.repeatTimes)
                    {
                        this.repeatRecords = this.RepeatOnce(this.repeatRecords);
                        this.currentRepeatTimes++;
                    }

                    if (this.currentRepeatTimes == this.repeatTimes)
                    {
                        this.repeatRecords.ForEach(this.repeatResultBuffer.Enqueue);
                        this.repeatRecords.Clear();
                        this.currentRepeatTimes++;
                        continue;
                    }
                }
                // using .until()
                else
                {
                    this.repeatRecords = this.RepeatOnce(this.repeatRecords);
                    this.repeatRecords = this.Until(this.repeatRecords);
                }

                this.Emit(this.repeatRecords);
                if (this.ShouldClose())
                {
                    this.Close();
                    break;
                }

            }
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.currentRepeatTimes = 0;
            this.initialSource.Reset();
            this.initialOp.ResetState();
            this.repeatTraversalSource.Reset();
            this.repeatTraversalOp.ResetState();
            this.repeatRecords.Clear();
            this.repeatResultBuffer.Clear();
            this.needInitialize = true;
            this.Open();
        }
    }

    internal class ChooseOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        private ContainerEnumerator targetSource;
        private GraphViewExecutionOperator targetSubQueryOp;

        private ContainerEnumerator trueBranchSource;
        private ContainerEnumerator falseBranchSource;

        private List<RawRecord> evaluatedTrueRecords;
        private List<RawRecord> evaluatedFalseRecords;

        private GraphViewExecutionOperator trueBranchTraversalOp;
        private GraphViewExecutionOperator falseBranchTraversalOp;

        private bool needInitialize;

        public ChooseOperator(
            GraphViewExecutionOperator inputOp,
            ContainerEnumerator targetSource,
            GraphViewExecutionOperator targetSubQueryOp,
            ContainerEnumerator trueBranchSource,
            GraphViewExecutionOperator trueBranchTraversalOp,
            ContainerEnumerator falseBranchSource,
            GraphViewExecutionOperator falseBranchTraversalOp
        )
        {
            this.inputOp = inputOp;
            this.targetSource = targetSource;
            this.targetSubQueryOp = targetSubQueryOp;

            this.trueBranchSource = trueBranchSource;
            this.trueBranchTraversalOp = trueBranchTraversalOp;
            this.falseBranchSource = falseBranchSource;
            this.falseBranchTraversalOp = falseBranchTraversalOp;

            this.evaluatedTrueRecords = new List<RawRecord>();
            this.evaluatedFalseRecords = new List<RawRecord>();

            this.needInitialize = true;
            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                // read inputs and set sub-traversal sources
                List<RawRecord> inputBuffer = new List<RawRecord>();
                RawRecord currentRecord = null;
                while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputBuffer.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(currentRecord);
                    inputBuffer.Add(batchRawRecord);
                }

                if (!inputBuffer.Any())
                {
                    this.Close();
                    this.needInitialize = false;
                    return null;
                }

                // only send one query
                this.targetSource.ResetTableCache(inputBuffer);
                this.targetSubQueryOp.ResetState();
                HashSet<int> haveOutput = new HashSet<int>();
                RawRecord targetOutput;
                while (this.targetSubQueryOp.State() && (targetOutput = this.targetSubQueryOp.Next()) != null)
                {
                    haveOutput.Add(int.Parse(targetOutput.RetriveData(0).ToValue));
                }

                // determine which branch should the records apply
                foreach (RawRecord record in inputBuffer)
                {
                    if (haveOutput.Contains(int.Parse(record.RetriveData(0).ToValue)))
                    {
                        this.evaluatedTrueRecords.Add(record.GetRange(1));
                    }
                    else
                    {
                        this.evaluatedFalseRecords.Add(record.GetRange(1));
                    }
                }


                this.trueBranchSource.ResetTableCache(this.evaluatedTrueRecords);
                this.trueBranchTraversalOp.ResetState();

                this.falseBranchSource.ResetTableCache(this.evaluatedFalseRecords);
                this.falseBranchTraversalOp.ResetState();

                this.needInitialize = false;
            }

            RawRecord trueBranchTraversalRecord;
            if (this.trueBranchTraversalOp.State() && (trueBranchTraversalRecord = this.trueBranchTraversalOp.Next()) != null)
            {
                return trueBranchTraversalRecord;
            }

            RawRecord falseBranchTraversalRecord;
            if (this.falseBranchTraversalOp.State() && (falseBranchTraversalRecord = this.falseBranchTraversalOp.Next()) != null)
            {
                return falseBranchTraversalRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();

            this.targetSource.Reset();
            this.targetSubQueryOp.ResetState();

            this.evaluatedTrueRecords.Clear();
            this.evaluatedFalseRecords.Clear();
            this.trueBranchSource.ResetState();
            this.falseBranchSource.ResetState();
            this.trueBranchTraversalOp.ResetState();
            this.falseBranchTraversalOp.ResetState();

            this.needInitialize = true;

            this.Open();
        }
    }

    internal class ChooseWithOptionsOperator : GraphViewExecutionOperator
    {
        private readonly GraphViewExecutionOperator inputOp;

        private readonly ContainerEnumerator targetSource;
        private readonly GraphViewExecutionOperator targetSubOp;

        private readonly ContainerEnumerator optionSource;

        private int currentOptionTraversalIndex;
        private readonly List<Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator>> traversalList;

        private readonly List<RawRecord> noneRawRecords;
        private GraphViewExecutionOperator optionNoneTraversalOp;

        private readonly Queue<RawRecord> outputBuffer;
        private bool needInitialize;

        public ChooseWithOptionsOperator(
            GraphViewExecutionOperator inputOp,
            ContainerEnumerator targetSource,
            GraphViewExecutionOperator targetSubOp,
            ContainerEnumerator optionSource
        )
        {
            this.inputOp = inputOp;
            this.targetSource = targetSource;
            this.targetSubOp = targetSubOp;
            this.optionSource = optionSource;

            this.noneRawRecords = new List<RawRecord>();
            this.optionNoneTraversalOp = null;

            this.traversalList = new List<Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator>>();
            this.outputBuffer = new Queue<RawRecord>();

            this.currentOptionTraversalIndex = 0;
            this.needInitialize = true;

            this.Open();
        }

        public void AddOptionTraversal(ScalarFunction value, GraphViewExecutionOperator optionTraversalOp)
        {
            if (value == null)
            {
                this.optionNoneTraversalOp = optionTraversalOp;
                return;
            }

            this.traversalList.Add(new Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator>(
                value.Evaluate(null),
                new List<RawRecord>(),
                optionTraversalOp));
        }


        private void ChooseOptionBranch(RawRecord input, FieldObject value)
        {
            foreach (Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator> tuple in this.traversalList)
            {
                if (tuple.Item1.Equals(value))
                {
                    tuple.Item2.Add(input);
                    return;
                }
            }

            if (this.optionNoneTraversalOp != null)
            {
                this.noneRawRecords.Add(input);
            }
        }


        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                Queue<RawRecord> inputs = new Queue<RawRecord>();
                RawRecord inputRecord;
                while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputs.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);
                    inputs.Enqueue(batchRawRecord);
                }

                this.targetSource.ResetTableCache(inputs.ToList());
                this.targetSubOp.ResetState();

                RawRecord targetRecord;
                while (this.targetSubOp.State() && (targetRecord = this.targetSubOp.Next()) != null)
                {
                    Debug.Assert(inputs.Peek().RetriveData(0).ToValue == targetRecord.RetriveData(0).ToValue,
                        "The provided traversal of choose() does not map to a value.");
                    this.ChooseOptionBranch(inputs.Dequeue().GetRange(1), targetRecord[1]);
                }

                Debug.Assert(!inputs.Any(), "The provided traversal of choose() does not map to a value.");

                // put none option branch to the last of traversal list
                if (this.optionNoneTraversalOp != null)
                {
                    this.traversalList.Add(new Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator>(
                        null,
                        this.noneRawRecords,
                        this.optionNoneTraversalOp));
                }

                this.needInitialize = false;
            }

            while (this.State())
            {
                if (this.outputBuffer.Any())
                {
                    return this.outputBuffer.Dequeue();
                }

                if (this.currentOptionTraversalIndex < this.traversalList.Count)
                {
                    this.optionSource.ResetTableCache(this.traversalList[this.currentOptionTraversalIndex].Item2);
                    GraphViewExecutionOperator op = this.traversalList[this.currentOptionTraversalIndex].Item3;
                    RawRecord record;
                    while (op.State() && (record = op.Next()) != null)
                    {
                        this.outputBuffer.Enqueue(record);
                    }
                    this.currentOptionTraversalIndex++;
                }
                else if (!this.outputBuffer.Any())
                {
                    this.Close();
                    return null;
                }
            }
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.targetSource.ResetState();

            this.currentOptionTraversalIndex = 0;

            this.noneRawRecords.Clear();
            this.optionNoneTraversalOp?.ResetState();

            this.optionSource.ResetState();
            foreach (Tuple<FieldObject, List<RawRecord>, GraphViewExecutionOperator> tuple in this.traversalList)
            {
                tuple.Item2.Clear();
                tuple.Item3.ResetState();
            }
            if (this.optionNoneTraversalOp != null)
            {
                this.traversalList.RemoveAt(-1);
            }

            this.outputBuffer.Clear();
            this.needInitialize = true;

            this.Open();
        }
    }

    internal class QueryDerivedTableOperator : GraphViewExecutionOperator
    {
        protected GraphViewExecutionOperator inputOp;
        protected GraphViewExecutionOperator derivedQueryOp;
        protected ContainerEnumerator sourceEnumerator;
        protected List<RawRecord> inputRecords;

        protected int carryOnCount;

        public QueryDerivedTableOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator derivedQueryOp,
            ContainerEnumerator sourceEnumerator,
            int carryOnCount)
        {
            this.inputOp = inputOp;
            this.derivedQueryOp = derivedQueryOp;
            this.inputRecords = new List<RawRecord>();
            this.sourceEnumerator = sourceEnumerator;
            this.carryOnCount = carryOnCount;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.inputOp != null && this.inputOp.State())
            {
                RawRecord inputRec;
                while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
                {
                    this.inputRecords.Add(inputRec);
                }

                this.sourceEnumerator.ResetTableCache(this.inputRecords);
            }

            RawRecord derivedRecord;
            while (this.derivedQueryOp.State() && (derivedRecord = this.derivedQueryOp.Next()) != null)
            {
                RawRecord returnRecord = new RawRecord();
                for (int i = 0; i < this.carryOnCount; i++)
                {
                    returnRecord.Append((FieldObject)null);
                }

                returnRecord.Append(derivedRecord);
                return returnRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp?.ResetState();
            this.derivedQueryOp.ResetState();
            this.inputRecords.Clear();
            this.sourceEnumerator.ResetState();

            this.Open();
        }
    }

    internal class QueryDerivedInBatchOperator : QueryDerivedTableOperator
    {
        private ProjectAggregationInBatch projectAggregationInBatchOp;
        private SortedDictionary<int, RawRecord> outputBuffer;

        public QueryDerivedInBatchOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator derivedQueryOp,
            ContainerEnumerator sourceEnumerator,
            ProjectAggregationInBatch projectAggregationInBatchOp,
            int carryOnCount)
            : base(inputOp, derivedQueryOp, sourceEnumerator, carryOnCount)
        {
            this.projectAggregationInBatchOp = projectAggregationInBatchOp;
            this.outputBuffer = new SortedDictionary<int, RawRecord>();
        }

        public override RawRecord Next()
        {
            if (this.inputOp != null && this.inputOp.State())
            {
                RawRecord inputRec;
                while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
                {
                    this.inputRecords.Add(inputRec);
                    this.outputBuffer[int.Parse(inputRec[0].ToValue)] = null;
                }

                this.sourceEnumerator.ResetTableCache(this.inputRecords);
            }

            RawRecord derivedRecord;
            while (this.derivedQueryOp.State() && (derivedRecord = this.derivedQueryOp.Next()) != null)
            {
                RawRecord returnRecord = new RawRecord();
                returnRecord.Append(derivedRecord[0]);
                for (int i = 1; i < this.carryOnCount; i++)
                {
                    returnRecord.Append((FieldObject)null);
                }
                returnRecord.Append(derivedRecord.GetRange(1));

                this.outputBuffer[int.Parse(derivedRecord[0].ToValue)] = returnRecord;
            }

            foreach (KeyValuePair<int, RawRecord> kvPair in this.outputBuffer)
            {
                int batchId = kvPair.Key;
                RawRecord returnRecord = kvPair.Value;
                // batch index was lost during sub-traversal, but aggregateOp must have output.
                if (returnRecord == null)
                {
                    RawRecord noAccumulateRec = this.projectAggregationInBatchOp.GetNoAccumulateRecord(batchId);
                    returnRecord = new RawRecord();
                    returnRecord.Append(noAccumulateRec[0]);
                    for (int i = 1; i < this.carryOnCount; i++)
                    {
                        returnRecord.Append((FieldObject)null);
                    }
                    returnRecord.Append(noAccumulateRec.GetRange(1));
                }

                outputBuffer.Remove(batchId);

                return returnRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.outputBuffer.Clear();
            base.ResetState();
        }
    }
}
