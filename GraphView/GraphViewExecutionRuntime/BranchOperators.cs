using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    [Serializable]
    internal class OptionalOperator : GraphViewExecutionOperator, ISerializable
    {
        // A list of record fields (identified by field indexes) from the input 
        // operator are to be returned when the optional traversal produces no results.
        // When a field index is less than 0, it means that this field value is always null. 
        private readonly List<int> inputIndexes;
        private readonly GraphViewExecutionOperator inputOp;

        // use this target traversal to determine which input records will have output
        // and put them together into optionalTraversal
        private GraphViewExecutionOperator targetSubQueryOp;
        private Container targetContainer;

        // The traversal inside the optional function. 
        // The records returned by this operator should have the same number of fields
        // as the records produced by the input operator, i.e., inputIndexes.Count 
        private GraphViewExecutionOperator optionalTraversal;
        private Container optionalContainer;

        private HashSet<int> haveOutput;
        private int currentIndex;

        private bool needInitialize;

        public OptionalOperator(
            GraphViewExecutionOperator inputOp,
            List<int> inputIndexes,
            Container targetContainer,
            GraphViewExecutionOperator targetSubQueryOp,
            Container optionalContainer,
            GraphViewExecutionOperator optionalTraversal)
        {
            this.inputOp = inputOp;
            this.inputIndexes = inputIndexes;

            this.targetContainer = targetContainer;
            this.targetSubQueryOp = targetSubQueryOp;

            this.optionalContainer = optionalContainer;
            this.optionalTraversal = optionalTraversal;

            this.currentIndex = 0;
            this.haveOutput = new HashSet<int>();
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

                this.targetContainer.ResetTableCache(inputBuffer);
                this.targetSubQueryOp.ResetState();

                RawRecord rec;
                while (this.targetSubQueryOp.State() && (rec = this.targetSubQueryOp.Next()) != null)
                {
                    this.haveOutput.Add(int.Parse(rec.RetriveData(0).ToValue));
                }

                List<RawRecord> optionalInputs = new List<RawRecord>();
                foreach (RawRecord record in inputBuffer)
                {
                    if (this.haveOutput.Contains(int.Parse(record.RetriveData(0).ToValue)))
                    {
                        optionalInputs.Add(record.GetRange(1));
                    }
                }
                this.optionalContainer.ResetTableCache(optionalInputs);

                this.needInitialize = false;
            }

            while (this.currentIndex < this.targetContainer.Count)
            {
                if (this.haveOutput.Contains(this.currentIndex))
                {
                    RawRecord record;
                    if (this.optionalTraversal.State() && (record = this.optionalTraversal.Next()) != null)
                    {
                        return record;
                    }
                    this.currentIndex++;
                }
                else
                {
                    return this.ConstructForwardingRecord(this.targetContainer[this.currentIndex++].GetRange(1));
                }
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();

            this.targetContainer.Clear();
            this.targetSubQueryOp.ResetState();

            this.optionalContainer.Clear();
            this.optionalTraversal.ResetState();

            this.currentIndex = 0;
            this.haveOutput.Clear();
            this.needInitialize = true;
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "inputIndexes", this.inputIndexes);
            info.AddValue("inputOp", this.inputOp);
            info.AddValue("targetSubQueryOp", this.targetSubQueryOp);
            info.AddValue("optionalTraversal", this.optionalTraversal);
        }

        protected OptionalOperator(SerializationInfo info, StreamingContext context)
        {
            this.inputIndexes = GraphViewSerializer.DeserializeList<int>(info, "inputIndexes");
            this.inputOp = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.targetSubQueryOp = (GraphViewExecutionOperator)info.GetValue("targetSubQueryOp", typeof(GraphViewExecutionOperator));
            this.optionalTraversal = (GraphViewExecutionOperator)info.GetValue("optionalTraversal", typeof(GraphViewExecutionOperator));
            this.currentIndex = 0;
            this.haveOutput = new HashSet<int>();
            this.needInitialize = true;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.targetContainer = new Container();
            EnumeratorOperator enumeratorOp = this.targetSubQueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.targetContainer);

            this.optionalContainer = new Container();
            enumeratorOp = this.optionalTraversal.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.optionalContainer);
        }
    }

    [Serializable]
    internal class UnionOperator : GraphViewExecutionOperator, ISerializable
    {
        // subtraversal operators.
        private List<GraphViewExecutionOperator> traversalList;
        private Container container;

        private GraphViewExecutionOperator inputOp;

        private bool needInitialize;

        public UnionOperator(GraphViewExecutionOperator inputOp, Container container)
        {
            this.inputOp = inputOp;
            this.traversalList = new List<GraphViewExecutionOperator>();
            this.container = container;

            this.needInitialize = true;
            this.Open();
        }

        public void AddTraversal(GraphViewExecutionOperator traversal)
        {
            this.traversalList.Add(traversal);
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                // read inputs
                List<RawRecord> inputBuffer = new List<RawRecord>();
                RawRecord inputRecord;
                while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    inputBuffer.Add(inputRecord);
                }

                foreach (GraphViewExecutionOperator traversal in this.traversalList)
                {
                    traversal.ResetState();
                }
                this.container.ResetTableCache(inputBuffer);
                this.needInitialize = false;
            }

            foreach (GraphViewExecutionOperator traversal in this.traversalList)
            {
                RawRecord result;
                if (traversal.State() && (result = traversal.Next()) != null)
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

            foreach (GraphViewExecutionOperator traversal in this.traversalList)
            {
                traversal.ResetState();
            }

            this.container.Clear();
            this.needInitialize = true;

            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            foreach (GraphViewExecutionOperator traversal in this.traversalList)
            {
                EnumeratorOperator enumeratorOp = traversal.GetFirstOperator() as EnumeratorOperator;
                enumeratorOp.SetContainer(this.container);
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "traversalList", this.traversalList);
            info.AddValue("inputOp", this.inputOp);
        }

        protected UnionOperator(SerializationInfo info, StreamingContext context)
        {
            this.traversalList = GraphViewSerializer.DeserializeList<GraphViewExecutionOperator>(info, "traversalList");
            this.inputOp = (GraphViewExecutionOperator) info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.needInitialize = true;
        }
    }

    [Serializable]
    internal class RepeatOperator : GraphViewExecutionOperator
    {
        private readonly GraphViewExecutionOperator inputOp;

        // Number of times the inner operator repeats itself.
        // If this number is less than 0, the termination condition 
        // is specified by a boolean function. 
        private readonly int repeatTimes;
        [NonSerialized]
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
        private readonly GraphViewExecutionOperator initialOp;
        [NonSerialized]
        private Container initialContainer;

        // loop body
        private readonly GraphViewExecutionOperator repeatTraversalOp;
        [NonSerialized]
        private Container repeatTraversalContainer;

        // After initialization, input records will become repeat rocords,
        // For each loop, this records will be the inputs of repeatTraversalOp(the loop body),
        // and the output of the loop body will replace this, to be new repeat records.
        [NonSerialized]
        private List<RawRecord> repeatRecords;
        [NonSerialized]
        private Queue<RawRecord> repeatResultBuffer;
        [NonSerialized]
        private bool needInitialize;

        public RepeatOperator(
            GraphViewExecutionOperator inputOp,
            Container initialContainer,
            GraphViewExecutionOperator initialOp,
            Container repeatTraversalContainer,
            GraphViewExecutionOperator repeatTraversalOp,
            BooleanFunction emitCondition,
            bool isEmitFront,
            BooleanFunction untilCondition = null,
            bool isUntilFront = false,
            int repeatTimes = -1)
        {
            this.inputOp = inputOp;
            this.initialContainer = initialContainer;
            this.initialOp = initialOp;

            this.repeatTraversalContainer = repeatTraversalContainer;
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
            this.repeatTraversalContainer.ResetTableCache(records);
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
                this.initialContainer.ResetTableCache(inputs);
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
            this.initialContainer.Clear();
            this.initialOp.ResetState();
            this.repeatTraversalContainer.Clear();
            this.repeatTraversalOp.ResetState();
            this.repeatRecords.Clear();
            this.repeatResultBuffer.Clear();
            this.needInitialize = true;
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.currentRepeatTimes = 0;
            this.repeatRecords = new List<RawRecord>();
            this.repeatResultBuffer = new Queue<RawRecord>();
            this.needInitialize = true;

            this.initialContainer = new Container();
            EnumeratorOperator enumeratorOp = this.initialOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.initialContainer);

            this.repeatTraversalContainer = new Container();
            enumeratorOp = this.repeatTraversalOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.repeatTraversalContainer);
        }
    }

    [Serializable]
    internal class ChooseOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        [NonSerialized]
        private Container container;
        private GraphViewExecutionOperator targetSubQueryOp;

        [NonSerialized]
        private Container trueBranchContainer;
        private GraphViewExecutionOperator trueBranchTraversalOp;

        [NonSerialized]
        private Container falseBranchContainer;
        private GraphViewExecutionOperator falseBranchTraversalOp;

        [NonSerialized]
        private List<bool> chooseBranch;
        [NonSerialized]
        private int currentIndex;
        [NonSerialized]
        private bool needInitialize;

        public ChooseOperator(
            GraphViewExecutionOperator inputOp,
            Container container,
            GraphViewExecutionOperator targetSubQueryOp,
            Container trueBranchContainer,
            GraphViewExecutionOperator trueBranchTraversalOp,
            Container falseBranchContainer,
            GraphViewExecutionOperator falseBranchTraversalOp)
        {
            this.inputOp = inputOp;
            this.container = container;
            this.targetSubQueryOp = targetSubQueryOp;

            this.trueBranchContainer = trueBranchContainer;
            this.trueBranchTraversalOp = trueBranchTraversalOp;

            this.falseBranchContainer = falseBranchContainer;
            this.falseBranchTraversalOp = falseBranchTraversalOp;

            this.chooseBranch = new List<bool>();
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
                    this.chooseBranch.Add(false);
                }

                if (!inputBuffer.Any())
                {
                    this.Close();
                    this.needInitialize = false;
                    return null;
                }

                // only send one query
                this.container.ResetTableCache(inputBuffer);
                this.targetSubQueryOp.ResetState();
                RawRecord targetOutput;
                while (this.targetSubQueryOp.State() && (targetOutput = this.targetSubQueryOp.Next()) != null)
                {
                    this.chooseBranch[int.Parse(targetOutput.RetriveData(0).ToValue)] = true;
                }

                // determine which branch should the records apply
                List<RawRecord> trueRawRecords = new List<RawRecord>();
                List<RawRecord> falseRawRecords = new List<RawRecord>();
                foreach (RawRecord record in inputBuffer)
                {
                    if (this.chooseBranch[int.Parse(record.RetriveData(0).ToValue)])
                    {
                        trueRawRecords.Add(record.GetRange(1));
                    }
                    else
                    {
                        falseRawRecords.Add(record.GetRange(1));
                    }
                }


                this.trueBranchContainer.ResetTableCache(trueRawRecords);
                this.trueBranchTraversalOp.ResetState();

                this.falseBranchContainer.ResetTableCache(falseRawRecords);
                this.falseBranchTraversalOp.ResetState();

                this.currentIndex = 0;
                this.needInitialize = false;
            }

            while (this.currentIndex < this.container.Count)
            {
                if (this.chooseBranch[this.currentIndex])
                {
                    RawRecord record;
                    if (this.trueBranchTraversalOp.State() && (record = this.trueBranchTraversalOp.Next()) != null)
                    {
                        return record;
                    }
                    else
                    {
                        this.currentIndex++;
                    }
                }
                else
                {
                    RawRecord record;
                    if (this.falseBranchTraversalOp.State() && (record = this.falseBranchTraversalOp.Next()) != null)
                    {
                        return record;
                    }
                    else
                    {
                        this.currentIndex++;
                    }
                }
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();

            this.container.Clear();
            this.targetSubQueryOp.ResetState();

            this.trueBranchContainer.Clear();
            this.falseBranchContainer.Clear();
            this.trueBranchTraversalOp.ResetState();
            this.falseBranchTraversalOp.ResetState();

            this.chooseBranch.Clear();
            this.needInitialize = true;

            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.chooseBranch = new List<bool>();
            this.needInitialize = true;

            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.targetSubQueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);

            this.trueBranchContainer = new Container();
            enumeratorOp = this.trueBranchTraversalOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.trueBranchContainer);

            this.falseBranchContainer = new Container();
            enumeratorOp = this.falseBranchTraversalOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.falseBranchContainer);
        }
    }

    [Serializable]
    internal class ChooseWithOptionsOperator : GraphViewExecutionOperator, ISerializable
    {
        private readonly GraphViewExecutionOperator inputOp;

        private Container container;
        private readonly GraphViewExecutionOperator targetSubOp;

        private List<Tuple<FieldObject, Container, GraphViewExecutionOperator>> traversalList;

        private Container optionNoneContainer;
        private GraphViewExecutionOperator optionNoneTraversalOp;

        private List<int> selectOption;
        private int currentIndex;
        private Queue<RawRecord> outputBuffer;
        private bool needInitialize;

        public ChooseWithOptionsOperator(
            GraphViewExecutionOperator inputOp,
            Container container,
            GraphViewExecutionOperator targetSubOp
        )
        {
            this.inputOp = inputOp;
            this.container = container;
            this.targetSubOp = targetSubOp;

            this.optionNoneTraversalOp = null;

            this.traversalList = new List<Tuple<FieldObject, Container, GraphViewExecutionOperator>>();

            this.selectOption = new List<int>();
            this.currentIndex = 0;
            this.outputBuffer = new Queue<RawRecord>();
            this.needInitialize = true;

            this.Open();
        }

        public void AddOptionTraversal(ScalarFunction value, Container container, GraphViewExecutionOperator optionTraversalOp)
        {
            if (value == null)
            {
                this.optionNoneTraversalOp = optionTraversalOp;
                this.optionNoneContainer = container;
                return;
            }

            this.traversalList.Add(new Tuple<FieldObject, Container, GraphViewExecutionOperator>(
                value.Evaluate(null),
                container,
                optionTraversalOp));
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                List<RawRecord> inputs = new List<RawRecord>();
                RawRecord inputRecord;
                while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputs.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);
                    inputs.Add(batchRawRecord);
                }

                if (!inputs.Any())
                {
                    this.Close();
                    return null;
                }

                this.container.ResetTableCache(inputs);
                this.targetSubOp.ResetState();

                this.selectOption = Enumerable.Repeat(-1, this.container.Count).ToList();
                int index = 0;
                RawRecord targetRecord;
                while (this.targetSubOp.State() && (targetRecord = this.targetSubOp.Next()) != null)
                {
                    // one input, one output. must one to one
                    Debug.Assert(this.container[index][0].ToValue == targetRecord[0].ToValue,
                        "The provided traversal of choose() does not map to a value.");

                    FieldObject value = targetRecord[1];
                    RawRecord input = this.container[index].GetRange(1);
                    for (int i = 0; i < this.traversalList.Count; i++)
                    {
                        if (this.traversalList[i].Item1.Equals(value))
                        {
                            this.traversalList[i].Item2.Add(input);
                            this.selectOption[index] = i;
                            break;
                        }

                        if (i == this.traversalList.Count - 1 && this.optionNoneTraversalOp != null)
                        {
                            this.optionNoneContainer.Add(input);
                        }
                    }

                    index++;
                }

                this.needInitialize = false;
            }

            if (this.State())
            {
                if (this.outputBuffer.Any())
                {
                    return this.outputBuffer.Dequeue();
                }

                while (this.currentIndex < this.container.Count)
                {
                    int select = this.selectOption[this.currentIndex];
                    if (select != -1)
                    {
                        GraphViewExecutionOperator op = this.traversalList[select].Item3;
                        RawRecord record;
                        while (op.State() && (record = op.Next()) != null)
                        {
                            this.outputBuffer.Enqueue(record);
                        }
                    }
                    else if (this.optionNoneTraversalOp != null)
                    {
                        RawRecord record;
                        while (this.optionNoneTraversalOp.State() && (record = this.optionNoneTraversalOp.Next()) != null)
                        {
                            this.outputBuffer.Enqueue(record);
                        }
                    }

                    this.currentIndex++;

                    if (this.outputBuffer.Any())
                    {
                        return this.outputBuffer.Dequeue();
                    }
                }

                if (!this.outputBuffer.Any())
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
            this.targetSubOp.ResetState();
            this.container.Clear();

            this.optionNoneTraversalOp?.ResetState();
            this.optionNoneContainer?.Clear();

            foreach (Tuple<FieldObject, Container, GraphViewExecutionOperator> tuple in this.traversalList)
            {
                tuple.Item2.Clear();
                tuple.Item3.ResetState();
            }

            this.selectOption.Clear();
            this.currentIndex = 0;
            this.outputBuffer.Clear();
            this.needInitialize = true;

            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.targetSubOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);

            if (this.optionNoneTraversalOp != null)
            {
                this.optionNoneContainer = new Container();
                enumeratorOp = this.optionNoneTraversalOp.GetFirstOperator() as EnumeratorOperator;
                enumeratorOp.SetContainer(this.optionNoneContainer);
            }

            foreach (Tuple<FieldObject, Container, GraphViewExecutionOperator> tuple in this.traversalList)
            {
                enumeratorOp = tuple.Item3.GetFirstOperator() as EnumeratorOperator;
                enumeratorOp.SetContainer(tuple.Item2);
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("inputOp", this.inputOp, typeof(GraphViewExecutionOperator));
            info.AddValue("targetSubOp", this.targetSubOp, typeof(GraphViewExecutionOperator));

            List<FieldObject> traversalListTuple1 = this.traversalList.Select(tuple => tuple.Item1).ToList();
            GraphViewSerializer.SerializeList(info, "traversalListTuple1", traversalListTuple1);
            List<GraphViewExecutionOperator> traversalListTuple3 = this.traversalList.Select(tuple => tuple.Item3).ToList();
            GraphViewSerializer.SerializeList(info, "traversalListTuple3", traversalListTuple3);

            info.AddValue("optionNoneTraversalOp", this.optionNoneTraversalOp, typeof(GraphViewExecutionOperator));
        }

        protected ChooseWithOptionsOperator(SerializationInfo info, StreamingContext context)
        {
            this.inputOp = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.targetSubOp = (GraphViewExecutionOperator)info.GetValue("targetSubOp", typeof(GraphViewExecutionOperator));

            List<FieldObject> traversalListTuple1 = 
                GraphViewSerializer.DeserializeList<FieldObject>(info, "traversalListTuple1");
            List<GraphViewExecutionOperator> traversalListTuple3 = 
                GraphViewSerializer.DeserializeList<GraphViewExecutionOperator>(info, "traversalListTuple3");
            this.traversalList = new List<Tuple<FieldObject, Container, GraphViewExecutionOperator>>();
            Debug.Assert(traversalListTuple1.Count == traversalListTuple3.Count);
            for (int i = 0; i < traversalListTuple1.Count; i++)
            {
                this.traversalList.Add(new Tuple<FieldObject, Container, GraphViewExecutionOperator>(
                    traversalListTuple1[i], new Container(), traversalListTuple3[i]));
            }

            this.optionNoneTraversalOp = (GraphViewExecutionOperator)info.GetValue("optionNoneTraversalOp", typeof(GraphViewExecutionOperator));

            this.selectOption = new List<int>();
            this.currentIndex = 0;
            this.outputBuffer = new Queue<RawRecord>();
            this.needInitialize = true;
        }

    }

    [Serializable]
    internal class QueryDerivedTableOperator : GraphViewExecutionOperator
    {
        protected GraphViewExecutionOperator inputOp;
        protected GraphViewExecutionOperator derivedQueryOp;
        [NonSerialized]
        protected Container container;

        protected int carryOnCount;

        public QueryDerivedTableOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator derivedQueryOp,
            Container container,
            int carryOnCount)
        {
            this.inputOp = inputOp;
            this.derivedQueryOp = derivedQueryOp;
            this.container = container;
            this.carryOnCount = carryOnCount;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.inputOp != null && this.inputOp.State())
            {
                List<RawRecord> inputRecords = new List<RawRecord>();
                RawRecord inputRec;
                while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
                {
                    inputRecords.Add(inputRec);
                }

                this.container.ResetTableCache(inputRecords);
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
            this.container.Clear();

            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            // enumeratorOp maybe is null. for example: g.V().count()
            EnumeratorOperator enumeratorOp = this.derivedQueryOp.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp?.SetContainer(this.container);
        }
    }

    [Serializable]
    internal class QueryDerivedInBatchOperator : QueryDerivedTableOperator
    {
        [NonSerialized]
        private ProjectAggregationInBatch projectAggregationInBatchOp;
        [NonSerialized]
        private SortedDictionary<int, RawRecord> outputBuffer;

        public QueryDerivedInBatchOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator derivedQueryOp,
            Container container,
            ProjectAggregationInBatch projectAggregationInBatchOp,
            int carryOnCount)
            : base(inputOp, derivedQueryOp, container, carryOnCount)
        {
            this.projectAggregationInBatchOp = projectAggregationInBatchOp;
            this.outputBuffer = new SortedDictionary<int, RawRecord>();
        }

        public override RawRecord Next()
        {
            if (this.inputOp != null && this.inputOp.State())
            {
                List<RawRecord> inputRecords = new List<RawRecord>();
                RawRecord inputRec;
                while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
                {
                    inputRecords.Add(inputRec);
                    this.outputBuffer[int.Parse(inputRec[0].ToValue)] = null;
                }

                this.container.ResetTableCache(inputRecords);
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

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.outputBuffer = new SortedDictionary<int, RawRecord>();
            this.projectAggregationInBatchOp = this.derivedQueryOp as ProjectAggregationInBatch;
        }
    }
}
