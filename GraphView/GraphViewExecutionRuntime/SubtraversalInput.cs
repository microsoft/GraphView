using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

// These classes are used as input of subtraversal.
namespace GraphView
{
    // only serialize in ChooseWithOptionsOperator
    [DataContract]
    [KnownType(typeof(ContainerWithFlag))]
    internal class Container
    {
        protected List<RawRecord> tableCache;
        public int Count => this.tableCache.Count;

        public Container()
        {
            this.tableCache = new List<RawRecord>();
        }

        public virtual RawRecord GetRawRecord(int offset, out int dummy)
        {
            dummy = offset;
            if (offset < 0 || offset >= this.tableCache.Count)
            {
                return null;
            }
            else
            {
                return this.tableCache[offset];
            }
        }

        public virtual void Add(RawRecord record)
        {
            this.tableCache.Add(record);
        }

        internal virtual RawRecord this[int index] => this.tableCache[index];

        public virtual void Clear()
        {
            this.tableCache.Clear();
        }

        public virtual void ResetTableCache(List<RawRecord> records)
        {
            foreach (RawRecord record in records)
            {
                Debug.Assert(record != null);
            }

            this.tableCache = records;
        }

        public virtual void ResetTableCache(RawRecord record)
        {
            Debug.Assert(record != null);

            this.tableCache = new List<RawRecord> { record };
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.tableCache = new List<RawRecord>();
        }
    }

    [DataContract]
    internal class ContainerWithFlag : Container
    {
        private List<bool> flags;
        public new int Count;

        public ContainerWithFlag()
        {
            this.flags = new List<bool>();
            this.Count = 0;
        }

        public override RawRecord GetRawRecord(int offset, out int realOffset)
        {
            if (offset < 0 || offset >= this.tableCache.Count)
            {
                realOffset = offset;
                return null;
            }
            else
            {
                while (offset < this.flags.Count && this.flags[offset] != true)
                {
                    offset++;
                }
                if (offset >= this.flags.Count)
                {
                    realOffset = offset;
                    return null;
                }
                else
                {
                    realOffset = offset;
                    return this.tableCache[offset];
                }
            }
        }

        public override void Add(RawRecord record)
        {
            this.tableCache.Add(record);
            this.flags.Add(true);
            this.Count++;
        }

        public void Delete(List<int> indexs)
        {
            foreach (int index in indexs)
            {
                Debug.Assert(this.flags[index] == true);
                this.flags[index] = false;
                this.Count--;
            }
        }

        internal override RawRecord this[int index]
        {
            get
            {
                if (this.flags[index])
                {
                    return this.tableCache[index];
                }
                else
                {
                    throw new ArgumentException();
                }
            }
        }

        public override void Clear()
        {
            this.tableCache.Clear();
            this.flags.Clear();
            this.Count = 0;
        }

        public override void ResetTableCache(List<RawRecord> records)
        {
            foreach (RawRecord record in records)
            {
                Debug.Assert(record != null);
            }

            this.tableCache = records;
            this.flags = Enumerable.Repeat(true, this.tableCache.Count).ToList();
            this.Count = this.tableCache.Count;
        }

        public override void ResetTableCache(RawRecord record)
        {
            Debug.Assert(record != null);

            this.tableCache = new List<RawRecord> { record };
            this.flags = new List<bool> { true };
            this.Count = this.tableCache.Count;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.flags = new List<bool>();
            this.Count = 0;
        }
    }


    [DataContract]
    internal class EnumeratorOperator : GraphViewExecutionOperator
    {
        private Container container;
        [DataMember]
        private int containerIndex;

        private int offset;

        public EnumeratorOperator()
        {
            this.offset = -1;
            this.containerIndex = -1;
            this.Open();
        }

        public EnumeratorOperator(Container container, int containerIndex)
        {
            this.container = container;
            this.containerIndex = containerIndex;
            this.offset = -1;
            this.Open();
        }

        public void SetContainer(Container aContainer, int aContainerIndex)
        {
            this.container = aContainer;
            this.containerIndex = aContainerIndex;
        }

        public override RawRecord Next()
        {
            if (this.container != null)
            {
                if (this.MoveNext())
                {
                    return this.Current;
                }
                else
                {
                    this.Close();
                    return null;
                }
            }
            else
            {
                if (this.State())
                {
                    this.Close();
                    return new RawRecord();
                }
                else
                {
                    return null;
                }
            }
        }

        public RawRecord Current => this.container.GetRawRecord(this.offset, out this.offset);

        public bool MoveNext()
        {
            this.offset++;
            RawRecord rec = this.container.GetRawRecord(this.offset, out this.offset);
            if (rec != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override void ResetState()
        {
            this.offset = -1;
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.offset = -1;
            if (this.containerIndex != -1)
            {
                this.container = SerializationData.Containers[this.containerIndex];
            }
        }
    }
}
