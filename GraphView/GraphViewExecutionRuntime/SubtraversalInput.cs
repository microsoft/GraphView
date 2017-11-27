using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// These classes are used as input of subtraversal.
namespace GraphView
{

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

        internal virtual RawRecord this[int index] => tableCache[index];

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
    }

    internal class ContainerWithFlag : Container
    {
        private List<bool> flags;
        public new int Count;

        public ContainerWithFlag()
        {
            this.tableCache = new List<RawRecord>();
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
    }

    internal class EnumeratorOperator : GraphViewExecutionOperator
    {
        public Container Container { get; set; }
        private int offset;

        public EnumeratorOperator()
        {
            this.offset = -1;
            this.Open();
        }

        public EnumeratorOperator(Container container)
        {
            this.Container = container;
            this.offset = -1;
            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.Container != null)
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

        public RawRecord Current => this.Container.GetRawRecord(this.offset, out this.offset);

        public bool MoveNext()
        {
            this.offset++;
            RawRecord rec = this.Container.GetRawRecord(this.offset, out this.offset);
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
    }
}
