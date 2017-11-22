using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// These classes are used as input of subtraversal.
namespace GraphView
{
    internal class ConstantSourceOperator : GraphViewExecutionOperator
    {
        private RawRecord _constantSource;
        ContainerEnumerator sourceEnumerator;

        public RawRecord ConstantSource
        {
            get { return _constantSource; }
            set { _constantSource = value; this.Open(); }
        }

        public ContainerEnumerator SourceEnumerator
        {
            get { return sourceEnumerator; }
            set
            {
                sourceEnumerator = value;
                Open();
            }
        }

        public ConstantSourceOperator()
        {
            Open();
        }

        public override RawRecord Next()
        {
            if (sourceEnumerator != null)
            {
                if (sourceEnumerator.MoveNext())
                {
                    return sourceEnumerator.Current;
                }
                else
                {
                    Close();
                    return null;
                }
            }
            else
            {
                if (!State())
                    return null;

                Close();
                return _constantSource;
            }
        }

        public override void ResetState()
        {
            if (sourceEnumerator != null)
            {
                sourceEnumerator.Reset();
                Open();
            }
            else
            {
                Open();
            }
        }
    }

    internal class ContainerEnumerator
    {
        private List<RawRecord> tableCache;
        private ContainerOperator containerOp;
        private int offset;

        public ContainerEnumerator(List<RawRecord> tableCache, ContainerOperator containerOp)
        {
            this.offset = -1;
            this.tableCache = tableCache;
            this.containerOp = containerOp;
        }

        public ContainerEnumerator()
        {
            this.offset = -1;
        }

        public void ResetTableCache(List<RawRecord> tableCache)
        {
            this.offset = -1;
            this.tableCache = tableCache;
        }

        public RawRecord Current
        {
            get
            {
                if (this.offset >= 0 && this.offset < this.tableCache.Count)
                {
                    return this.tableCache[this.offset];
                }
                else
                {
                    return null;
                }
            }
        }

        public bool MoveNext()
        {
            if (this.offset + 1 >= this.tableCache.Count)
            {
                this.containerOp?.Next();
                if (this.offset + 1 < this.tableCache.Count)
                {
                    this.offset++;
                    return true;
                }
                else
                {
                    return false;
                }
            }

            this.offset++;
            return true;
        }

        public void ResetState()
        {
            this.offset = -1;
            this.tableCache?.Clear();
            this.containerOp?.ResetState();
        }

        public void Reset()
        {
            this.offset = -1;
        }
    }

    internal class ContainerOperator : GraphViewExecutionOperator
    {
        List<RawRecord> tableCache;
        GraphViewExecutionOperator TableInput;

        public ContainerOperator(GraphViewExecutionOperator input)
        {
            this.TableInput = input;
            this.tableCache = new List<RawRecord>();
            this.Open();
        }

        public override RawRecord Next()
        {
            if (this.TableInput.State())
            {
                RawRecord rec = this.TableInput.Next();
                if (rec != null)
                {
                    this.tableCache.Add(rec);
                }
                else
                {
                    this.TableInput.Close();
                    return null;
                }
            }

            return this.tableCache.Count > 0 ? this.tableCache[this.tableCache.Count - 1] : null;
        }

        public ContainerEnumerator GetEnumerator()
        {
            return new ContainerEnumerator(tableCache, this);
        }

        public override void ResetState()
        {
            Open();
            tableCache.Clear();
            TableInput.ResetState();
        }
    }

    internal class ContainerScanOperator : GraphViewExecutionOperator
    {
        ContainerEnumerator containerEnumerator;

        public ContainerScanOperator(ContainerEnumerator enumerator)
        {
            containerEnumerator = enumerator;
        }

        public override RawRecord Next()
        {
            if (containerEnumerator.MoveNext())
            {
                return containerEnumerator.Current;
            }
            else
            {
                return null;
            }
        }
    }
}
