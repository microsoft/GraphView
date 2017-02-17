using System;
using System.Collections;
using System.Collections.Generic;

namespace GraphView
{
    internal class ContainerEnumerator
    {
        List<RawRecord> tableCache;
        ContainerOperator containerOp;
        int offset;

        public ContainerEnumerator(List<RawRecord> tableCache, ContainerOperator containerOp)
        {
            offset = -1;
            this.tableCache = tableCache;
            this.containerOp = containerOp;
        }

        public RawRecord Current
        {
            get
            {
                if (offset >= 0 && offset < tableCache.Count)
                {
                    return tableCache[offset];
                }
                else
                {
                    return null;
                }
            }
        }

        public bool MoveNext()
        {
            if (offset + 1 >= tableCache.Count)
            {
                containerOp.Next();
                if (offset + 1 < tableCache.Count)
                {
                    offset++;
                    return true;
                }
                else
                {
                    return false;
                }
            }

            offset++;
            return true;
        }

        public void ResetState()
        {
            offset = -1;
            containerOp.ResetState();
        }

        public void Reset()
        {
            offset = -1;
        }
    }

    internal class ContainerOperator : GraphViewExecutionOperator
    {
        List<RawRecord> tableCache;
        GraphViewExecutionOperator TableInput;

        public ContainerOperator(GraphViewExecutionOperator input)
        {
            TableInput = input;
            tableCache = new List<RawRecord>();
            Open();
        }

        public override RawRecord Next()
        {
            if (TableInput.State())
            {
                RawRecord rec = TableInput.Next();
                if (rec != null)
                {
                    tableCache.Add(rec);
                    return rec;
                }
                else
                {
                    TableInput.Close();
                    return null;
                }
            }

            return tableCache.Count > 0 ? tableCache[tableCache.Count - 1] : null;
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
