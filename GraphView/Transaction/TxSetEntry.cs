using System;

namespace GraphView.Transaction
{
    class TxSetEntry : IResource
    {
        internal string TableId;

        internal object RecordKey;

        private bool inUse;

        public TxSetEntry()
        {
            this.inUse = false;
        }

        public TxSetEntry(string tableId, object recordKey)
        {
            this.inUse = false;
            this.TableId = tableId;
            this.RecordKey = recordKey;
        }

        public override bool Equals(object obj)
        {
            
            TxSetEntry other = obj as TxSetEntry;
            if (other == null)
            {
                return false;
            }

            return this.TableId == other.TableId &&
                this.RecordKey == other.RecordKey;
        }

        public void Free()
        {
            this.inUse = false;
        }

        public bool IsActive()
        {
            return this.inUse;
        }

        public void Use()
        {
            this.inUse = true;
        }
    }
}
