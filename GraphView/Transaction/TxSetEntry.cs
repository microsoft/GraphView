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
                this.RecordKey.Equals(other.RecordKey);
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

        public class EqualPredicate
        {
            public EqualPredicate()
            {
                // Using method as a function creates new closure object
                // See https://stackoverflow.com/questions/7133013/c-sharp-does-lambda-generate-garbage
                // and https://answers.unity.com/questions/1074493/does-passing-an-action-as-a-parameter-create-garba.html
                // So a cache is used to prevent creating additional closures.
                this.closureCache = this.Call;
            }

            public Func<TxSetEntry, bool>
            Get(string tableId, object recordKey)
            {
                this.tableId = tableId;
                this.recordKey = recordKey;
                return this.closureCache;
            }

            private bool Call(TxSetEntry entry)
            {
                return entry.TableId == this.tableId
                    && entry.RecordKey.Equals(this.recordKey);
            }

            Func<TxSetEntry, bool> closureCache;
            private string tableId;
            private object recordKey;
        }
    }
}
