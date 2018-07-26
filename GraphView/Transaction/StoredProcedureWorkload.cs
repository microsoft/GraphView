using System;

namespace GraphView.Transaction
{
    public class StoredProcedureWorkload
    {
        public static Action<StoredProcedureWorkload> Reload { get; set; }

        public StoredProcedureWorkload()
        {

        }

        public virtual void Set(StoredProcedureWorkload workload) { }
    }
}
