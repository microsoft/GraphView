using TransactionBenchmarkTest.YCSB;

namespace GraphView.Transaction
{
    public enum StoredProcedureType
    {
        YCSBStordProcedure,
        TPCCStordProcedure,
    }

    internal class StoredProcedureFactory
    {
        internal static StoredProcedure CreateStoredProcedure(
            StoredProcedureType type,
            TxResourceManager txResourceManager)
        {
            switch (type)
            {
                case StoredProcedureType.YCSBStordProcedure:
                    return new YCSBStoredProcedure(txResourceManager);
                    break;
            }
            return null;
        }
    }
}
