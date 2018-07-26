using TransactionBenchmarkTest.YCSB;

namespace GraphView.Transaction
{
    public enum StoredProcedureType
    {
        YCSBStordProcedure,
        HybridYCSBStordProcedure,
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
                case StoredProcedureType.HybridYCSBStordProcedure:
                    return new HybridYCSBStoredProcedure(txResourceManager);
            }
            return null;
        }
    }
}
