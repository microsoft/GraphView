
namespace GraphView.Transaction
{
    class PartitionedCassandraVersionDbVisitor : VersionDbVisitor
    {
        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        internal override void Visit(NewTxIdRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            base.Visit(req);
        }
    }
}
