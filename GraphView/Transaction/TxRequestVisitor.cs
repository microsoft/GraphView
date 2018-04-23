
namespace GraphView.Transaction
{
    internal abstract class TxRequestVisitor
    {
        internal virtual void Visit(TxRequest req) { }
        internal virtual void Visit(DeleteVersionRequest req) { }
        internal virtual void Visit(GetVersionListRequest req) { }
        internal virtual void Visit(GetTxEntryRequest req) { }
        internal virtual void Visit(InitiGetVersionListRequest req) { }
        internal virtual void Visit(InsertTxIdRequest req) { }
        internal virtual void Visit(NewTxIdRequest req) { }
        internal virtual void Visit(ReadVersionRequest req) { }
        internal virtual void Visit(ReplaceVersionRequest req) { }
        internal virtual void Visit(SetCommitTsRequest req) { }
        internal virtual void Visit(UpdateCommitLowerBoundRequest req) { }
        internal virtual void Visit(UpdateTxStatusRequest req) { }
        internal virtual void Visit(UpdateVersionMaxCommitTsRequest req) { }
        internal virtual void Visit(UploadVersionRequest req) { }
    }
}
