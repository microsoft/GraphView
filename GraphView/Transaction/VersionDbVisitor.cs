
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    public abstract class VersionDbVisitor : TxEntryVisitor
    {
        public virtual void Invoke(IEnumerable<TxEntryRequest> reqs)
        {
            foreach (TxEntryRequest req in reqs)
            {
                req.Accept(this);
            }
        }

        public virtual void Invoke(TxEntryRequest req)
        {
            req.Accept(this);
        }
    }
}
