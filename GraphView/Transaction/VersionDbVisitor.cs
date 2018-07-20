
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    public abstract class VersionDbVisitor : TxEntryVisitor
    {
        public virtual void Invoke(Queue<TxEntryRequest> reqQueue)
        {
            while (reqQueue.Count > 0)
            {
                TxEntryRequest req = reqQueue.Dequeue();
                req.Accept(this);
            }
        }

        public virtual void Invoke(TxEntryRequest req)
        {
            req.Accept(this);
        }
    }
}
