
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    public class VersionTableVisitor : VersionEntryVisitor
    {
        public virtual void Invoke(Queue<VersionEntryRequest> reqQueue)
        {
            while (reqQueue.Count > 0)
            {
                VersionEntryRequest req = reqQueue.Dequeue();
                req.Accept(this);
            }
        }

        public virtual void Invoke(VersionEntryRequest req)
        {
            req.Accept(this);
        }
    }
}
