
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    public class VersionTableVisitor : VersionEntryVisitor
    {
        public virtual void Invoke(IEnumerable<VersionEntryRequest> reqs)
        {
            foreach (VersionEntryRequest req in reqs)
            {
                req.Accept(this);
            }
        }
    }
}
