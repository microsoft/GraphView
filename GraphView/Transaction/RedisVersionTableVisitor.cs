using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    public class RedisVersionTableVisitor : VersionTableVisitor
    {
        private readonly RedisConnectionPool clientPool;

        public RedisVersionTableVisitor(RedisConnectionPool clientPool)
        {
            this.clientPool = clientPool;
        }

        public override void Invoke(IEnumerable<VersionEntryRequest> reqs)
        {
            foreach (VersionEntryRequest req in reqs)
            {
                clientPool.EnqueueVersionEntryRequest(req);
            }
            clientPool.Visit();
        }
    }
}
