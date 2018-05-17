using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    public class RedisVersionDbVisitor : VersionDbVisitor
    {
        private readonly RedisConnectionPool clientPool;

        public RedisVersionDbVisitor(RedisConnectionPool clientPool)
        {
            this.clientPool = clientPool;
        }

        public override void Invoke(IEnumerable<TxEntryRequest> reqs)
        {
            foreach (TxEntryRequest req in reqs)
            {
                clientPool.EnqueueTxEntryRequest(req);
            }
            clientPool.Visit();
        }
    }
}
