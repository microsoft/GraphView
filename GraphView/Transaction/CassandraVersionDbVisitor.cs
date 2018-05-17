using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    public class CassandraVersionDbVisitor : VersionDbVisitor
    {
        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        internal override void Visit(GetTxEntryRequest req)
        {
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(NewTxIdRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(RecycleTxRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(RemoveTxRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            base.Visit(req);
        }
    }
}
