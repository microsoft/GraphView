using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    public class CassandraVersionTableVisitor : VersionTableVisitor
    {
        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(GetVersionListRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(ReadVersionRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(ReplaceWholeVersionRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            base.Visit(req);
        }

        internal override void Visit(UploadVersionRequest req)
        {
            base.Visit(req);
        }
    }
}
