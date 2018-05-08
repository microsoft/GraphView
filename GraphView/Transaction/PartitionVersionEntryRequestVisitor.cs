
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    internal class PartitionVersionEntryRequestVisitor : TxRequestVisitor
    {
        private readonly Dictionary<object, Dictionary<long, VersionBlob>> dict;

        internal override void Visit(DeleteVersionRequest req)
        {
            if (dict.ContainsKey(req.RecordKey) && 
                dict[req.RecordKey].ContainsKey(req.VersionKey))
            {
                dict[req.RecordKey].Remove(req.VersionKey);
            }

            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            base.Visit(req);
        }
    }
}
