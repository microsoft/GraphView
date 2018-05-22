using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    using Cassandra;
    public partial class CassandraVersionDbVisitor : VersionDbVisitor
    {
        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }
    }

    public partial class CassandraVersionDbVisitor : VersionDbVisitor {
        internal RowSet CQLExecute(string cql)
        {
            return this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
        }
        
        internal bool CQLExecuteWithIfApplied(string cql)
        {
            var rs = this.SessionManager.GetSession(CassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            return rse.Current.GetValue<bool>("[applied]");
        }

        internal override void Visit(GetTxEntryRequest req)
        {
            var rs = this.CQLExecute(string.Format(CassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                    VersionDb.TX_TABLE, req.TxId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            if (row == null)
            {
                req.Result = null;
            } else
            {
                req.Result = new TxTableEntry(
                    req.TxId,
                    (TxStatus)row.GetValue<sbyte>("status"),
                    row.GetValue<long>("committime"),
                    row.GetValue<long>("commitlowerbound"));
            }

            req.Finished = true;
        }

        internal override void Visit(InsertTxIdRequest req)
        {
            // because we do the work in `NewTxIdRequest`
            req.Finished = true;
        }

        // hold the place if `req.TxId` not exists
        // we assume txId conflicts rarely,
        // otherwise, it is the cause of txId generator
        // return: 0L-conflict, 1L-ok
        internal override void Visit(NewTxIdRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(
                string.Format(CassandraVersionDb.CQL_INSERT_NEW_TX,
                                VersionDb.TX_TABLE,
                                req.TxId,
                                (sbyte)TxStatus.Ongoing,     // default status
                                TxTableEntry.DEFAULT_COMMIT_TIME,
                                TxTableEntry.DEFAULT_LOWER_BOUND));
            if (applied)
            {
                req.Result = 1L;
            }
            else
            {
                // txId exists
                req.Result = 0L;
            }
            req.Finished = true;
        }

        internal override void Visit(RecycleTxRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_RECYCLE_TX,
                                                       VersionDb.TX_TABLE,
                                                       (sbyte)TxStatus.Ongoing,     // default status
                                                       TxTableEntry.DEFAULT_COMMIT_TIME,
                                                       TxTableEntry.DEFAULT_LOWER_BOUND,
                                                       req.TxId));
            req.Result = applied ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_REMOVE_TX,
                                                       VersionDb.TX_TABLE, req.TxId));
            req.Result = applied ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_SET_COMMIT_TIME,
                                                VersionDb.TX_TABLE,
                                                req.ProposedCommitTs,
                                                req.TxId));
            var rs = this.CQLExecute(string.Format(CassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                   VersionDb.TX_TABLE, req.TxId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            if (row == null)
            {
                req.Result = -1L;
            } else
            {
                req.Result = row.GetValue<long>("committime");
            }
            req.Finished = true;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_UPDATE_COMMIT_LB,
                                                VersionDb.TX_TABLE, req.CommitTsLowerBound, req.TxId));

            var rs = this.CQLExecute(string.Format(CassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                   VersionDb.TX_TABLE, req.TxId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            if (row == null)
            {
                req.Result = -2L;
            } else
            {
                req.Result = row.GetValue<long>("committime");
            }
            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(CassandraVersionDb.CQL_UPDATE_TX_STATUS,
                                                VersionDb.TX_TABLE, (sbyte)req.TxStatus, req.TxId));
            if (applied)
            {
                req.Result = 1L;    // yes
            } else
            {
                req.Result = -1L;   // no
            }
            req.Finished = true;
        }
    }
}
