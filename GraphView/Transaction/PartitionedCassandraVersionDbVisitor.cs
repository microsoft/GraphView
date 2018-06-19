
namespace GraphView.Transaction
{
    using Cassandra;
    internal partial class PartitionedCassandraVersionDbVisitor : VersionDbVisitor
    {
        private CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }
    }

    internal partial class PartitionedCassandraVersionDbVisitor
    {
        internal RowSet CQLExecute(string cql)
        {
            return this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
        }

        internal bool CQLExecuteWithIfApplied(string cql)
        {
            var rs = this.SessionManager.GetSession(PartitionedCassandraVersionDb.DEFAULT_KEYSPACE).Execute(cql);
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            return rse.Current.GetValue<bool>("[applied]");
        }

        //----
        internal Row GetTxTableEntryRow(long txId)
        {
            var rs = this.CQLExecute(string.Format(PartitionedCassandraVersionDb.CQL_GET_TX_TABLE_ENTRY,
                                                    VersionDb.TX_TABLE, txId));
            var rse = rs.GetEnumerator();
            rse.MoveNext();
            Row row = rse.Current;
            return row;
        }

        internal long GetTxTableEntryCommitTs(Row row)
        {
            IsCommitTsOrLB isCommitTsOrLB = (IsCommitTsOrLB)row.GetValue<sbyte>("iscommittsorlb");
            long commitTime = row.GetValue<long>("committime");
            long realCommitTime = isCommitTsOrLB ==
                IsCommitTsOrLB.CommitTs ? commitTime : TxTableEntry.DEFAULT_COMMIT_TIME;

            return realCommitTime;
        }

        internal bool GetTxTableEntry(long txId, TxTableEntry txEntry)
        {
            Row row = GetTxTableEntryRow(txId);
            if (row == null)
            {
                return false;
            }

            IsCommitTsOrLB isCommitTsOrLB = (IsCommitTsOrLB)row.GetValue<sbyte>("iscommittsorlb");
            long commitTime = row.GetValue<long>("committime");
            long realCommitTime = isCommitTsOrLB ==
                IsCommitTsOrLB.CommitTs ? commitTime : TxTableEntry.DEFAULT_COMMIT_TIME;

            txEntry.UpdateValue(
                txId,
                (TxStatus)row.GetValue<sbyte>("status"),
                realCommitTime,
                commitTime);
            return true;
        }
        //----

        internal override void Visit(GetTxEntryRequest req)
        {
            if (this.GetTxTableEntry(req.TxId, req.LocalTxEntry))
            {
                req.Result = req.LocalTxEntry;
            } else
            {
                req.Result = null;
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
                string.Format(PartitionedCassandraVersionDb.CQL_INSERT_NEW_TX,
                                VersionDb.TX_TABLE,
                                req.TxId,
                                (sbyte)TxStatus.Ongoing,     // default status
                                TxTableEntry.DEFAULT_COMMIT_TIME,
                                (sbyte)IsCommitTsOrLB.CommitLowerBound));
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
            bool applied = this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_RECYCLE_TX,
                                                       VersionDb.TX_TABLE,
                                                       (sbyte)TxStatus.Ongoing,     // default status
                                                       TxTableEntry.DEFAULT_COMMIT_TIME,
                                                       (sbyte)IsCommitTsOrLB.CommitLowerBound,
                                                       req.TxId));
            req.Result = applied ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(RemoveTxRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_REMOVE_TX,
                                                       VersionDb.TX_TABLE, req.TxId));
            req.Result = applied ? 1L : 0L;
            req.Finished = true;
        }

        internal override void Visit(SetCommitTsRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME,
                                                                    VersionDb.TX_TABLE,
                                                                    req.ProposedCommitTs,
                                                                    (sbyte)IsCommitTsOrLB.CommitTs,
                                                                    req.TxId));
            if (!applied)
            {
                this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_SET_COMMIT_TIME_SET_FLAG,
                                                        VersionDb.TX_TABLE,
                                                        (sbyte)IsCommitTsOrLB.CommitTs,
                                                        req.TxId,
                                                        (sbyte)IsCommitTsOrLB.CommitLowerBound));
            }

            Row row = this.GetTxTableEntryRow(req.TxId);
            if (row == null)
            {
                req.Result = -1L;
            }
            else
            {
                req.Result = this.GetTxTableEntryCommitTs(row);
            }
            req.Finished = true;
        }

        internal override void Visit(UpdateCommitLowerBoundRequest req)
        {
            this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_COMMIT_LB,
                                                VersionDb.TX_TABLE, req.CommitTsLowerBound, req.TxId, (sbyte)IsCommitTsOrLB.CommitLowerBound));
                        
            Row row = this.GetTxTableEntryRow(req.TxId);
            if (row == null)
            {
                req.Result = -2L;
            }
            else
            {
                req.Result = this.GetTxTableEntryCommitTs(row);
            }
            req.Finished = true;
        }

        internal override void Visit(UpdateTxStatusRequest req)
        {
            bool applied = this.CQLExecuteWithIfApplied(string.Format(PartitionedCassandraVersionDb.CQL_UPDATE_TX_STATUS,
                                                VersionDb.TX_TABLE, (sbyte)req.TxStatus, req.TxId));
            if (applied)
            {
                req.Result = 1L;    // yes
            }
            else
            {
                req.Result = -1L;   // no
            }
            req.Finished = true;
        }
    }
}
