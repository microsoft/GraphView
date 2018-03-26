namespace GraphView.Transaction
{
    using Cassandra;
    using RecordRuntime;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// In cassandraVersionDb, we assume a cassandra table associates with a GraphView
    /// table, which takes (recordKey, versionKey) as the primary keys. A cassandra 
    /// keyspace associcates with a cassandraVersionDb. Every table name is in
    /// the format of 'keyspace.tablename', such as 'UserDb.Profile'
    /// </summary>
    internal partial class CassandraVersionDb : VersionDb
    {

    }

    internal partial class CassandraVersionDb
    {
        public override TxStatus GetTxStatusByTxId(long txId)
        {
            throw new NotImplementedException();
        }

        public override void InsertNewTx(long txId)
        {
            throw new NotImplementedException();
        }

        public override long SetCommitTime(long txId, long lowerBound, long upperBound)
        {
            throw new NotImplementedException();
        }

        public override bool SetMaxCommitLowerBound(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }

        public override void UpdateTxStatus(long txId)
        {
            throw new NotImplementedException();
        }

        public override long GetCommitTime(long txId)
        {
            throw new NotImplementedException();
        }
    }
}
