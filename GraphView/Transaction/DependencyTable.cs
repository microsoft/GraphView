
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    public class DependencyTable
    {
        private Dictionary<long, HashSet<Tuple<long, bool>>> depTable;

        public DependencyTable()
        {
            this.depTable = new Dictionary<long, HashSet<Tuple<long, bool>>>();
        }

        //expectCommit: 'true' means tx1 expect tx2 commit (tx1 can commit only if tx2 commit)
        //'false' means that tx1 expect tx2 abort (tx1 cab commit only if tx2 abort).
        internal void AddCommitDependency(long txId1, long txId2, bool expectCommit)
        {
            if (!this.depTable.ContainsKey(txId1))
            {
                this.depTable[txId1] = new HashSet<Tuple<long, bool>>();
            }

            this.depTable[txId1].Add(new Tuple<long, bool>(txId2, expectCommit));
        }

        internal IEnumerable<Tuple<long, bool>> GetDependencyByTxId(long txId)
        {
            if (!this.depTable.ContainsKey(txId))
            {
                return null;
            }

            return this.depTable[txId];
        }
    }
}
