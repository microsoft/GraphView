

using System.Runtime.InteropServices.WindowsRuntime;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal partial class SingletonVersionDb : VersionDb
    {

    }

    internal partial class SingletonVersionDb
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
