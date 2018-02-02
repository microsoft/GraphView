

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal partial class SingletonVersionTreeMap : SingletonVersionTable
    {
    }

    internal partial class SingletonVersionTreeMap : IVersionedTableStore
    {
        public new JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public new IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
