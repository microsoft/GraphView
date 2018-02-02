namespace GraphView.RecordRuntime
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.Transaction;
    using Newtonsoft.Json.Linq;

    internal interface IVersionedDataStore
    {
        JObject GetJson(string tableId, object key, Transaction tx);

        IList<JObject> GetRangeJsons(string tableId, object lowerKey, object upperKey, Transaction tx);

        IList<object> GetRecordKeyList(string tableId, object value, Transaction tx);

        IList<object> GetRangeRecordKeyList(string tableId, object lowerValue, object upperValue, Transaction tx);
    }

    internal interface IVersionedTableStore
    {
        JObject GetJson(object key, Transaction tx);

        IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx);

        IList<object> GetRecordKeyList(object value, Transaction tx);

        IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx);
    }
}
