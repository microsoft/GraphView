
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal abstract class SingletonVersionTable : VersionTable, IVersionedTableStore
    {
        public SingletonVersionTable(string tableId) 
            : base(tableId) { }

        public JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool UpdateJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool DeleteJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
