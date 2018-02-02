

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal class SingletonVersionDb : VersionDb, IVersionedDataStore
    {
        private Dictionary<string, SingletonVersionTable> versionedTables;

        public JObject GetJson(string tableId, object key, Transaction tx)
        {
            if (!versionedTables.ContainsKey(tableId))
            {
                throw new ArgumentException($"Invalid table reference '{tableId}'");
            }

            return versionedTables[tableId].GetJson(key, tx);
        }

        public IList<JObject> GetRangeJsons(string tableId, object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRangeRecordKeyList(string tableId, object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRecordKeyList(string tableId, object value, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }

}
