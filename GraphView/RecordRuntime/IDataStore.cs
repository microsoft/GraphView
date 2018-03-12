namespace GraphView.RecordRuntime
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.Transaction;
    using Newtonsoft.Json.Linq;

    public interface IVersionedDataStore
    {
        JObject GetJson(string tableId, object key, Transaction tx);

        IList<JObject> GetRangeJsons(string tableId, object lowerKey, object upperKey, Transaction tx);

        IList<object> GetRecordKeyList(string tableId, object value, Transaction tx);

        IList<object> GetRangeRecordKeyList(string tableId, object lowerValue, object upperValue, Transaction tx);

        bool InsertJson(string tableId, object key, JObject record, Transaction tx);

        bool UpdateJson(string tableId, object key, JObject record, Transaction tx);

        bool DeleteJson(string tableId, object key, Transaction tx);
    }

    public interface IVersionedTableStore
    {
        JObject GetJson(object key, Transaction tx);

        IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx);

        IList<object> GetRecordKeyList(object value, Transaction tx);

        IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx);

        bool InsertJson(object key, JObject record, Transaction tx);

        bool UpdateJson(object key, JObject record, Transaction tx);

        bool DeleteJson(object key, Transaction tx);
    }

    public interface IDataStore
    {
        // Returns a list of base tables in the data store
        IList<string> GetTables();

        // create a table with the given tableId
        bool CreateTable(string tableId);

        // delete a table with the given tableId
        bool DeleteTable(string tableId);

        // Given a base table Id, returns its index tables
        IList<Tuple<string, IndexSpecification>> GetIndexTables(string tableId);
    }
}
