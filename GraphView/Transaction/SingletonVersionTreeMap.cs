
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
        public SingletonVersionTreeMap(string tableId) 
            : base(tableId) { }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            return base.GetVersionList(recordKey);
        }

        internal override void InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            base.InsertAndUploadVersion(recordKey, version);
        }

        internal override bool UpdateAndUploadVersion(object recordKey, long versionKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            return base.UpdateAndUploadVersion(recordKey, versionKey, oldVersion, newVersion);
        }

        internal override void DeleteVersionEntry(object recordKey, long versionKey)
        {
            base.DeleteVersionEntry(recordKey, versionKey);
        }
    }

    internal partial class SingletonVersionTreeMap : SingletonVersionTable
    {
        public override bool DeleteJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
