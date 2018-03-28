namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A version table implementation in single machine environment.
    /// Use a Dictionary.
    /// </summary>
    internal partial class SingletonDictionaryVersionTable : VersionTable
    {
        private readonly Dictionary<object, VersionList> dict;
        private readonly object listlock;


        public SingletonDictionaryVersionTable(string tableId) : base(tableId)
        {
            this.dict = new Dictionary<object, VersionList>();
            this.listlock = new object();
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            if (!this.dict.ContainsKey(recordKey))
            {
                return null;
            }

            List<VersionEntry> versionList = new List<VersionEntry>();
            while (true)
            {
                VersionNode current = this.dict[recordKey].Head;
                versionList.Clear();
                do
                {
                    if (current.NextNode == null)
                    {
                        //arrive at the end of the list, return the version list
                        return versionList;
                    }

                    if ((current.State & 0x0F).Equals(0x0F))
                    {
                        //if current node is being deleted, rescan the list from head.
                        break;
                    }

                    versionList.Add(current.VersionEntry);
                    current = current.NextNode;
                } while (true);
            }
        }

    }
}
