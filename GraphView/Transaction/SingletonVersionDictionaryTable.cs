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
        public SingletonDictionaryVersionTable(string tableId) : base(tableId)
        {

        }
    }
}
