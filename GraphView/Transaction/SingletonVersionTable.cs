
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal abstract class SingletonVersionTable : VersionTable
    {
        public SingletonVersionTable(string tableId) 
            : base(tableId) { }
    }
}
