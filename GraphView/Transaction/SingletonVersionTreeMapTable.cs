
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal partial class SingletonTreeMapVersionTable : VersionTable
    {
        public SingletonTreeMapVersionTable(VersionDb versionDb, string tableId)
            : base(versionDb, tableId)
        {
        }
    }
}
