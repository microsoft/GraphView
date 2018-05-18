using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class PostProcessingEntry
    {
        internal string TableId { get; set; }
        internal object RecordKey { get; set; }
        internal long VersionKey { get; set; }
        internal long BeginTimestamp { get; set; }
        internal long EndTimestamp { get; set; }

        public PostProcessingEntry()
        {

        }

        public PostProcessingEntry(long versionKey, long beginTimestamp, long endTimestamp)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }
    }
}
