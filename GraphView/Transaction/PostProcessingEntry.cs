using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class PostProcessingEntry
    {
        internal long VersionKey { get; private set; }
        internal long BeginTimestamp { get; private set; }
        internal long EndTimestamp { get; private set; }

        public PostProcessingEntry(long versionKey, long beginTimestamp, long endTimestamp)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }
    }
}
