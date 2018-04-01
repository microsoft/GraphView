using System.Runtime.CompilerServices;

namespace GraphView.Transaction
{
    using System.Runtime.Serialization;
    using System;
    using System.Collections.Generic;

    internal class VersionEntry
    {
        internal object RecordKey { get; }
        internal long VersionKey { get; }
        internal long BeginTimestamp { get; }
        internal long EndTimestamp { get; }
        internal object Record { get; }
        internal long TxId { get; }
        internal long MaxCommitTs { get; }


        public VersionEntry(
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            object record,
            long txId,
            long maxCommitTs)
        {
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.Record = record;
            this.TxId = txId;
            this.MaxCommitTs = maxCommitTs;
        }

        public VersionEntry(
            object recordKey,
            long versionKey,
            object record,
            long txId)
        {
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTimestamp = -1L;
            this.EndTimestamp = -1L;
            this.Record = record;
            this.TxId = txId;
            this.MaxCommitTs = 0;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.VersionKey.GetHashCode();
            hash = hash * 23 + this.RecordKey.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            VersionEntry ventry = obj as VersionEntry;
            if (ventry == null)
            {
                return false;
            }

            return this.VersionKey == ventry.VersionKey &&
                this.RecordKey == ventry.RecordKey;
        }

        /// <summary>
        /// Serialize essential properties in version entry to bytes array
        /// </summary>
        /// 
        /// The format of bytes stream is like:
        /// ------- 8 bytes------ ------- 8 bytes---- --- 8 bytes------ 8 bytes----- --X bytes----
        /// [beginTimestamp bytes][endTimestamp bytes][txId bytes][maxCommitTs bytes][record bytes]
        /// 
        /// <returns>a byte array</returns>
        public static byte[] Serialize(long beginTimestamp, long endTimestamp, long txId, long maxCommitTs, object record)
        {
            List<byte> byteList = new List<byte>();

            byteList.AddRange(BitConverter.GetBytes(beginTimestamp));
            byteList.AddRange(BitConverter.GetBytes(endTimestamp));
            byteList.AddRange(BitConverter.GetBytes(txId));
            byteList.AddRange(BitConverter.GetBytes(maxCommitTs));
            byteList.AddRange(BytesSerializer.Serialize(record));

            return byteList.ToArray();
        }

        /// <summary>
        /// Deserialize a version entry by the given recordKey, versionKey and content bytes
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static VersionEntry Deserialize(object recordKey, long versionKey, byte[] bytes)
        {
            long beginTimestamp = BitConverter.ToInt64(bytes, 0);
            long endTimestamp = BitConverter.ToInt64(bytes, 8);
            long txId = BitConverter.ToInt64(bytes, 2*8);
            long maxCommitTs = BitConverter.ToInt64(bytes, 3*8);

            byte[] recordBytes = new byte[bytes.Length - 4*8];
            Buffer.BlockCopy(bytes, 4*8, recordBytes, 0, recordBytes.Length);
            object record = BytesSerializer.Deserialize(recordBytes);

            return new VersionEntry(recordKey, versionKey, beginTimestamp, endTimestamp,
                record, txId, maxCommitTs);
        }
    }
}
