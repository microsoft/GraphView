using System.Runtime.CompilerServices;
using System.Threading;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    public class VersionEntry : IComparable<VersionEntry>
    {
        /// <summary>
        /// The offsets of fields in serialized binary data
        /// </summary>
        private static readonly int BEGIN_TIMESTAMP_OFFSET = 0;
        private static readonly int END_TIMESTAMP_OFFSET = 1 * 8;
        private static readonly int TXID_OFFSET = 2 * 8;
        private static readonly int MAX_COMMIT_TS_OFFSET = 3 * 8;
        private static readonly int RECORD_OFFSET = 4 * 8;

        /// <summary>
        /// Default values of the version entry
        /// </summary>
        public static readonly long DEFAULT_BEGIN_TIMESTAMP = -1L;
        public static readonly long DEFAULT_END_TIMESTAMP = -1L;
        public static readonly long DEFAULT_MAX_COMMIT_TS = 0L;

        public static readonly object EMPTY_RECORD = "";
        public static readonly long EMPTY_TXID = -1L;

        public static readonly long VERSION_KEY_START_INDEX = -1L;

        // The following three properties are readonly
        internal long VersionKey;
        internal object Record { get; set; }

        // The following properties may be changed during the lifetime
        // of a tx, after a version entry is created
        internal long BeginTimestamp;
        internal long EndTimestamp;
        internal long TxId;
        internal long MaxCommitTs;

        internal int latch = 0;

        /// <summary>
        /// A circular queue of signals controlling threads who can or cannot proceed.
        /// The capacity of the array is no less than # of threads.
        /// </summary>
        internal bool[] latchQueue = new bool[64];
        internal long ticketCounter;

        private ReaderWriterLockSlim readerWriterLock = new ReaderWriterLockSlim();

        public VersionEntry()
        {
            this.Reset();
        }

        public VersionEntry(
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            object record,
            long txId,
            long maxCommitTs)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.Record = record;
            this.TxId = txId;
            this.MaxCommitTs = maxCommitTs;

            this.ResetLatchQueue();
        }

        public VersionEntry(
            long versionKey,
            object record,
            long txId)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = VersionEntry.DEFAULT_BEGIN_TIMESTAMP;
            this.EndTimestamp = VersionEntry.DEFAULT_END_TIMESTAMP;
            this.Record = record;
            this.TxId = txId;
            this.MaxCommitTs = VersionEntry.DEFAULT_MAX_COMMIT_TS;
        }

        public void ResetLatchQueue()
        {
            for (int tid = 0; tid < this.latchQueue.Length; tid++)
            {
                Volatile.Write(ref this.latchQueue[tid], false);
            }
            Volatile.Write(ref this.latchQueue[0], true);
            Interlocked.Exchange(ref this.ticketCounter, -1);
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.VersionKey.GetHashCode();
            //hash = hash * 23 + this.RecordKey.GetHashCode();

            return hash;
        }

        public static void CopyValue(VersionEntry src, VersionEntry dst)
        {
            dst.VersionKey = src.VersionKey;
            dst.BeginTimestamp = src.BeginTimestamp;
            dst.EndTimestamp = src.EndTimestamp;
            dst.TxId = src.TxId;
            dst.Record = src.Record;
            dst.MaxCommitTs = src.MaxCommitTs;
        }

        public static void CopyFromRemote(VersionEntry src, VersionEntry dst)
        {
            dst.VersionKey = Interlocked.Read(ref src.VersionKey);
            dst.BeginTimestamp = Interlocked.Read(ref src.BeginTimestamp);
            dst.EndTimestamp = Interlocked.Read(ref src.EndTimestamp);
            dst.TxId = Interlocked.Read(ref src.TxId);
            dst.Record = src.Record;
            dst.MaxCommitTs = Interlocked.Read(ref src.MaxCommitTs);
        }

        public void Set(
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            object record,
            long txId,
            long maxCommitTs)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.Record = record;
            this.TxId = txId;
            this.MaxCommitTs = maxCommitTs;
        }

        public void Reset()
        {
            this.VersionKey = VersionEntry.VERSION_KEY_START_INDEX;
            this.BeginTimestamp = VersionEntry.DEFAULT_BEGIN_TIMESTAMP;
            this.EndTimestamp = VersionEntry.DEFAULT_END_TIMESTAMP;
            this.Record = null;
            this.TxId = VersionEntry.EMPTY_TXID;
            this.MaxCommitTs = VersionEntry.DEFAULT_MAX_COMMIT_TS;
        }

        public override bool Equals(object obj)
        {
            VersionEntry ventry = obj as VersionEntry;
            if (ventry == null)
            {
                return false;
            }

            return this.VersionKey == ventry.VersionKey;
        }

        public void Latch()
        {
            while (Interlocked.CompareExchange(ref this.latch, 1, 0) != 0)
                continue;
        }
        
        public void Unlatch()
        {
            Interlocked.Exchange(ref this.latch, 0);
        }

        /// <summary>
        /// Enqueus the current thread by obtaining a ticket/position in the queue. 
        /// Blocks itself if it has not been given a green light.
        /// </summary>
        /// <returns>The ticket/position of the current thread in the queue</returns>
        public int EnterQueuedLatch()
        {
            int ticket = (int)(Interlocked.Increment(ref ticketCounter) % this.latchQueue.Length);
            while (!Volatile.Read(ref this.latchQueue[ticket])) ;
            return ticket;
        }

        /// <summary>
        /// Signals the next ticket/position in the queue.
        /// </summary>
        /// <param name="ticket">The ticket of the current thread</param>
        public void ExitQueuedLatch(int ticket)
        {
            int nextTicket = (ticket + 1) % this.latchQueue.Length;
            Volatile.Write(ref this.latchQueue[nextTicket], true);
            Volatile.Write(ref this.latchQueue[ticket], false);
        }

        public void ReadLock()
        {
            readerWriterLock.EnterReadLock();
        }

        public void UnReadLock()
        {
            readerWriterLock.ExitReadLock();
        }

        public void WriteLock()
        {
            readerWriterLock.EnterWriteLock();
        }

        public void UnWriteLock()
        {
            readerWriterLock.ExitWriteLock();
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
        public static byte[] Serialize(VersionEntry versionEntry)
        {
            List<byte> byteList = new List<byte>();

            byteList.AddRange(BitConverter.GetBytes(versionEntry.BeginTimestamp));
            byteList.AddRange(BitConverter.GetBytes(versionEntry.EndTimestamp));
            byteList.AddRange(BitConverter.GetBytes(versionEntry.TxId));
            byteList.AddRange(BitConverter.GetBytes(versionEntry.MaxCommitTs));
            byteList.AddRange(BytesSerializer.Serialize(versionEntry.Record));

            return byteList.ToArray();
        }

        /// <summary>
        /// Deserialize a version entry by the given recordKey, versionKey and content bytes
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static VersionEntry Deserialize(long versionKey, byte[] bytes, VersionEntry versionEntry = null)
        {
            long beginTimestamp = BitConverter.ToInt64(bytes, VersionEntry.BEGIN_TIMESTAMP_OFFSET);
            long endTimestamp = BitConverter.ToInt64(bytes, VersionEntry.END_TIMESTAMP_OFFSET);
            long txId = BitConverter.ToInt64(bytes, VersionEntry.TXID_OFFSET);
            long maxCommitTs = BitConverter.ToInt64(bytes, VersionEntry.MAX_COMMIT_TS_OFFSET);

            byte[] recordBytes = new byte[bytes.Length - VersionEntry.RECORD_OFFSET];
            Buffer.BlockCopy(bytes, VersionEntry.RECORD_OFFSET, recordBytes, 0, recordBytes.Length);
            object record = BytesSerializer.Deserialize(recordBytes);

            if (versionEntry == null)
            {
                return new VersionEntry(versionKey, beginTimestamp, endTimestamp,
                    record, txId, maxCommitTs);
            }
            else
            {
                versionEntry.Set(versionKey, beginTimestamp, endTimestamp,
                    record, txId, maxCommitTs);
                return versionEntry;
            }
        }

        public static VersionEntry InitEmptyVersionEntry(VersionEntry version = null)
        {
            if (version == null)
            {
                return new VersionEntry(VersionEntry.VERSION_KEY_START_INDEX,
                    VersionEntry.EMPTY_RECORD, VersionEntry.EMPTY_TXID);
            }
            else
            {
                version.Reset();
                return version;
            }
        }

        public static VersionEntry InitFirstVersionEntry(object payload, VersionEntry version = null)
        {
            version = version == null ? new VersionEntry() : version;
            version.Reset();

            version.VersionKey = 0L;
            version.BeginTimestamp = 0L;
            version.EndTimestamp = long.MaxValue;
            version.Record = payload;

            return version;
        }

        public int CompareTo(VersionEntry other)
        {
            if (this.VersionKey < other.VersionKey)
            {
                return -1;
            }
            else if (this.VersionKey == other.VersionKey)
            {
                return 0;
            }
            else
            {
                return 1;
            }
        }
    }
}
