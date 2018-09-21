
namespace GraphView.Transaction
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// A circular list for representing version lists, whose concurrency follows the pattern
    /// such that only one thread is allowed to append while others are allowed to see the old list. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class CircularVersionList
    {
        private VersionEntry[] internalArray;
        /// <summary>
        /// The higher 4 bytes represent the list size. 
        /// The lower 4 bytes represnet the index of the tail in the array. 
        /// </summary>
        private long metaIndex;

        internal CircularVersionList(int capacity = 8)
        {
            this.internalArray = new VersionEntry[capacity];
            this.metaIndex = 0;
        }

        /// <summary>
        /// Only one thread will enter this method to append a new element to the tail. 
        /// </summary>
        /// <param name="element">The element to append to the list</param>
        /// <returns></returns>
        internal bool AddToTail(VersionEntry element)
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(localMetaIndex >> 32);
            int tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

            if (size + 1 > this.internalArray.Length)
            {
                return false;
            }

            int newTailIndex = (tailIndex + 1) % this.internalArray.Length;
            // By the time Interlocked.Exchange() finishes, the new element is added to the array. 
            // But the meta-index has not been updated. As a result, other reading threads 
            // continue to see an old version of the list, until the meta-index is updated.
            Interlocked.Exchange(ref this.internalArray[newTailIndex], element);
            long newMetaIndex = ((long)size + 1) << 32 | (long)newTailIndex;
            Interlocked.Exchange(ref this.metaIndex, newMetaIndex);

            return true;
        }

        /// <summary>
        /// Only one thread will enter this method to remove the head. 
        /// </summary>
        /// <returns></returns>
        internal VersionEntry RemoveFromHead()
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(localMetaIndex >> 32);
            int tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

            if (size == 0)
            {
                return null;
            }

            int headIndex =
                (tailIndex - size + 1 + this.internalArray.Length) %
                this.internalArray.Length;
            VersionEntry element = this.internalArray[headIndex];

            long newMetaIndex = ((long)size - 1) << 32 | (long)tailIndex;
            // As soon as the meta-index is updated, other threads will see the new head. 
            Interlocked.Exchange(ref this.metaIndex, newMetaIndex);
            
            return element;
        }

        internal void RemoveFromTail(long txId, long targetVersionKey)
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(localMetaIndex >> 32);
            int tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

            if (size == 0)
            {
                return;
            }

            VersionEntry lastVersion = Volatile.Read(ref this.internalArray[tailIndex]);

            long newMetaIndex = ((long)size - 1) << 32 | (long)tailIndex - 1;
            while (lastVersion != null && 
                lastVersion.VersionKey == targetVersionKey && 
                lastVersion.TxId == txId)
            {
                if (Interlocked.CompareExchange(ref this.metaIndex, newMetaIndex, localMetaIndex) != localMetaIndex)
                {
                    localMetaIndex = Interlocked.Read(ref this.metaIndex);
                    size = (int)(localMetaIndex >> 32);
                    tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

                    lastVersion = Volatile.Read(ref this.internalArray[tailIndex]);
                }
            }
        }

        /// <summary>
        /// Peeks last two elements in the list. 
        /// 
        /// Note that since concurrent threads may remove elements from the head or the tail, 
        /// the elements returned by this method may have been removed.
        /// What's more, since removed elements are recycled to accommondate new data, 
        /// it's possible the returned elements do not contain the original data anymore. 
        /// It's the caller's responsibility to check this discrepancy.  
        /// </summary>
        /// <param name="last">The last element</param>
        /// <param name="secondToLast">The second to the last element</param>
        public void PeekTail(out VersionEntry last, out VersionEntry secondToLast)
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(localMetaIndex >> 32);
            int tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

            if (size < 1)
            {
                last = null;
                secondToLast = null;
                return;
            }

            last = Volatile.Read(ref this.internalArray[tailIndex]);

            if (size < 2)
            {
                secondToLast = null;
                return;
            }

            secondToLast = Volatile.Read(ref this.internalArray[tailIndex - 1]);
        }

        public void Clear()
        {
            this.metaIndex = 0;
        }

        internal CircularVersionList Upsize()
        {
            CircularVersionList newList = new CircularVersionList(this.internalArray.Length << 1);
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(localMetaIndex >> 32);
            int tailIndex = (int)(localMetaIndex & 0xFFFFFFFFL);

            if (size == 0)
            {
                return newList;
            }

            int headIndex =
                (tailIndex - size + 1 + this.internalArray.Length) %
                this.internalArray.Length;

            if (tailIndex >= headIndex)
            {
                Array.Copy(this.internalArray, headIndex, newList.internalArray, 0, size);
            }
            else
            {
                Array.Copy(
                    this.internalArray,
                    headIndex,
                    newList.internalArray,
                    0,
                    this.internalArray.Length - headIndex);

                Array.Copy(
                    this.internalArray,
                    0,
                    newList.internalArray,
                    this.internalArray.Length - headIndex,
                    tailIndex + 1);
            }

            newList.metaIndex = (long)size << 32 | (long)(size - 1);

            return newList;
        }

        internal int Count
        {
            get
            {
                long localMetaIndex = Interlocked.Read(ref this.metaIndex);
                int size = (int)(localMetaIndex >> 32);

                return size;
            }
        }
    }

    internal class VersionList : IDictionary<long, VersionEntry>
    {
        internal static int MaxCapacity = 32;

        // The tail key acts as the latch to prevent concurrent modifications of the list,
        // while allowing concurrent reads. 
        private long tailKey;
        private CircularVersionList versionList;

        public VersionList(int capacity = 8)
        {
            this.tailKey = -1;
            this.versionList = new CircularVersionList(capacity < 0 ? VersionList.MaxCapacity : capacity);
        }

        #region IDictionary interfaces
        public VersionEntry this[long key] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public ICollection<long> Keys => throw new NotImplementedException();

        public ICollection<VersionEntry> Values => throw new NotImplementedException();

        public int Count => throw new NotImplementedException();

        public bool IsReadOnly => throw new NotImplementedException();

        public void Add(long key, VersionEntry value)
        {
            this.TryAdd(value, out VersionEntry ve);
        }

        public void Add(KeyValuePair<long, VersionEntry> item)
        {
            this.TryAdd(item.Value, out VersionEntry ve);
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(KeyValuePair<long, VersionEntry> item)
        {
            throw new NotImplementedException();
        }

        public bool ContainsKey(long key)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(KeyValuePair<long, VersionEntry>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<long, VersionEntry>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public bool Remove(long key)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<long, VersionEntry> item)
        {
            throw new NotImplementedException();
        }

        public bool TryGetValue(long key, out VersionEntry value)
        {
            throw new NotImplementedException();
        }
        #endregion

        internal bool TryAdd(VersionEntry versionEntry, out VersionEntry remoteEntry)
        {
            remoteEntry = null;

            long expectedTailKey = versionEntry.VersionKey - 1;

            if (Interlocked.CompareExchange(
                ref this.tailKey, versionEntry.VersionKey, expectedTailKey) != expectedTailKey)
            {
                // Someone else has uploaded a new version and increased the tail key. 
                return false;
            }

            // Up until this point, the current thread has successfully increased the tail key,  
            // and yet the version list is not modified. Other threads will continue to see 
            // the unchanged version list and the old tail key, and as a result always fail 
            // in the above CAS' assertion, when they try to upload a new version. 
            // Hence, the current thread gains exclusive modification ownership of the version list. 

            if (!this.versionList.AddToTail(versionEntry))
            {
                if (this.versionList.Count < VersionList.MaxCapacity)
                {
                    CircularVersionList newVersionList = this.versionList.Upsize();
                    newVersionList.AddToTail(versionEntry);
                    Interlocked.Exchange(ref this.versionList, newVersionList);
                }
                else
                {
                    remoteEntry = this.versionList.RemoveFromHead();
                    this.versionList.AddToTail(versionEntry);
                }
            }

            remoteEntry = remoteEntry ?? new VersionEntry();

            return true;
        }

        internal void TryPeek(out VersionEntry last, out VersionEntry secondToLast)
        {
            this.versionList.PeekTail(out last, out secondToLast);
        }

        internal void TryRemove(long senderTxId, long targetVersionKey)
        {
            this.versionList.RemoveFromTail(senderTxId, targetVersionKey);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
