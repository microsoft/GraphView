
namespace GraphView.Transaction
{
    using System;
    using System.Threading;

    /// <summary>
    /// A circular list for representing version lists, whose concurrency follows the pattern
    /// such that only one thread is allowed to append while others are allowed to see the old list. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class CircularList<T> where T : class
    {
        private T[] internalArray;
        /// <summary>
        /// The higher 4 bytes represent the list size. 
        /// The lower 4 bytes represnet and the index of the tail in the array. 
        /// </summary>
        private long metaIndex;

        public CircularList(int capacity = 8)
        {
            this.internalArray = new T[capacity];
            this.metaIndex = 0;
        }

        /// <summary>
        /// Only one thread will enter this method to append a new element to the tail. 
        /// </summary>
        /// <param name="element">The element to append to the list</param>
        /// <returns></returns>
        public bool AddToTail(T element)
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(this.metaIndex >> 32);
            int tailIndex = (int)(this.metaIndex & 0xFFFFFFFFL);

            if (size + 1 > this.internalArray.Length)
            {
                return false;
            }

            int newTailIndex = (tailIndex + 1) % this.internalArray.Length;
            Interlocked.Exchange(ref this.internalArray[newTailIndex], element);
            // By the time the above instruction finishes, other reading threads still see the old meta-index,
            // hence will not see the new tail.
            long newMetaIndex = ((long)size + 1) << 32 | (long)newTailIndex;
            Interlocked.Exchange(ref this.metaIndex, newMetaIndex);

            return true;
        }

        public T RemoveFromHead()
        {
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(this.metaIndex >> 32);
            int tailIndex = (int)(this.metaIndex & 0xFFFFFFFFL);

            if (size == 0)
            {
                return default(T);
            }

            int headIndex = 
                (tailIndex - size + 1 + this.internalArray.Length) % 
                this.internalArray.Length;

            T element = this.internalArray[headIndex];
            long newMetaIndex = ((long)size - 1) << 32 | (long)tailIndex;
            // Before the meta index is updated, other threads will still see the old head. 
            Interlocked.Exchange(ref this.metaIndex, newMetaIndex);

            return element;
        }

        public void Clear()
        {
            this.metaIndex = 0;
        }

        public CircularList<T> Upsize()
        {
            CircularList<T> newList = new CircularList<T>(this.internalArray.Length << 1);
            long localMetaIndex = Interlocked.Read(ref this.metaIndex);
            int size = (int)(this.metaIndex >> 32);
            int tailIndex = (int)(this.metaIndex & 0xFFFFFFFFL);

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
    }

    internal class VersionList 
    {
        internal static bool readSnapshot = false;

        // The tail key acts as the latch to prevent concurrent modifications of the list,
        // while allowing concurrent reads. 
        private long tailKey;
        private CircularList<VersionEntry> versionList;

        public VersionList(int capacity = 8)
        {
            this.tailKey = -1;
            this.versionList = new CircularList<VersionEntry>();
        }

        internal bool TryAdd(long versionKey, VersionEntry versionEntry, out VersionEntry remoteEntry)
        {
            remoteEntry = null;

            long expectedTailKey = versionKey - 1;

            if (Interlocked.CompareExchange(
                ref this.tailKey, versionKey, expectedTailKey) != expectedTailKey)
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
                if (VersionList.readSnapshot)
                {
                    CircularList<VersionEntry> newVersionList = this.versionList.Upsize();
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
    }
}
