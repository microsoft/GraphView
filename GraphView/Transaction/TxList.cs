namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

    public class TxList<T> where T : new()
    {
        public static readonly int CAPACITY = 8;

        public int Count
        {
            get
            {
                return this.localIndex + 1;
            }
        }

        public bool IsEmpty
        {
            get
            {
                return this.localIndex == -1;
            }
        }

        private T[] entries;

        private int localIndex = -1;

        private int capacity;

        public TxList(int capacity = 0)
        {
            this.capacity = capacity == 0 ? TxList<T>.CAPACITY : capacity;
            this.entries = new T[this.capacity];
        }

        public T this[int i]
        {
            get
            {
                if (i > this.localIndex || i < 0)
                {
                    throw new ArgumentOutOfRangeException("index out of range");
                }
                return this.entries[i];
            }
            set
            {
                if (i > this.localIndex || i < 0)
                {
                    throw new ArgumentOutOfRangeException("index out of range");
                }
                this.entries[i] = value;
            }
        }

        public void Add(T entry)
        {
            int entryIdx = this.localIndex + 1;
            if (entryIdx == this.capacity)
            {
                this.Resize();
            }

            this.entries[entryIdx] = entry;
            this.localIndex++;
        }

        public T PopRight()
        {
            if (this.localIndex == -1)
            {
                throw new ArgumentOutOfRangeException("The list is empty");
            }
            else
            {
                return this.entries[this.localIndex--];
            }
        }

        public bool Contains(T key)
        {
            return this.FindIndex(key) != -1;
        }

        public T Find(T key, int limit = -1)
        {
            int index = this.FindIndex(key, limit);
            if (index == -1)
            {
                return default(T);
            }
            else
            {
                return this.entries[index];
            }
        }

        public int IndexOf(T key)
        {
            return this.FindIndex(key);
        }

        public void Clear()
        {
            this.localIndex = -1;
        }

        /// <summary>
        /// Resize the list and fill the empty space with new T() until
        /// it reaches the resizeCount
        /// </summary>
        /// <param name="resizedCount"></param>
        public void ResizeAndFill(int resizedCount)
        {
            if (resizedCount < 0 || resizedCount < this.Count)
            {
                throw new ArgumentException("resizeCount should be larger than the current Count");
            }

            while (this.Count < resizedCount)
            {
                this.Add(new T());
            }
        }

        private int FindIndex(T key, int limit = -1)
        {
            limit = limit == -1 ? this.localIndex : limit - 1;
            for (int i = 0; i <= limit; i++)
            {
                T entry = this.entries[i];
                if (entry != null && entry.Equals(key))
                {
                    return i;
                }
            }
            return -1;
        }

        private void Resize()
        {
            int newCapacity = this.capacity * 2;
            T[] newEntryArray = new T[newCapacity];
            Array.Copy(this.entries, newEntryArray, this.capacity);

            this.entries = newEntryArray;
            this.capacity = newCapacity;
        }

        /// <summary>
        /// Only sort part of those entries in the list
        /// </summary>
        /// <param name="limit">The count of elements will be sorted</param>
        public void Sort(int limit = -1)
        {
            limit = limit == -1 ? this.Count : limit;
            Array.Sort(this.entries, 0, limit);
        }

        public void Sort(int limit, IComparer<T> comparer) {
            limit = limit == -1 ? this.Count : limit;
            Array.Sort<T>(this.entries, 0, limit, comparer);
        }
    }
}
