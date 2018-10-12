namespace GraphView.Transaction
{
    using System;

    /// <summary>
    /// `TxObjPoolList` is a sequential data structure that can reuse the
    /// objects it creaeted by `Push` or `AllocateNew`. This is a desirable
    /// feature when we need minimize the pressure of garbage collection.
    /// </summary>
    /// <typeparam name="T">Element type of the list</typeparam>
    public class TxObjPoolList<T> where T : new()
    {
        private TxList<T> pool;
        private int size;

        public T this[int i]
        {
            get
            {
                if (i < 0 || i >= this.size)
                {
                    throw new ArgumentOutOfRangeException($"invalid index {i} with size {this.size}");
                }
                return pool[i];
            }
        }

        public int Count { get { return this.size; } }

        public const int INIT_POOL_SIZE = 8;

        public TxObjPoolList(int initialPoolSize = INIT_POOL_SIZE)
        {
            this.pool = new TxList<T>();
            this.pool.ResizeAndFill(initialPoolSize);
            this.size = 0;
        }

        /// <summary>
        /// Allocate a new object of type T at the end of the list w.r.t.
        /// `this.Count`
        /// </summary>
        /// <returns>The reference to the newly usable object</returns>
        public T AllocateNew()
        {
            if (this.size == this.pool.Count)
            {
                this.pool.ResizeAndFill(this.size * 2);
            }
            return this[this.size++];
        }

        public T Pop()
        {
            T result = this[this.size - 1];
            --this.size;
            return result;
        }

        public T Find(Func<T, bool> pred)
        {
            for (int i = 0; i < this.Count; ++i)
            {
                if (pred(this[i]))
                {
                    return this[i];
                }
            }
            return default(T);
        }

        public void Clear()
        {
            this.size = 0;
        }
    }
}