
namespace GraphView.Transaction
{
    using System;
    using System.Threading;

    /// <summary>
    /// ResourceQueue implements the Enqueue(), Dequeue() interfaces to allow
    /// multiple threads to enqueue resources and one thread to dequeue them concurrently. 
    /// Enqueue threads are partitioned into separate space, each modeled as an infinite array. 
    /// An enqueue thread always advances the tail and the dequeue thread always advances the head,
    /// eliminating synchronization between dequeue and enqueue. 
    /// The integrity is guaranteed through the invariant: head < tail. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class RequestQueue<T>
    {
        public static readonly int segmentSize = 128;

        private class Segment
        {
            public T[] elements;
            public Segment next;
            public int localHead;
            public int localTail;

            public Segment()
            {
                this.elements = new T[RequestQueue<T>.segmentSize];
                this.next = null;
                this.localHead = 0;
                this.localTail = 0;
            }

            public void Reset()
            {
                this.next = null;
                this.localHead = 0;
                this.localTail = 0;
            }

            public T this[int index]
            {
                get
                {
                    return this.elements[index];
                }
                set
                {
                    this.elements[index] = value;
                }
            }
        }

        private Segment[] headCollection;
        private Segment[] tailCollection;
        private Segment[] freeCollection;
        private int latch;
        private int dequeueIndex;

        public RequestQueue()
        {
            this.headCollection = new Segment[128];
            this.tailCollection = new Segment[128];
            this.freeCollection = new Segment[128];
        }

        private Segment GetNewSegment(int pk)
        {
            if (this.freeCollection[pk] == null)
            {
                return new Segment();
            }
            else
            {
                Segment newSegment = this.freeCollection[pk];
                this.freeCollection[pk] = newSegment.next;
                newSegment.next = null;
                return newSegment;
            }
        }

        private void ShelveFreeSegment(int pk, Segment seg)
        {
            seg.Reset();

            if (this.freeCollection[pk] == null)
            {
                this.freeCollection[pk] = seg;
            }
            else
            {
                seg.next = this.freeCollection[pk];
                this.freeCollection[pk] = seg;
            }
        }

        public void Enqueue(T element, int pk)
        {
            while (pk >= this.tailCollection.Length)
            {
                // # of partitions exceeds allocated capacity, which should be extremely rare.
                if (Interlocked.CompareExchange(ref latch, 1, 0) == 0)
                {
                    int newCapacity = this.tailCollection.Length * 2;

                    Segment[] newTailArray = new Segment[newCapacity];
                    Array.Copy(this.tailCollection, newTailArray, this.tailCollection.Length);

                    Segment[] newHeadArray = new Segment[newCapacity];
                    Array.Copy(this.headCollection, newHeadArray, this.headCollection.Length);

                    Segment[] newFreeArray = new Segment[newCapacity];
                    Array.Copy(this.freeCollection, newFreeArray, this.freeCollection.Length);

                    Interlocked.Exchange(ref this.freeCollection, newFreeArray);
                    Interlocked.Exchange(ref this.tailCollection, newTailArray);
                    Interlocked.Exchange(ref this.headCollection, newHeadArray);

                    Interlocked.Exchange(ref latch, 0);
                }
            }

            if (this.tailCollection[pk] == null)
            {
                Segment segment = this.GetNewSegment(pk);
                this.tailCollection[pk] = segment;
                this.headCollection[pk] = segment;
            }

            Segment tailSegment = this.tailCollection[pk];
            tailSegment[tailSegment.localTail] = element;
            Interlocked.Increment(ref tailSegment.localTail);

            if (tailSegment.localTail == RequestQueue<T>.segmentSize)
            {
                Segment newSegment = this.GetNewSegment(pk);
                Volatile.Write(ref tailSegment.next, newSegment);
                this.tailCollection[pk] = newSegment;
            }
        }

        public bool TryDequeue(out T element)
        {
            int count = 0;
            int capacity = this.tailCollection.Length;
            bool success = true;

            while (!this.TryDequeue(this.dequeueIndex, out element))
            {
                this.dequeueIndex++;
                this.dequeueIndex = this.dequeueIndex >= this.tailCollection.Length ? 0 : this.dequeueIndex;
                count++;

                if (count >= capacity)
                {
                    success = false;
                    break;
                }
            }
            this.dequeueIndex++;
            this.dequeueIndex = this.dequeueIndex >= this.tailCollection.Length ? 0 : this.dequeueIndex;

            return success;
        }

        private bool TryDequeue(int pk, out T element)
        {
            if (pk >= this.headCollection.Length || this.headCollection[pk] == null)
            {
                element = default(T);
                return false;
            }

            Segment headSegment = this.headCollection[pk];
            // The dequeue thread may see an old value of localTail when a new element has been added, 
            // leading to a false positive conclusion that resources are empty but are in fact not. 
            // Since localTail increases monotonically, the integrity is still guaranteed. 
            if (headSegment.localHead < Volatile.Read(ref headSegment.localTail))
            {
                element = headSegment[headSegment.localHead];
                headSegment.localHead++;

                if (headSegment.localHead == RequestQueue<T>.segmentSize)
                {
                    // When the tail and head segments refer to the same entity,
                    // there may be a small window when both the tail and the head 
                    // need to move to the next segment. 
                    // Spinlock until the enqueue thread allocates a new segment. 
                    while (Volatile.Read(ref headSegment.next) == null) ;
                    this.headCollection[pk] = headSegment.next;
                    this.ShelveFreeSegment(pk, headSegment);
                }

                return true;
            }
            else
            {
                element = default(T);
                return false;
            }
        }
    }
}
