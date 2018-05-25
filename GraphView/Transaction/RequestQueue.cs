
namespace GraphView.Transaction
{
    using System.Collections.Generic;
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
    public class RequestQueue<T>
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

        /// <summary>
        /// Each partition maintains a queue/list of free segments to be re-used.
        /// Recycling a segment appends it to the tail, 
        /// while obtaining a segment detaches it from the head.
        /// </summary>
        private Segment[] freeHeadCollection;
        private Segment[] freeTailCollection;

        private int latch;
        private int partitionIndex;

        public RequestQueue(int partitionCount)
        {
            this.headCollection = new Segment[partitionCount];
            this.tailCollection = new Segment[partitionCount];
            this.freeHeadCollection = new Segment[partitionCount];
            this.freeTailCollection = new Segment[partitionCount];

            for (int pk = 0; pk < this.headCollection.Length; pk++)
            {
                Segment firstSegment = new Segment();
                this.headCollection[pk] = firstSegment;
                this.tailCollection[pk] = firstSegment;

                Segment firstFreeSegment = new Segment();
                this.freeHeadCollection[pk] = firstFreeSegment;
                this.freeTailCollection[pk] = firstFreeSegment;
            }
        }

        private Segment GetNewSegment(int pk)
        {
            // At any time, the free segment list contains at least one segment, 
            // so as to create a barrier to isolate enqueue and dequeue threads. 
            // The enqueue thread appends to the tail by updating its next pointer, 
            // while the dequeue thread removes the head and advances the head pointer its follower.
            // Having at least one segment guarantees that the two threads modify no common data and 
            // therefore need no synchronization at all.  
            if (this.freeHeadCollection[pk].next == null)
            {
                // The free list has only one segment. 
                // This segment cannot be re-used until a new free segment is appended to the tail.  
                return new Segment();
            }
            else
            {
                Segment newSegment = this.freeHeadCollection[pk];
                this.freeHeadCollection[pk] = newSegment.next;
                newSegment.next = null;
                return newSegment;
            }
        }

        private void ShelveFreeSegment(int pk, Segment seg)
        {
            seg.Reset();

            this.freeTailCollection[pk].next = seg;
            this.freeTailCollection[pk] = seg;
        }

        public void Enqueue(T element, int pk)
        {
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

            while (!this.TryDequeue(this.partitionIndex, out element))
            {
                this.partitionIndex++;
                this.partitionIndex = this.partitionIndex >= this.tailCollection.Length ? 0 : this.partitionIndex;
                count++;

                if (count >= capacity)
                {
                    success = false;
                    break;
                }
            }
            this.partitionIndex++;
            this.partitionIndex = this.partitionIndex >= this.tailCollection.Length ? 0 : this.partitionIndex;

            return success;
        }

        public void Dequeue(Queue<T> outputQueue, int expectedCount)
        {
            int elementCount = 0;
            int partitionCount = 0;

            while (elementCount < expectedCount && partitionCount < this.tailCollection.Length)
            {
                T element = default(T);

                if (this.TryDequeue(this.partitionIndex, out element))
                {
                    outputQueue.Enqueue(element);
                    elementCount++;
                }
                else
                {
                    this.partitionIndex++;
                    this.partitionIndex = this.partitionIndex >= this.tailCollection.Length ? 0 : this.partitionIndex;

                    partitionCount++;
                }
            }
        }

        internal bool TryDequeue(int pk, out T element)
        {
            if (pk >= this.headCollection.Length)
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

        internal int FreeSegmentCount()
        {
            int count = 0;

            for (int pk = 0; pk < this.freeHeadCollection.Length; pk++)
            {
                Segment seg = this.freeHeadCollection[pk];
                while (seg.next != null)
                {
                    count++;
                    seg = seg.next;
                }
            }

            return count;
        }
    }
}
