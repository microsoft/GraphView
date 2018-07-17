
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using NonBlocking;

    internal interface IResource
    {
        void Use();
        bool IsActive();
        void Free();
    }

    internal class ResourcePool<T> where T : IResource
    {
        Queue<T> resourcePool;

        public ResourcePool(int capacity)
        {
            this.resourcePool = new Queue<T>(capacity);
        }

        internal T GetResource()
        {
            int count = 0, size = this.resourcePool.Count;
            while (count < size)
            {
                count++;
                T resource = this.resourcePool.Dequeue();
                this.resourcePool.Enqueue(resource);

                if (!resource.IsActive())
                {
                    // use the resource and return
                    resource.Use();
                    return resource;
                }
            }

            return default(T);
        }

        internal void Recycle(T resource)
        {
            resource.Free();
        }

        internal void AddNewResource(T resource)
        {
            this.resourcePool.Enqueue(resource);
        }
    }

    internal class TxResourceManager
    {
        internal static readonly int workingsetCapacity = 100;
        // It's for the tx version entries during execution
        private ResourcePool<TransactionRequest> transRequests;

        public TxResourceManager()
        {
            this.transRequests = new ResourcePool<TransactionRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.transRequests.AddNewResource(new TransactionRequest());
            }

            // Fill enough entries for transaction execution
            //for (int i = 0; i < 2000000; i++)
            //{
            //    this.versionEntries.Enqueue(new VersionEntry());
            //}
        }

        internal TransactionRequest TransactionRequest(
            string sessionId,
            string tableId,
            object key,
            object value,
            OperationType operationType)
        {
            TransactionRequest transReq = this.transRequests.GetResource();
            if (transReq == null)
            {
                transReq = new TransactionRequest();
                transReq.Use();
                this.transRequests.AddNewResource(transReq);
            }

            transReq.SessionId = sessionId;
            transReq.TableId = tableId;
            transReq.RecordKey = key;
            transReq.Payload = value;
            transReq.OperationType = operationType;

            return transReq;
        }

        internal void RecycleTransRequest(ref TransactionRequest transReq)
        {
            transReq.Free();
            transReq = null;
        }
    }
}
