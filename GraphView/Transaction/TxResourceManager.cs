
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

        // Entry Resource
        private readonly ResourcePool<ReadSetEntry> readSetEntries;
        private readonly ResourcePool<PostProcessingEntry> postprocessingEntries;
        private readonly ResourcePool<WriteSetEntry> writeSetEntries;
        private readonly ResourcePool<VersionKeyEntry> versionKeyEntries;

        private ResourcePool<TransactionRequest> transRequests;

        public TxResourceManager()
        {
            this.readSetEntries = new ResourcePool<ReadSetEntry>(TxResourceManager.workingsetCapacity);
            this.postprocessingEntries = new ResourcePool<PostProcessingEntry>(TxResourceManager.workingsetCapacity);
            this.writeSetEntries = new ResourcePool<WriteSetEntry>(TxResourceManager.workingsetCapacity);
            this.versionKeyEntries = new ResourcePool<VersionKeyEntry>(TxResourceManager.workingsetCapacity);

            this.transRequests = new ResourcePool<TransactionRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.readSetEntries.AddNewResource(new ReadSetEntry());
                this.postprocessingEntries.AddNewResource(new PostProcessingEntry());
                this.writeSetEntries.AddNewResource(new WriteSetEntry());
                this.versionKeyEntries.AddNewResource(new VersionKeyEntry());

                this.transRequests.AddNewResource(new TransactionRequest());
            }
        }

        internal void RecycleTxSetEntry(ref TxSetEntry entry)
        {
            entry.Free();
            entry = null;
        }

        internal ReadSetEntry GetReadSetEntry(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            object record,
            long tailKey)
        {
            ReadSetEntry entry = this.readSetEntries.GetResource();
            if (entry == null)
            {
                entry = new ReadSetEntry();
                entry.Use();
                this.readSetEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;
            entry.BeginTimestamp = beginTimestamp;
            entry.EndTimestamp = endTimestamp;
            entry.TxId = txId;
            entry.Record = record;
            entry.TailKey = tailKey;

            return entry;
        }

        internal PostProcessingEntry GetPostProcessingEntry(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp)
        {
            PostProcessingEntry entry = this.postprocessingEntries.GetResource();
            if (entry == null)
            {
                entry = new PostProcessingEntry();
                entry.Use();
                this.postprocessingEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;
            entry.BeginTimestamp = beginTimestamp;
            entry.EndTimestamp = endTimestamp;

            return entry;
        }

        internal WriteSetEntry GetWriteSetEntry(string tableId, object recordKey, object payload, long versionKey)
        {
            WriteSetEntry entry = this.writeSetEntries.GetResource();
            if (entry == null)
            {
                entry = new WriteSetEntry();
                entry.Use();
                this.writeSetEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.Payload = payload;
            entry.VersionKey = versionKey;
            return entry;
        }

        internal VersionKeyEntry GetVersionKeyEntry(string tableId, object recordKey, long versionKey)
        {
            VersionKeyEntry entry = this.versionKeyEntries.GetResource();
            if (entry == null)
            {
                entry = new VersionKeyEntry();
                entry.Use();
                this.versionKeyEntries.AddNewResource(entry);
            }
            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;

            return entry;
        }

        internal TransactionRequest TransactionRequest(
            string sessionId,
            string tableId,
            object key,
            object value,
            OperationType operationType,
            int recordIntKey)
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
            transReq.RecordIntKey = recordIntKey;

            return transReq;
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
