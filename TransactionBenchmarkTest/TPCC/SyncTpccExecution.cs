using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using GraphView.Transaction;

namespace TransactionBenchmarkTest.TPCC
{
    internal class SyncExecution
    {
        public SyncExecution(TransactionExecution txExec)
        {
            this.txExec = txExec;
        }

        public SyncExecution(VersionDb versionDb, int workerId = 0)
        {
            this.txExec = MakeSimpleExecution(versionDb, workerId);
        }

        static private
        TransactionExecution MakeSimpleExecution(
            VersionDb versionDb, int workerId)
        {
            int visitorId = workerId % versionDb.PartitionCount;
            return new TransactionExecution(
                null, versionDb, null, new TxRange(workerId), visitorId);
        }

        public void Start()
        {
            this.txExec.Reset();
        }

        public SyncExecution Commit()
        {
            this.txExec.Commit();
            return this;
        }

        public SyncExecution Abort()
        {
            this.txExec.Abort();
            return this;
        }

        public SyncExecution Update(TpccTableKey key, TpccTablePayload record)
        {
            this.txExec.Update(key.Table.Name(), key, record);
            return this;
        }

        public SyncExecution Insert(TpccTableKey key, TpccTablePayload record)
        {
            this.txExec.InitAndInsert(key.Table.Name(), key, record);
            return this;
        }

        public bool IsAborted()
        {
            return this.txExec.TxStatus == TxStatus.Aborted;
        }

        public SyncExecution Read<T>(TpccTableKey key, out T record)
            where T : TpccTablePayload
        {
            object payload = this.txExec.SyncRead(key.Table.Name(), key);
            record = payload as T;
            if (!this.IsAborted())
            {
                if (payload == null)
                {
                    return this.Abort();
                }
                Debug.Assert(record != null);
            }
            return this;
        }

        public SyncExecution ReadCopy<T>(TpccTableKey key, out T record)
            where T : TpccTablePayload
        {
            TpccTablePayload payload;
            this.ReadCopyImpl(key, out payload);
            record = payload as T;
            return this;
        }

        protected virtual SyncExecution ReadCopyImpl(
            TpccTableKey key, out TpccTablePayload record)
        {
            return this.Read(key, out record);
        }

        protected TransactionExecution txExec;
    }

    internal class SingletonExecution : SyncExecution
    {
        public SingletonExecution(
            SingletonVersionDb versionDb, int workerId)
            : base(versionDb, workerId)
        {
            this.objectPools =
                SingletonExecution.GetObjectPoolRefs(versionDb, workerId);
        }

        static private CachableObjectPool[]
        GetObjectPoolRefs(SingletonVersionDb versionDb, int workerId)
        {
            var poolRefs = new CachableObjectPool[TpccTable.allTypes.Length];
            foreach (TableType t in TpccTable.AllUsedTypes)
            {
                poolRefs[(int)t] =
                    SingletonExecution.GetLocalObjectPool(
                        versionDb.GetVersionTable(t.Name()), workerId);
            }
            return poolRefs;
        }

        static private CachableObjectPool
            GetLocalObjectPool(VersionTable versionTable, int workerId)
        {
            var visitor = versionTable.GetWorkerLocalVisitor(workerId)
                as SingletonVersionTableVisitor;
            return visitor.recordPool;
        }

        protected override SyncExecution ReadCopyImpl(
            TpccTableKey key, out TpccTablePayload record)
        {
            if (!Read(key, out record).IsAborted())
            {
                record = this.GetObjectPool(
                    key.Table.Type()).GetCopy(record) as TpccTablePayload;
            }
            return this;
        }

        private CachableObjectPool GetObjectPool(TableType type)
        {
            return objectPools[(int)type];
        }

        private SingletonVersionDb versionDb;
        private CachableObjectPool[] objectPools;
    }

    internal class SyncExecutionBuilder
    {
        public SyncExecutionBuilder(VersionDb versionDb)
        {
            this.VersionDb = versionDb;
        }

        public virtual SyncExecution Build(int workerId = 0)
        {
            return new SyncExecution(this.VersionDb, workerId);
        }

        public SyncExecution[] BuildAll()
        {
            SyncExecution[] execs = new SyncExecution[VersionDb.PartitionCount];
            for (int i = 0; i < execs.Length; ++i)
            {
                execs[i] = this.Build(i);
            }
            return execs;
        }

        public VersionDb VersionDb { get; private set; }
    }

    internal class SingletonExecutionBuilder : SyncExecutionBuilder
    {
        public SingletonExecutionBuilder(SingletonVersionDb versionDb)
            : base(versionDb) { }

        public override SyncExecution Build(int workerId = 0)
        {
            return new SingletonExecution(
                this.VersionDb as SingletonVersionDb, workerId);
        }
    }
}
