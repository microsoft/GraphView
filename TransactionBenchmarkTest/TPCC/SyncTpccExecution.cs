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
            return new TransactionExecution(
                null, versionDb, null, new TxRange(workerId), workerId);
        }

        public void Start()
        {
            this.txExec.Reset();
        }

        public void Commit()
        {
            this.txExec.Commit();
        }

        public void Abort()
        {
            this.txExec.Abort();
        }

        public
        bool Read(TpccTableKey key, out TpccTablePayload record)
        {
            record = this.txExec.SyncRead(
                key.Table.Name(), key) as TpccTablePayload;
            return this.IsAborted();
        }

        public
        bool Update(TpccTableKey key, TpccTablePayload record)
        {
            this.txExec.Update(key.Table.Name(), key, record);
            return this.IsAborted();
        }

        public
        bool Insert(TpccTableKey key, TpccTablePayload record)
        {
            this.txExec.InitAndInsert(key.Table.Name(), key, record);
            return this.IsAborted();
        }

        public bool IsAborted()
        {
            return this.txExec.TxStatus == TxStatus.Aborted;
        }

        public bool ReadAs<T>(TpccTableKey key, out T record)
            where T : TpccTablePayload
        {
            TpccTablePayload payload;
            bool isAborted = this.Read(key, out payload);
            record = payload as T;
            return isAborted;
        }

        public bool ReadCopyAs<T>(TpccTableKey key, out T record)
            where T : TpccTablePayload
        {
            TpccTablePayload payload;
            bool isAborted = this.ReadCopy(key, out payload);
            record = payload as T;
            return isAborted;
        }

        public virtual
        bool ReadCopy(TpccTableKey key, out TpccTablePayload record)
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
            for (int i = 0; i < poolRefs.Length; ++i)
            {
                string tableName = TpccTable.allTypes[i].Name();
                poolRefs[i] = SingletonExecution.GetLocalObjectPool(
                    versionDb.GetVersionTable(tableName), workerId);
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

        public override
        bool ReadCopy(TpccTableKey key, out TpccTablePayload record)
        {
            bool isAborted = Read(key, out record);
            if (isAborted)
            {
                return false;
            }
            record = this.GetObjectPool(
                key.Table.Type()).GetCopy(record) as TpccTablePayload;
            return record != null;
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
