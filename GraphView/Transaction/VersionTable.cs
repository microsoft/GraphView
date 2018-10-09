
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public abstract partial class VersionTable
    {
        public readonly string tableId;

        /// <summary>
        /// The maximal version entry count for version list
        /// </summary>
        internal static readonly int VERSION_CAPACITY = 1024;

        /// <summary>
        /// table visitors for version entry requests
        /// </summary>
        internal VersionTableVisitor[] tableVisitors;   // to avoid memory overflow used by cassandra

        

        /// <summary>
        /// The version db instance of the current version table
        /// In case of version db may hold some information about the index, partition etc.
        /// </summary>
        internal VersionDb VersionDb { get; set; }

        /// <summary>
        /// The number of partitions for the current version table
        /// </summary>
        internal int PartitionCount { get; set; }

        public VersionTable(VersionDb versionDb, string tableId, int partitionCount = 4)
        {
            // private properties
            this.VersionDb = versionDb;
            this.tableId = tableId;
            this.PartitionCount = partitionCount;

            // the table visitors
            this.tableVisitors = new VersionTableVisitor[partitionCount];
        }

        public VersionTableVisitor GetWorkerLocalVisitor(int workerId)
        {
            return this.tableVisitors[workerId];
        }

        /// <summary>
        /// Add new partitions
        /// </summary>
        /// <param name="partitionCount">The number of partitions after add new partitions</param>
        internal virtual void AddPartition(int partitionCount)
        {
            this.PartitionCount = partitionCount;
        }

        internal virtual void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            // Interlocked.Increment(ref VersionDb.EnqueuedRequests);
            return;
        }

        internal virtual void Visit(int partitionKey)
        {
            return;   
        }

        /// <summary>
        /// Clear all contents of a version table
        /// </summary>
        internal virtual void Clear()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get a list of version entries, which will be used to check visiablity
        /// </summary>
        /// <returns></returns>
        internal virtual IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }
        
        /// <summary>
        /// To keep the same actions whether the version list is empty or not when insert a new version
        /// That is computing the new version's version key as largestKey + 1
        /// We would try to add an useless version entry at the head of version list if it's empty
        /// InitializeAndGetVersionList has two steps:
        /// (1) initialize a version list with adding an empty version if the version list is empty
        /// (2) read all version entries inside the version list
        /// </summary>
        /// <returns>An IEnumerable of version entries</returns>
        internal virtual IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// This method will be called during Uploading Phase and the PostProcessing Phase.
        /// </summary>
        /// <returns></returns>
        internal virtual VersionEntry ReplaceVersionEntry(object recordKey, long versionKey, 
            long beginTimestamp, long endTimestamp, long txId, long readTxId, long expectedEndTimestamp) 
        {
            throw new NotImplementedException();
        }
        
        /// <summary>
        /// Replace the whole version entry, which will be called in the commit postprocessing phase.
        /// For write operations, like delete and update, the old version entry must be holden by the current 
        /// transaction, which can be replaced directly rather than call lua script
        /// </summary>
        /// <param name="recordKey">The specify record key</param>
        /// <param name="versionKey">The specify version key</param>
        /// <param name="versionEntry">The version entry will be put</param>
        /// <returns></returns>
        internal virtual bool ReplaceWholeVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Upload a new version entry when insert or update a version
        /// </summary>
        /// <returns>True of False</returns>
        internal virtual bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In negotiation phase, if no another transaction is updating tx, make sure future tx's who
        /// updates x have CommitTs greater than or equal to the commitTime of current transaction
        /// 
        /// <returns></returns>
        /// Update the version's maxCommitTs in the validataion phase
        /// </summary>
        /// <param name="commitTs">The current transaction's commit time</param>
        /// </param>
        /// <returns>A updated or non-updated version entry</returns>
        internal virtual VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTs)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In the Validate phase, get this version entry to check its MaxCommitTs.
        /// </summary>
        /// <returns>The version entry with recordKey and version key</returns>
        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Given a batch of verion keys, retrieves a collection of verion entries in a batch
        /// </summary>
        /// <param name="batch">a list of record keys and version keys</param>
        /// <returns></returns>
        internal virtual IDictionary<VersionPrimaryKey, VersionEntry> GetVersionEntryByKey(
            IEnumerable<VersionPrimaryKey> batch)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete a version entry by recordKey and version Key
        /// It will be called when the insertion or update is aborted.
        /// Inserted new version will be deleted to avoid unnecessary write conflicts
        /// </summary>
        /// <returns></returns>
        internal virtual bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// ONLY FOR BENCHMARK TEST
        /// A mock method to load data without the tx, which will load data directly
        /// rather than by transaction
        /// </summary>
        internal virtual void MockLoadData(int recordCount)
        {
            throw new NotImplementedException();
        }
    }
}