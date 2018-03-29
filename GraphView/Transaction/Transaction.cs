using System.Runtime.CompilerServices;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Data.Entity;
    using System.Threading.Tasks;
    using GraphView.GraphViewDBPortal;
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;

    internal class ReadSetEntry
    {
        internal long BeginTimestamp { get; private set; }
        internal long EndTimestamp { get; private set; }
        internal object Record { get; private set; }

        public ReadSetEntry(long begin, long end, object record)
        {
            this.BeginTimestamp = begin;
            this.EndTimestamp = end;
            this.Record = record;
        }
    }

    public partial class Transaction
    {
        /// <summary>
        /// Data store for loggingl
        /// </summary>
        private readonly LogStore logStore;

        /// <summary>
        /// Version Db for concurrency control
        /// </summary>
        private readonly VersionDb versionDb;


        /// <summary>
        /// Transaction id assigned to this transaction
        /// </summary>
        private readonly long txId;

        /// <summary>
        /// The status of this transaction.
        /// </summary>
        private TxStatus txStatus;

        private long commitTs;

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, VersionEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, List<VersionEntry>>> writeSet;

        private readonly Dictionary<string, Dictionary<object, object>> writeSet2;

        /// <summary>
        /// A set of version entries that need to be rolled back upon abortion
        /// </summary>
        private readonly Dictionary<string, HashSet<object>> rollbackSet;
 
        /// <summary>
        /// Use this tuple to track the last version we have uploaded to the version table successfully.
        /// Item1: tableId
        /// Item2: recordKey
        /// Item3: versionKey
        /// </summary>
        private Tuple<string, object, long> uploadProgress;

        public Transaction(LogStore logStore, VersionDb versionDb)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.readSet = new Dictionary<string, Dictionary<object, VersionEntry>>();
            this.writeSet = new Dictionary<string, Dictionary<object, List<VersionEntry>>>();

            this.txId = this.versionDb.InsertNewTx();
            this.txStatus = TxStatus.Ongoing;

            this.commitTs = -1;
            this.uploadProgress = null;
        }

    }

    // For low-level operations
    public partial class Transaction
    {
        
    }

    // For Json operations
    public partial class Transaction
    {
        internal long GetBeginTimestamp()
        {
            long maxReadTimestamp = 0;
            //Tranverse the readSet to get the begin timestamp
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    long currentBeginTimestamp = readSet[tableId][recordKey].BeginTimestamp;
                    if (maxReadTimestamp < currentBeginTimestamp)
                    {
                        maxReadTimestamp = currentBeginTimestamp;
                    } 
                }
            }

            return maxReadTimestamp;
        }

        internal bool UploadLocalWriteRecords()
        {
            foreach (string tableId in this.writeSet2.Keys)
            {
                foreach (object recordKey in this.writeSet2[tableId])
                {
                    if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
                    {
                        // Upload the new version entry when the new image is not null
                        if (this.writeSet2[tableId][recordKey] != null)
                        {
                            VersionEntry newImageEntry = new VersionEntry(
                                recordKey,
                                this.readSet[tableId][recordKey].VersionKey + 1,
                                this.writeSet2[tableId][recordKey],
                                txId,
                                -1,
                                -1,
                                0);

                            // Call VersionDB API to upload
                            if (!this.versionDb.UploadRecordByKey(tableId, recordKey, null, newImageEntry))
                            {
                                return false;
                            }
                        }

                        // Replace the old tail in the version list with an entry whose txId is set to the current tx
                        VersionEntry newTailEntry = new VersionEntry(
                            recordKey,
                            this.readSet[tableId][recordKey].VersionKey,
                            this.readSet[tableId][recordKey].Record,
                            this.txId,
                            this.readSet[tableId][recordKey].BeginTimestamp,
                            this.readSet[tableId][recordKey].EndTimestamp,
                            this.readSet[tableId][recordKey].MaxCommitTs);

                        if (!this.versionDb.UploadRecordByKey(tableId, recordKey, this.readSet[tableId][recordKey], newTailEntry))
                        {
                            return false;
                        }
                    }
                    else
                    {

                    }
                }
            }


            foreach (string tableId in this.writeSet.Keys)
            {
                foreach (object recordKey in this.writeSet[tableId].Keys)
                {

                    if (this.writeSet[tableId][recordKey].Count() == 2)
                    {
                        //update
                        //first try upload the old version
                        if (!this.versionDb.UploadRecordByKey(tableId, recordKey, 
                            this.readSet[tableId][recordKey],
                            this.writeSet[tableId][recordKey].First()))
                        {
                            this.Abort();
                            return false;
                        }
                        this.uploadProgress = new Tuple<string, object, long>(
                            tableId, recordKey, this.writeSet[tableId][recordKey].First().VersionKey);
                        //then upload the new version
                        if (!this.versionDb.UploadRecordByKey(tableId, recordKey,
                            null,
                            this.writeSet[tableId][recordKey].Last()))
                        {
                            this.Abort();
                            return false;
                        }
                        this.uploadProgress = new Tuple<string, object, long>(tableId, recordKey,
                            this.writeSet[tableId][recordKey].Last().VersionKey);
                    }
                    else if (this.writeSet[tableId][recordKey].First().EndTimestamp != long.MaxValue)
                    {
                        //delete
                        if (!this.versionDb.UploadRecordByKey(tableId, recordKey,
                            this.readSet[tableId][recordKey],
                            this.writeSet[tableId][recordKey].First()))
                        {
                            this.Abort();
                            return false;
                        }
                        this.uploadProgress = new Tuple<string, object, long>(
                            tableId, recordKey, this.writeSet[tableId][recordKey].First().VersionKey);
                    }
                    else
                    {
                        //insert
                        if (!this.versionDb.UploadRecordByKey(tableId, recordKey,
                            null,
                            this.writeSet[tableId][recordKey].First()))
                        {
                            this.Abort();
                            return false;
                        }
                        this.uploadProgress = new Tuple<string, object, long>(
                            tableId, recordKey, this.writeSet[tableId][recordKey].First().VersionKey);
                    }
                }
            }

            return true;
        }

        internal void GetCommitTimestamp()
        {
            long lowerBound = this.GetBeginTimestamp();
            //just check the old version's maxCommitTs in writeSet
            foreach (string tableId in this.writeSet.Keys)
            {
                foreach (object recordKey in this.writeSet[tableId].Keys)
                {
                    if (this.writeSet[tableId][recordKey].First().EndTimestamp != long.MaxValue)
                    {
                        long maxCommitTs = this.writeSet[tableId][recordKey].First().MaxCommitTs;
                        if (lowerBound < maxCommitTs)
                        {
                            lowerBound = maxCommitTs;
                        }
                    }
                }
            }

            this.commitTs = this.versionDb.GetAndSetCommitTime(this.txId, lowerBound);
        }

        internal bool Validate()
        {
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    VersionEntry reReadVersion = this.versionDb.GetVersionEntryByKey(tableId, recordKey, 
                        this.readSet[tableId][recordKey].VersionKey);
                    if (reReadVersion.TxId == -1)
                    {
                        //A committed version
                        if (this.commitTs > reReadVersion.EndTimestamp)
                        {
                            this.Abort();
                            return false;
                        }
                        //try to update the version's maxCommitTs

                    }
                    else
                    {
                        TxTableEntry txEntry = this.versionDb.GetTxTableEntry(reReadVersion.TxId);
                        //check the tx's status
                        if (txEntry.Status == TxStatus.Committed)
                        {
                            if (this.commitTs > reReadVersion.EndTimestamp)
                            {
                                this.Abort();
                                return false;
                            }
                            //try to update the version's maxCommitTs

                        }
                        else if (txEntry.Status == TxStatus.Aborted)
                        {
                            //try to update the version's maxCommitTs

                        }
                        else if (txEntry.CommitTime == -1)
                        {
                            if (!this.versionDb.UpdateCommitTsLowerBound(reReadVersion.TxId, this.commitTs + 1))
                            {
                                this.Abort();
                                return false;
                            }
                            //try to update the version's maxCommitTs

                        }
                        else
                        {
                            if (this.commitTs > txEntry.CommitTime)
                            {
                                this.Abort();
                                return false;
                            }
                            //try to update the version's maxCommitTs

                        }
                    }

                    if (!this.versionDb.UpdateVersionMaxCommitTs(tableId, recordKey, 
                        reReadVersion.VersionKey,
                        reReadVersion,
                        this.commitTs))
                    {
                        this.Abort();
                        return false;
                    }
                }
            }

            return true;
        }

        internal void Abort()
        {
            this.txStatus = TxStatus.Aborted;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Aborted);

            this.PostProcessing();
        }

        internal void PostProcessing()
        {
            if (this.txStatus == TxStatus.Committed)
            {
                foreach (string tableId in this.writeSet.Keys)
                {
                    foreach (object recordKey in this.writeSet[tableId].Keys)
                    {
                        foreach (VersionEntry entry in this.writeSet[tableId][recordKey])
                        {
                            bool isOld = entry.EndTimestamp != long.MaxValue;
                            this.versionDb.UpdateCommittedVersionTimestamp(
                                tableId, recordKey, entry.VersionKey, this.commitTs, this.txId, isOld);
                        }
                    }
                }
            }
            else
            {
                //if the uploadProgress is null, 
                //the tx abort before Uploading phase,
                //no version is uploaded to DB, do nothing
                if (this.uploadProgress != null)
                {
                    foreach (string tableId in this.writeSet.Keys)
                    {
                        foreach (object recordKey in this.writeSet[tableId].Keys)
                        {
                            foreach (VersionEntry entry in this.writeSet[tableId][recordKey])
                            {
                                bool isOld = entry.EndTimestamp != long.MaxValue;
                                this.versionDb.UpdateAbortedVersionTimestamp(
                                    tableId, recordKey, entry.VersionKey, this.txId, isOld);

                                if (tableId == this.uploadProgress.Item1 &&
                                    recordKey == this.uploadProgress.Item2 &&
                                    entry.VersionKey == this.uploadProgress.Item3)
                                {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }

        internal bool Commit()
        {
            if (!this.UploadLocalWriteRecords())
            {
                return false;
            }

            this.GetCommitTimestamp();

            if (!this.Validate())
            {
                return false;
            }

            this.WriteChangetoLog();
            this.txStatus = TxStatus.Committed;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Committed);
            
            this.PostProcessing();

            return true;
        }

        internal void WriteChangetoLog()
        {
            throw new NotImplementedException();
        }
    }

    public partial class Transaction
    {
        public bool InsertJson(string tableId, object recordKey, JObject record)
        {
            //first try to find whether the record already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey].Count() == 1 &&
                    this.writeSet[tableId][recordKey].First().EndTimestamp != long.MaxValue)
                {
                    long oldVersionKey = this.writeSet[tableId][recordKey].First().VersionKey;
                    //the transaction just delete this record, we can perform insert op.
                    //(just like an update op in a whole)
                    this.writeSet[tableId][recordKey].Add(new VersionEntry(recordKey, 
                        oldVersionKey + 1, record, this.txId, this.txId, long.MaxValue, 0));

                    return true;
                }

                this.Abort();
                return false;
            }

            //this record is not in the readSet and the writeSet
            if ((!this.writeSet.ContainsKey(tableId) || !this.writeSet[tableId].ContainsKey(recordKey)) &&
                (!this.readSet.ContainsKey(tableId) || !this.readSet[tableId].ContainsKey(recordKey)))
            {
                long largestVersionKey = -1;
                //try to get the most recent version from DB
                if (this.versionDb.GetRecentVersionEntry(tableId, recordKey, out largestVersionKey) != null)
                {
                    //find a version from DB, can not insert
                    this.Abort();
                    return false;
                }
                //can not find, insert
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, List<VersionEntry>>();
                }

                if (!this.writeSet[tableId].ContainsKey(recordKey))
                {
                    this.writeSet[tableId][recordKey] = new List<VersionEntry>();
                }

                this.writeSet[tableId][recordKey].Add(new VersionEntry(recordKey, 
                    largestVersionKey+1, record, this.txId, this.txId, long.MaxValue, 0));

                return true;
            }

            this.Abort();
            return false;
        }

        public JObject ReadJson(string tableId, object recordKey)
        {
            //first try to find whether the record already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey].Count() == 2)
                {
                    //just perform update, we can read the updated record
                    return this.writeSet[tableId][recordKey].Last().JsonRecord;
                }
                else
                {
                    if (this.writeSet[tableId][recordKey].First().EndTimestamp == long.MaxValue)
                    {
                        //just perform insert, we can read this inserted record
                        return this.writeSet[tableId][recordKey].First().JsonRecord;
                    }
                    else
                    {
                        //just perform delete, we can not read this record
                        return null;
                    }
                }
            }

            //check whether the record is in the readSet
            if (this.readSet.ContainsKey(tableId) && !this.readSet[tableId].ContainsKey(recordKey))
            {
                return this.readSet[tableId][recordKey].JsonRecord;
            }

            //can not find in local, try to read the most recent version from DB
            long largestVersionKey = -1;
            VersionEntry entry = this.versionDb.GetRecentVersionEntry(tableId, recordKey, out largestVersionKey);
            if (entry != null)
            {
                //successfully read a version
                //add to readSet
                if (!this.readSet.ContainsKey(tableId))
                {
                    this.readSet[tableId] = new Dictionary<object, VersionEntry>();
                }

                this.readSet[tableId][recordKey] = (VersionEntry) entry.Clone();
                return entry.JsonRecord;
            }

            return null;
        }

        public bool UpdateJson(string tableId, object recordKey, JObject record)
        {
            //first try to find whether the record already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey].Count() == 2)
                {
                    //just perform update, we can perform update op again.
                    this.writeSet[tableId][recordKey].Last().Record = record;
                    return true;
                }
                else
                {
                    if (this.writeSet[tableId][recordKey].First().EndTimestamp == long.MaxValue)
                    {
                        //just perform insert, we can perform update op.
                        this.writeSet[tableId][recordKey].First().Record = record;
                        return true;
                    }
                    else
                    {
                        //just perform delete, we can not update.
                        this.Abort();
                        return false;
                    }
                }
            }

            //then check whether this record is in the readSet.
            if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
            {
                //we have read this before, check whether this version can be updated
                if (this.readSet[tableId][recordKey].EndTimestamp != long.MaxValue)
                {
                    this.Abort();
                    return false;
                }

                //perform update op.
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, List<VersionEntry>>();
                }

                if (!this.writeSet[tableId].ContainsKey(recordKey))
                {
                    this.writeSet[tableId][recordKey] = new List<VersionEntry>();
                }

                //add the old version to writeSet
                VersionEntry entry = (VersionEntry) this.readSet[tableId][recordKey].Clone();
                entry.EndTimestamp = this.txId;
                entry.TxId = this.txId;
                this.writeSet[tableId][recordKey].Add(entry);
                //add the new version to writeSet
                long oldVersionKey = entry.VersionKey;
                this.writeSet[tableId][recordKey].Add(new VersionEntry(recordKey,
                    oldVersionKey + 1, record, this.txId, this.txId, long.MaxValue, 0));
                return true;
            }

            //the record is not in local, try to get the record from DB
            long largestVersionKey = -1;
            VersionEntry mostRecentVersionEntry = this.versionDb.GetRecentVersionEntry(tableId, recordKey, out largestVersionKey);
            if (mostRecentVersionEntry == null || mostRecentVersionEntry.EndTimestamp != long.MaxValue)
            {
                this.Abort();
                return false;
            }
            //get the most recent version, add it to readSet, add the old and new version to writeSet
            if (!this.readSet.ContainsKey(tableId))
            {
                this.readSet[tableId] = new Dictionary<object, VersionEntry>();
            }
            this.readSet[tableId][recordKey] = mostRecentVersionEntry;

            //perform update op.
            if (!this.writeSet.ContainsKey(tableId))
            {
                this.writeSet[tableId] = new Dictionary<object, List<VersionEntry>>();
            }

            if (!this.writeSet[tableId].ContainsKey(recordKey))
            {
                this.writeSet[tableId][recordKey] = new List<VersionEntry>();
            }

            VersionEntry oldVersionEntry = (VersionEntry) mostRecentVersionEntry.Clone();
            oldVersionEntry.EndTimestamp = this.txId;
            oldVersionEntry.TxId = this.txId;
            this.writeSet[tableId][recordKey].Add(oldVersionEntry);
            this.writeSet[tableId][recordKey].Add(new VersionEntry(recordKey,
                largestVersionKey + 1, record, this.txId, this.txId, long.MaxValue, 0));
            return true;
        }

        public bool DeleteJson(string tableId, object recordKey)
        {
            //first try to find whether the record already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey].Count() == 2)
                {
                    //just perform update, we can perform delete op.
                    this.writeSet[tableId][recordKey].RemoveAt(1);
                    return true;
                }
                else
                {
                    if (this.writeSet[tableId][recordKey].First().EndTimestamp == long.MaxValue)
                    {
                        //just perform insert, we can perform delete op.
                        this.writeSet[tableId][recordKey].Clear();
                        this.writeSet[tableId].Remove(recordKey);
                        return true;
                    }
                    else
                    {
                        //just perform delete, we can not delete again.
                        this.Abort();
                        return false;
                    }
                }
            }

            //then check whether this record is in the readSet.
            if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
            {     
                //we have read this before, check whether this version can be deleted
                if (this.readSet[tableId][recordKey].EndTimestamp != long.MaxValue)
                {
                    this.Abort();
                    return false;
                }

                //perform delete op.
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, List<VersionEntry>>();
                }

                if (!this.writeSet[tableId].ContainsKey(recordKey))
                {
                    this.writeSet[tableId][recordKey] = new List<VersionEntry>();
                }

                VersionEntry entry = (VersionEntry) this.readSet[tableId][recordKey].Clone();
                entry.EndTimestamp = this.txId;
                entry.TxId = this.txId;
                this.writeSet[tableId][recordKey].Add(entry);
                return true;
            }

            //the record is not in local, try to get the record from DB
            long largestVersionKey = -1;
            VersionEntry mostRecentVersionEntry = this.versionDb.GetRecentVersionEntry(tableId, recordKey, out largestVersionKey);
            if (mostRecentVersionEntry == null || mostRecentVersionEntry.EndTimestamp != long.MaxValue)
            {
                this.Abort();
                return false;
            }
            //get the most recent version, add it to readSet, add the deleted version to writeSet
            if (!this.readSet.ContainsKey(tableId))
            {
                this.readSet[tableId] = new Dictionary<object, VersionEntry>();
            } 
            this.readSet[tableId][recordKey] = mostRecentVersionEntry;

            //perform delete op.
            if (!this.writeSet.ContainsKey(tableId))
            {
                this.writeSet[tableId] = new Dictionary<object, List<VersionEntry>>();
            }

            if (!this.writeSet[tableId].ContainsKey(recordKey))
            {
                this.writeSet[tableId][recordKey] = new List<VersionEntry>();
            }

            VersionEntry deletedEntry = (VersionEntry) mostRecentVersionEntry.Clone();
            deletedEntry.EndTimestamp = this.txId;
            deletedEntry.TxId = this.txId;
            this.writeSet[tableId][recordKey].Add(deletedEntry);
            return true;
        }
    }
}

