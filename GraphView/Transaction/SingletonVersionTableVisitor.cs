namespace GraphView.Transaction
{
    using System.Threading;
    using System.Diagnostics;
    using System;
    using NonBlocking;
    using System.Collections.Generic;

    public interface Copyable
    {
        Copyable Copy();

        /// <summary>
        /// Copy another `Copyable` to `this`
        /// </summary>
        /// <returns>
        /// returns `true` if copy successfully
        /// `false` if `that` is not the same type as `this`
        /// </returns>
        bool CopyFrom(Copyable that);
    }

    class CachableObjectPool
    {
        private List<Copyable> objectPool = new List<Copyable>();

        public void Cache(Copyable obj)
        {
            objectPool.Add(obj);
        }
        public Copyable GetCopy(Copyable obj)
        {
            for (int i = objectPool.Count - 1; i >= 0; --i)
            {
                Copyable o = objectPool[i];
                if (o.CopyFrom(obj))
                {
                    objectPool.RemoveAt(i);
                    return o;
                }
            }
            return obj.Copy();
        }
        public object TryGetCopy(object obj)
        {
            Copyable copyable = obj as Copyable;
            if (copyable == null) return obj;
            return this.GetCopy(copyable);
        }
        public bool TryCache(object obj)
        {
            Copyable copyable = obj as Copyable;
            if (copyable == null)
            {
                return false;
            }
            this.Cache(copyable);
            return true;
        }
    }

    internal class SingletonVersionTableVisitor : VersionTableVisitor
    {
        private readonly Dictionary<object, ConcurrentDictionary<long, VersionEntry>> dict;
        public readonly CachableObjectPool recordPool = new CachableObjectPool();

        public SingletonVersionTableVisitor(Dictionary<object, ConcurrentDictionary<long, VersionEntry>> dict)
        {
            this.dict = dict;
        }

        static void ResetTailEntry(VersionEntry tailEntry)
        {
            Interlocked.Exchange(
                ref tailEntry.BeginTimestamp, VersionEntry.DEFAULT_BEGIN_TIMESTAMP);
            Interlocked.Exchange(
                ref tailEntry.EndTimestamp, VersionEntry.DEFAULT_END_TIMESTAMP);
        }

        internal override void Visit(DeleteVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = req.RemoteVerList as
                ConcurrentDictionary<long, VersionEntry>;
            // Only get the version list location when version list is null
            if (versionList == null)
            {
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    req.Result = true;
                    req.Finished = true;
                    return;
                }
            }

            VersionEntry tailEntry = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);

            long headKey = Interlocked.Read(ref tailEntry.EndTimestamp);
            long tailKey = Interlocked.Read(ref tailEntry.BeginTimestamp);

            // Debug Assertion
            // if (tailKey != req.VersionKey)
            // {
            //     throw new Exception("Abort isn't deleting it's dirty write?");
            // }

            // As only if a tx uploads a version entry, it will change the value tailEntry
            // Here the dirty version entry hasn't been deleted, so there will no other txs update the tailEntry
            // We don't need to take Interlocked to update its value

            // when the deleted entry is the only one in version list
            if (headKey == tailKey)
            {
                ResetTailEntry(tailEntry);
                req.Result = true;
                req.Finished = true;
                return;
            }

            Interlocked.Exchange(ref tailEntry.BeginTimestamp, tailKey - 1);

            VersionEntry versionEntry = null;
            if (headKey > 0)
            {
                versionList.TryAdd(headKey - 1, versionEntry);
                Interlocked.Exchange(ref tailEntry.EndTimestamp, headKey - 1);
            }

            if (versionList.TryRemove(req.VersionKey, out versionEntry))
            {
                // Debug Assertion
                // if (Interlocked.Read(ref versionEntry.TxId) != req.SenderId)
                // {
                //     throw new Exception("I'm not deleting my own dirty write?");
                // }
                this.recordPool.TryCache(versionEntry.Record);
                versionEntry.Record = null;
                req.Result = true;
            }
            else
            {
                req.Result = false;
            }

            req.Finished = true;
        }

        internal override void Visit(InitiGetVersionListRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                ConcurrentDictionary<long, VersionEntry> newVersionList = new ConcurrentDictionary<long, VersionEntry>(32);
                // Adds a special entry whose key is -1 when the list is initialized.
                // The entry uses beginTimestamp as a pointer pointing to the newest verion in the list.
                VersionEntry entry = VersionEntry.InitEmptyVersionEntry();
                ResetTailEntry(entry);
                newVersionList.TryAdd(SingletonDictionaryVersionTable.TAIL_KEY, entry);

                // if concurrentDict.TryAdd()
                this.dict.Add(req.RecordKey, newVersionList);

                // The version list is newly created by this tx. 
                // No meaningful versions exist, except for the artificial entry as a tail pointer. 
                req.RemoteVerList = newVersionList;
                req.Result = true;
                req.Finished = true;
                return;
            }
            req.RemoteVerList = null;
            req.Result = false;
            req.Finished = true;
        }

        internal override void Visit(ReplaceVersionRequest req)
        {
            VersionEntry entry = req.RemoteVerEntry;
            if (entry == null)
            {
                ConcurrentDictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    throw new TransactionException("The specified record does not exist.");
                }

                if (!versionList.TryGetValue(req.VersionKey, out entry))
                {
                    throw new TransactionException("The specified version does not exist.");
                }
            }

            Debug.Assert(entry.VersionKey == req.VersionKey);
            // Debug Assertion
            // if (!entry.RecordKey.Equals(req.RecordKey))
            // {
            //     throw new Exception("Inconsistent record key");
            // }

            //int ticket = entry.EnterQueuedLatch();
            entry.WriteLock();
            if (Interlocked.Read(ref entry.TxId) == req.SenderId &&
                Interlocked.Read(ref entry.EndTimestamp) == req.ExpectedEndTs)
            {
                Interlocked.Exchange(ref entry.BeginTimestamp, req.BeginTs);
                Interlocked.Exchange(ref entry.EndTimestamp, req.EndTs);
                Interlocked.Exchange(ref entry.TxId, req.TxId);
                VersionEntry.CopyFromRemote(entry, req.LocalVerEntry);
                // req.LocalVerEntry.RecordKey = req.RecordKey;
            }
            //entry.ExitQueuedLatch(ticket);
            entry.UnWriteLock();

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        static private void UploadFail(UploadVersionRequest req)
        {
            req.RemoteVerEntry = req.VersionEntry;
            req.Result = false;
            req.Finished = true;
        }

        internal override void Visit(UploadVersionRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = req.RemoteVerList as
                ConcurrentDictionary<long, VersionEntry>;
            if (versionList == null)
            {
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    throw new TransactionException("The specified record does not exist.");
                }
            }

            if (versionList.TryAdd(req.VersionKey, req.VersionEntry))
            {
                req.VersionEntry.ResetLatchQueue();

                // The new version has been inserted successfully. Re-directs the tail pointer to the new version.  
                VersionEntry tailEntry = null;
                versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);

                long headKey = Interlocked.Read(ref tailEntry.EndTimestamp);
                long tailKey = Interlocked.Read(ref tailEntry.BeginTimestamp);

                if (req.VersionKey < headKey)
                {
                    versionList.Remove(req.VersionKey);
                    UploadFail(req);
                    return;
                }
                // Here we use Interlocked to atomically update the tail entry, instead of ConcurrentDict.TryUpdate().
                // This is because once created, the whole tail entry always stays and is never replaced.
                // All concurrent tx's only access the tail pointer, i.e., the beginTimestamp field.  
                long oldVersion = Interlocked.Exchange(ref tailEntry.BeginTimestamp, req.VersionKey);

                // Debug Assertion
                // if (oldVersion != req.VersionKey - 1)
                // {
                //     throw new Exception("inconsistent version key");
                // }

                VersionEntry oldVerEntry = null;

                // EndTimestamp (headKey) being never set means we just insert
                // the first valid version. So the headKey should be redirected.
                if (headKey == VersionEntry.DEFAULT_END_TIMESTAMP)
                {
                    Interlocked.Exchange(ref tailEntry.EndTimestamp, tailKey);
                }
                else if (versionList.Count > VersionTable.VERSION_CAPACITY)
                {
                    Interlocked.Exchange(ref tailEntry.EndTimestamp, headKey + 1);
                    versionList.TryRemove(headKey, out oldVerEntry);
                    if (oldVerEntry != null)
                    {
                        this.recordPool.TryCache(oldVerEntry.Record);
                        oldVerEntry.Record = null;
                    }
                }

                // Debug Assertion
                // long debugTailkey = Interlocked.Read(ref tailEntry.BeginTimestamp);
                // if (debugTailkey != req.VersionKey)
                // {
                //     throw new Exception("Someone kicks in :(");
                // }

                req.RemoteVerEntry = oldVerEntry == null ? new VersionEntry() : oldVerEntry;
                req.Result = true;
                req.Finished = true;
                return;
            }
            else
            {
                // The same version key has been added before or by a concurrent tx. 
                // The new version cannot be inserted.
                UploadFail(req);
            }
        }

        internal override void Visit(UpdateVersionMaxCommitTsRequest req)
        {
            VersionEntry verEntry = req.RemoteVerEntry;
            if (verEntry == null)
            {
                ConcurrentDictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    throw new TransactionException("The specified record does not exist.");
                }

                if (!versionList.TryGetValue(req.VersionKey, out verEntry))
                {
                    throw new TransactionException("The specified version does not exist.");
                }
            }

            Debug.Assert(verEntry.VersionKey == req.VersionKey);

            //int ticket = verEntry.EnterQueuedLatch();
            verEntry.WriteLock();
            Interlocked.Exchange(
                ref verEntry.MaxCommitTs,
                Math.Max(req.MaxCommitTs, Interlocked.Read(ref verEntry.MaxCommitTs)));
            VersionEntry.CopyFromRemote(verEntry, req.LocalVerEntry);
            //verEntry.ExitQueuedLatch(ticket);
            verEntry.UnWriteLock();

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }

        internal override void Visit(GetVersionListRequest req)
        {
            ConcurrentDictionary<long, VersionEntry> versionList = null;
            if (!this.dict.TryGetValue(req.RecordKey, out versionList))
            {
                req.RemoteVerList = null;
                req.Result = 0;
                req.Finished = true;
                return;
            }
            int entryCount = 0;

            // The value at -1 in the version list is a special entry, 
            // whose beginTimestamp points to the newest version. 
            VersionEntry tailEntry = null;
            versionList.TryGetValue(SingletonDictionaryVersionTable.TAIL_KEY, out tailEntry);
            long lastVersionKey = Interlocked.Read(ref tailEntry.BeginTimestamp);

            TxList<VersionEntry> localList = req.LocalContainer;
            TxList<VersionEntry> remoteList = req.RemoteContainer;
            // Only returns top 2 newest versions. This is enough for serializability. 
            // For other isolation levels, more versions may need to be returned.
            // When old versions may be truncated, it is desirable to maintain a head pointer as well,
            // so as to increase the lower bound of version keys and reduce the number of iterations. 
            while (lastVersionKey >= 0 && entryCount < 2)
            {
                VersionEntry verEntry = null;
                if (versionList.TryGetValue(lastVersionKey, out verEntry))
                {
                    //int ticket = verEntry.EnterQueuedLatch();
                    verEntry.ReadLock();
                    // Debug Assertion
                    // if (!verEntry.RecordKey.Equals(req.RecordKey))
                    // {
                    //     throw new Exception("Inconsistent record key");
                    // }

                    VersionEntry.CopyFromRemote(verEntry, localList[entryCount]);
                    //verEntry.ExitQueuedLatch(ticket);
                    verEntry.UnReadLock();

                    // Here only add a reference to the list, no need to take the latch
                    remoteList.Add(verEntry);
                    entryCount++;

                    if (Interlocked.Read(ref verEntry.TxId) == VersionEntry.EMPTY_TXID)
                    {
                        break;
                    }
                }

                lastVersionKey--;
            }

            req.RemoteVerList = versionList;
            req.Result = entryCount;
            req.Finished = true;
        }

        internal override void Visit(ReadVersionRequest req)
        {
            VersionEntry versionEntry = req.RemoteVerEntry;
            if (versionEntry == null)
            {
                ConcurrentDictionary<long, VersionEntry> versionList = null;
                if (!this.dict.TryGetValue(req.RecordKey, out versionList))
                {
                    req.Result = null;
                    req.Finished = true;
                    return;
                }

                if (!versionList.TryGetValue(req.VersionKey, out versionEntry))
                {
                    req.Result = null;
                    req.Finished = true;
                    return;
                }
            }

            Debug.Assert(Interlocked.Read(ref versionEntry.VersionKey) == req.VersionKey);
            // Debug Assertion
            // if (!versionEntry.RecordKey.Equals(req.RecordKey))
            // {
            //     throw new Exception("Inconsistent record key");
            // }

            //int ticket = versionEntry.EnterQueuedLatch();
            versionEntry.ReadLock();
            VersionEntry.CopyFromRemote(versionEntry, req.LocalVerEntry);
            //versionEntry.ExitQueuedLatch(ticket);
            versionEntry.UnReadLock();

            req.Result = req.LocalVerEntry;
            req.Finished = true;
        }
    }
}
