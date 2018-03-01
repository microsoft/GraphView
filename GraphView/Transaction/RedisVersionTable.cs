namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal partial class RedisVersionTable : VersionTable
    {
        /// <summary>
        /// The redis database index of the current table
        /// </summary>
        private readonly long redisDbIndex;

        /// <summary>
        /// Get redisClient from the client pool
        /// </summary>
        private RedisNativeClient RedisClient
        {
            get
            {
                return RedisClientManager.Instance.GetRedisClient();
            }
        }

        public RedisVersionTable(string tableId, long redisDbIndex)
            : base(tableId)
        {
            this.redisDbIndex = redisDbIndex;
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            if (this.RedisClient == null)
            {
                throw new ArgumentException($"redisClient is null");
            }

            string hashKey = recordKey as string;
            byte[][] versionBytes = this.RedisClient.HGetAll(hashKey);
           
            if (versionBytes == null)
            {
                throw new ArgumentException("Invalid recordKey reference '{recordKey}'");
            }

            List<VersionEntry> versionList = new List<VersionEntry>();
            foreach (byte[] bytes in versionBytes)
            {
                VersionEntry versionEntry = VersionEntrySerializer.DeserializeFromBytes(bytes);
                versionList.Add(versionEntry);
            }
            return versionList;
        }

        internal override IEnumerable<VersionEntry> GetVersionList(object recordKey, long timestamp)
        {
            return base.GetVersionList(recordKey, timestamp);
        }

        internal override void InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            if (this.RedisClient == null)
            {
                throw new ArgumentException($"redisClient is null");
            }

            string hashKey = recordKey as string;
            byte[] key = BitConverter.GetBytes(version.VersionKey);
            byte[] value = VersionEntrySerializer.SerializeToBytes(version);

            this.RedisClient.HSet(hashKey, key, value);
        }

        internal override bool UpdateAndUploadVersion(object recordKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            if (this.RedisClient == null)
            {
                throw new ArgumentException("redisClient is null");
            }

            string hashKey = recordKey as string;
            byte[] key = BitConverter.GetBytes(oldVersion.VersionKey);
            byte[] value = VersionEntrySerializer.SerializeToBytes(newVersion);

            long result = this.RedisClient.HSet(hashKey, key, value);

            return result == 0;
        }

        internal override void DeleteVersionEntry(object recordKey, long versionKey)
        {
            if (this.RedisClient == null)
            {
                throw new ArgumentException("redisClient is null");
            }

            string hashKey = recordKey as string;
            byte[] key = BitConverter.GetBytes(versionKey);

            this.RedisClient.HDel(hashKey, key);
        }
    }

    /// <summary>
    /// The implemetation of IVersionTableStore part
    /// </summary>
    internal partial class RedisVersionTable : IVersionedTableStore
    {
        public bool DeleteJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public JObject GetJson(object key, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRecordKeyList(object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool InsertJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public bool UpdateJson(object key, JObject record, Transaction tx)
        {
            throw new NotImplementedException();
        }
    }
}
