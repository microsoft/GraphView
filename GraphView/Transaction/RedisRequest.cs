
namespace GraphView.Transaction
{
    internal enum RedisRequestType
    {
        NewTx1,
        NewTx2,
        GetTxEntry,
        UpdateTxStatus,
        SetCommitTs,
        UpdateCommitLowerBound,
        GetVersionList,
        InitiGetVersionList,
        ReplaceVersion,
        UploadVersion,
        UpdateVersionMaxTs,
        DeleteVersion,
    }

    internal class RedisRequest
    {
        internal object Result { get; private set; }

        internal string HashId { get; private set; }
        internal byte[] Key { get; private set; }
        internal byte[] Value { get; private set; }

        internal byte[][] Keys { get; private set; }

        internal byte[][] Values { get; private set; }

        internal string Sha { get; private set; }

        internal RedisRequestType Type { get; private set; }

        public RedisRequest(string hashId, byte[] key, byte[] value, RedisRequestType type)
        {
            this.HashId = hashId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
        }

        public RedisRequest(string hashId, byte[] key, RedisRequestType type)
        {
            this.HashId = hashId;
            this.Key = key;
            this.Type = type;
        }

        public RedisRequest(string hashId, byte[][] keys, RedisRequestType type)
        {
            this.HashId = HashId;
            this.Keys = keys;
            this.Type = type;
        }

        public RedisRequest(string hashId, byte[][] keys, byte[][] values, RedisRequestType type)
        {
            this.HashId = HashId;
            this.Keys = keys;
            this.Values = values;
            this.Type = type;
        }

        public RedisRequest(byte[][] keys, string sha, RedisRequestType type)
        {
            this.Keys = keys;
            this.Sha = sha;
            this.Type = type;
        } 

        public RedisRequest(string hashId, RedisRequestType type)
        {
            this.HashId = hashId;
            this.Type = type;
        }

        internal void SetLong(long result)
        {
            this.Result = result;
        }

        internal void SetValues(byte[][] result)
        {
            this.Result = result;
        }
    }
}
