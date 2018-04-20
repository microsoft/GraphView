namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json.Linq;

	internal class LogVersionEntry
	{
		internal object RecordKey { get; private set; }
		internal object Payload { get; private set; }
		internal long CommitTs { get; private set; }

		public LogVersionEntry(object recordKey, object payload, long commitTs)
		{
			this.RecordKey = recordKey;
			this.Payload = payload;
			this.CommitTs = commitTs;
		}
	}

    /// <summary>
    /// A data store for logging
    /// </summary>
    public interface ILogStore
    {
        bool WriteCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs);
        bool WriteCommittedTx(long txId);
    }
    
    public class CosmosDBStore : ILogStore
    {
        private readonly string url;
        private readonly string primaryKey;
        private readonly string databaseId;
        private readonly string collectionId;
        private readonly DocumentClient cosmosDbClient;

        public CosmosDBStore(
            string dbEndpointUrl,
            string dbAuthorizationKey,
            string dbId,
            string dbCollection,
            string preferredLocation = null)
        {
            this.url = dbEndpointUrl;
            this.primaryKey = dbAuthorizationKey;
            this.databaseId = dbId;
            this.collectionId = dbCollection;

            ConnectionPolicy connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };

            if (!string.IsNullOrEmpty(preferredLocation))
            {
                connectionPolicy.PreferredLocations.Add(preferredLocation);
            }

            this.cosmosDbClient = new DocumentClient(new Uri(this.url), this.primaryKey, connectionPolicy);
            this.cosmosDbClient.OpenAsync().Wait();
        }

        public bool WriteCommittedTx(long txId)
        {
            throw new NotImplementedException();
        }

        public bool WriteCommittedVersion(string tableId, object recordKey, object payload, long txId, long commitTs)
        {
            throw new NotImplementedException();
        }
    }
}
