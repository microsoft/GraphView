

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A data store for logging
    /// </summary>
    public abstract class LogStore
    {
        public virtual long GetMaxTxSequenceNumber()
        {
            return 0;
        }

        public virtual IEnumerable<JObject> ReadJson(string recordId)
        {
            throw new NotImplementedException(); 
        }

        public virtual IEnumerable<JObject> ReadJson(IEnumerable<string> ridList)
        {
            throw new NotImplementedException();
        }

        public virtual IEnumerable<JObject> ReadJson(RecordQuery recordQuery)
        {
            throw new NotImplementedException();
        }

        public virtual void WriteJson(JObject record)
        {
            throw new NotImplementedException();
        }

        public virtual void WriteJson(IEnumerable<JObject> recordList)
        {
            throw new NotImplementedException();
        }
    }
    
    public class CosmosDBStore : LogStore
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
    }
}
