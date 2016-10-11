using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace GraphView
{
    public class BulkInsertCommand
    {
        public DocumentClient client { get; set; }

        public BulkInsertCommand()
        { }

        public BulkInsertCommand(DocumentClient pClient)
        { client = pClient; }

        public async Task<StoredProcedure> TryCreatedStoredProcedure(string collectionLink, StoredProcedure sproc)
        {
            StoredProcedure check =
                client.CreateStoredProcedureQuery(collectionLink)
                    .Where(s => s.Id == sproc.Id)
                    .AsEnumerable()
                    .FirstOrDefault();

            if (check != null) return check;

            Console.WriteLine("BulkInsert proc doesn't exist, try to create a new one.");
            sproc = await client.CreateStoredProcedureAsync(collectionLink, sproc);
            return sproc;
        }

        public async Task<int> BulkInsertAsync(string sprocLink, dynamic[] objs)
        {
            StoredProcedureResponse<int> scriptResult =
                await
                    client.ExecuteStoredProcedureAsync<int>(sprocLink, objs);
            return scriptResult.Response;
        }

        public static string GenerateNodesJsonString(List<string> nodes, int currentIndex, int maxJsonSize)
        {
            var jsonDocArr = new StringBuilder();
            jsonDocArr.Append("[");

            jsonDocArr.Append(GraphViewJsonCommand.ConstructNodeJsonString(nodes[currentIndex]));

            while (jsonDocArr.Length < maxJsonSize && ++currentIndex < nodes.Count)
                jsonDocArr.Append(", " + GraphViewJsonCommand.ConstructNodeJsonString(nodes[currentIndex]));

            jsonDocArr.Append("]");

            return jsonDocArr.ToString();
        }
    }
}
