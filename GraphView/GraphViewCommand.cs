// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;


namespace GraphView
{
    public partial class GraphViewCommand : IDisposable
    {
        public GraphViewConnection GraphViewConnection { get; set; }
        
        public string CommandText { get; set; }

        public OutputFormat OutputFormat { get; set; }

        public GraphViewCommand(GraphViewConnection connecion)
        {
            GraphViewConnection = connecion;
        }

        public GraphViewCommand(string commandText)
        {
            CommandText = commandText;
        }

        public GraphViewCommand(string commandText, GraphViewConnection connection)
        {
            CommandText = commandText;
            GraphViewConnection = connection;
        }

        public IEnumerable<string> Execute()
        {
            if (CommandText == null)
            {
                throw new QueryExecutionException("CommandText of GraphViewCommand is not set.");
            }
            return g().EvalGremlinTraversal(CommandText);
        }

        public async Task<StoredProcedure> TryCreatedStoredProcedureAsync(string collectionLink, StoredProcedure sproc)
        {
            StoredProcedure check =
                GraphViewConnection.DocDBclient.CreateStoredProcedureQuery(collectionLink)
                    .Where(s => s.Id == sproc.Id)
                    .AsEnumerable()
                    .FirstOrDefault();

            if (check != null) return check;

            Console.WriteLine("BulkInsert proc doesn't exist, try to create a new one.");
            sproc = await GraphViewConnection.DocDBclient.CreateStoredProcedureAsync(collectionLink, sproc);
            return sproc;
        }

        public async Task<int> BulkInsertAsync(string sprocLink, dynamic[] objs)
        {
            StoredProcedureResponse<int> scriptResult =
                await
                    GraphViewConnection.DocDBclient.ExecuteStoredProcedureAsync<int>(sprocLink, objs);
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

        public void Dispose()
        {
        }

        public GraphTraversal2 g()
        {
            return new GraphTraversal2(GraphViewConnection, OutputFormat);
        }
    }
}
