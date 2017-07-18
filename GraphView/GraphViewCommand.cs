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
        public DocumentDBConnection DocumentDbConnection { get; set; }
        
        public string CommandText { get; set; }

        public OutputFormat OutputFormat { get; set; }

        public GraphViewCommand(DocumentDBConnection connecion)
        {
            this.DocumentDbConnection = connecion;
        }

        public GraphViewCommand(string commandText)
        {
            CommandText = commandText;
        }

        public GraphViewCommand(string commandText, DocumentDBConnection connection)
        {
            CommandText = commandText;
            this.DocumentDbConnection = connection;
        }

        public IEnumerable<string> Execute()
        {
            if (CommandText == null)
            {
                throw new QueryExecutionException("CommandText of GraphViewCommand is not set.");
            }
            return g().EvalGremlinTraversal(CommandText);
        }

        public List<string> ExecuteAndGetResults()
        {
            List<string> results = new List<string>();
            foreach (var result in Execute())
            {
                results.Add(result);
            }
            return results;
        }

        public void Dispose()
        {
        }

        public GraphTraversal g()
        {
            return new GraphTraversal(this.DocumentDbConnection, OutputFormat);
        }
    }
}
