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
using System.Runtime.Serialization;


namespace GraphView
{
    [Serializable]
    public class GraphViewCommand : IDisposable, ISerializable
    {
        public GraphViewConnection Connection { get; set; }

        public VertexObjectCache VertexCache { get; private set; }

        public bool InLazyMode { get; set; } = false;
        
        public string CommandText { get; set; }

        public OutputFormat OutputFormat { get; set; }


        private int indexColumnCount;
        public string IndexColumnName => (indexColumnCount++).ToString();

        public GraphViewCommand(GraphViewConnection connection)
        {
            this.Connection = connection;
            this.VertexCache = new VertexObjectCache(this);
        }

        public GraphViewCommand(string commandText)
        {
            CommandText = commandText;
        }

        public GraphViewCommand(string commandText, GraphViewConnection connection)
        {
            CommandText = commandText;
            this.Connection = connection;
        }

        protected GraphViewCommand(SerializationInfo info, StreamingContext context)
        {
            this.Connection = (GraphViewConnection)info.GetValue("Connection", typeof(GraphViewConnection));
            this.InLazyMode = info.GetBoolean("InLazyMode");
            this.OutputFormat = (OutputFormat)info.GetValue("OutputFormat", typeof(OutputFormat));
            this.VertexCache = new VertexObjectCache(this);
            // CommandText and indexColumnCount don't need be serialized and deserialized.
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Connection", this.Connection, typeof(GraphViewConnection));
            info.AddValue("InLazyMode", this.InLazyMode, typeof(bool));
            info.AddValue("OutputFormat", this.OutputFormat, typeof(OutputFormat));
        }

        // we need this method to test command-serialization.
        public void SetCommand(GraphViewCommand command)
        {
            this.Connection = command.Connection;
            this.VertexCache = command.VertexCache;
            this.InLazyMode = command.InLazyMode;
            this.OutputFormat = command.OutputFormat;
            this.CommandText = command.CommandText;
            this.indexColumnCount = command.indexColumnCount;
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
            return new GraphTraversal(this, OutputFormat);
        }
    }
}
