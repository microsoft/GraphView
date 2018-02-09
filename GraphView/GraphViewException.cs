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
    public class GraphViewException: Exception
    {
        public GraphViewException() { }
        public GraphViewException(string message) : base(message) { }

        public GraphViewException(string message, Exception innerException) :
            base(message, innerException) { }

        protected GraphViewException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class SyntaxErrorException : GraphViewException
    {
        public SyntaxErrorException() { }
        public SyntaxErrorException(string message) : base(message) { }
        public SyntaxErrorException(string message, Exception innerException) :
            base(message, innerException) { }
        protected SyntaxErrorException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public SyntaxErrorException(int line, string key, string mes)
            : base("\nLine " + line + ":\n" + "Incorrect syntax near " + key + ": " + mes + "\n")
        {
        }

        public SyntaxErrorException(int line, string key)
            : base("\nLine " + line + ":\n" + "Incorrect syntax near " + key + "\n")
        {
        }
    }

    [Serializable]
    public class TranslationException : GraphViewException
    {
        public TranslationException() { }
        public TranslationException(string message) : base(message) { }
        public TranslationException(string message, Exception innerException) :
            base(message, innerException)
        { }
    }

    [Serializable]
    public class QueryCompilationException : GraphViewException
    {
        public QueryCompilationException() { }
        public QueryCompilationException(string message) : base(message) { }
        public QueryCompilationException(string message, Exception innerException) :
            base(message, innerException) { }
    }

    [Serializable]
    public class QueryExecutionException : GraphViewException
    {
        public QueryExecutionException() { }
        public QueryExecutionException(string message) : base(message) { }
        public QueryExecutionException(string message, Exception innerException)
            : base (message, innerException) { }
    }

    [Serializable]
    public class SqlExecutionException : GraphViewException
    {
        public SqlExecutionException() { }
        public SqlExecutionException(string message) : base(message) { }

        public SqlExecutionException(string message, Exception innerException) :
            base(message, innerException) { }

        protected SqlExecutionException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class BulkInsertNodeException : GraphViewException
    {
        public BulkInsertNodeException () { }
        public BulkInsertNodeException(string message) : base(message) { }

        public BulkInsertNodeException(string message, Exception innerException) :
            base(message, innerException) { }

        protected BulkInsertNodeException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class BulkInsertEdgeException : GraphViewException
    {
        public BulkInsertEdgeException() { }
        public BulkInsertEdgeException(string message) : base(message) { }

        public BulkInsertEdgeException(string message, Exception innerException) :
            base(message, innerException) { }

        protected BulkInsertEdgeException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class EdgeViewException : GraphViewException
    {
        public EdgeViewException() { }
        public EdgeViewException(string message) : base(message) { }

        public EdgeViewException(string message, Exception innerException) :
            base(message, innerException) { }

        protected EdgeViewException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class NodeViewException : GraphViewException
    {
        public NodeViewException() { }
        public NodeViewException(string message) : base(message) { }

        public NodeViewException(string message, Exception innerException) :
            base(message, innerException) { }

        protected NodeViewException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    public class RecordServiceException : GraphViewException
    {
        public RecordServiceException() { }
        public RecordServiceException(string message) : base(message) { }

        public RecordServiceException(string message, Exception innerException) :
            base(message, innerException)
        { }

        protected RecordServiceException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}
