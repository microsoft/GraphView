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
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace GraphView
{
    public partial class GraphViewCommand : IDisposable
    {
        /// <summary>
        /// Returns the translated T-SQL script. For testing only.
        /// </summary>
        /// <returns>The translated T-SQL script</returns>
        internal string GetTsqlQuery()
        {
            var sr = new StringReader(CommandText);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            // Translation and Check CheckInvisibleColumn
            using (SqlTransaction tx = GraphViewConnection.BeginTransaction())
            {
                var visitor = new TranslateMatchClauseVisitor(tx);
                visitor.Invoke(script);

                return script.ToString();
            }
        }

        public CommandType CommandType
        {
            get { return Command.CommandType; }
            set { Command.CommandType = value; }
        }
        public GraphViewConnection GraphViewConnection { get; set; }
        
        public string CommandText { get; set; }

        public int CommandTimeOut
        {
            get { return Command.CommandTimeout; }
            set { Command.CommandTimeout = value; }
        }
        public System.Data.SqlClient.SqlParameterCollection Parameters
        {
            get { return Command.Parameters; }
        }
        internal SqlCommand Command { get; private set; }

        internal SqlTransaction Tx { get; private set; }


        public GraphViewCommand()
        {
        }

        public GraphViewCommand(string commandText)
        {
            CommandText = commandText;
        }

        public GraphViewCommand(string commandText, GraphViewConnection connection)
        {
            CommandText = commandText;
            GraphViewConnection = connection;
            Command = GraphViewConnection.Conn.CreateCommand();
        }

        public GraphViewCommand(string commandText, GraphViewConnection connection, SqlTransaction transaction)
        {
            CommandText = commandText;
            GraphViewConnection = connection;
            Command = GraphViewConnection.Conn.CreateCommand();
            Tx = transaction;
        }

        public void CreateParameter()
        {
            Command.CreateParameter();
        }

        public void Cancel()
        {
            Command.Cancel();
        }

#if DEBUG
        // For debugging
        private void OutputResult(string input, string output)
        {
            Trace.WriteLine("Input string: \n" + input + "\n");
            Trace.WriteLine("Output string: \n" + output);
        }
#endif

        public GraphViewDataReader ExecuteReader()
        {
            try
            {
                var sr = new StringReader(CommandText);
                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var script = parser.Parse(sr, out errors) as WSqlScript;
                if (errors.Count > 0)
                    throw new SyntaxErrorException(errors);

                var DocumentDBConnection = GraphViewConnection;

                DocumentDBConnection.SetupClient();

                foreach (var Batch in script.Batches)
                    foreach (var statement in Batch.Statements)
                        if (statement is WSelectStatement)
                        {
                            var selectStatement = (statement as WSelectStatement);
                            var Query = selectStatement.QueryExpr as WSelectQueryBlock;
                            GraphViewDataReader Reader = new GraphViewDataReader(Query.Generate(DocumentDBConnection));
                            return Reader;
                        }
                return null;
            }
            catch (DocumentClientException e)
            {
                throw new SqlExecutionException("An error occurred when executing the query", e);
            }
        }
        
        public int ExecuteNonQuery()
        {
            try
            {
                var sr = new StringReader(CommandText);
                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var script = parser.Parse(sr, out errors) as WSqlScript;
                if (errors.Count > 0)
                    throw new SyntaxErrorException(errors);
                
                var DocumentDBConnection = GraphViewConnection;

                DocumentDBConnection.SetupClient();

                foreach (var Batch in script.Batches)
                {
                    var DocDB_script = new WSqlScript();
                    DocDB_script.Batches = new List<WSqlBatch>();
                    DocDB_script.Batches.Add(new WSqlBatch());
                    DocDB_script.Batches[0].Statements = new List<WSqlStatement>();

                    foreach (var statement in Batch.Statements)
                    {
                        DocDB_script.Batches[0].Statements.Clear();
                        DocDB_script.Batches[0].Statements.Add(statement);

                        if (statement is WInsertSpecification)
                        {
                            var insertSpecification = (statement as WInsertSpecification);

                            if (insertSpecification.Target.ToString() == "Node")
                            {
                                var insertNodeStatement = new WInsertNodeSpecification(insertSpecification);
                                var Insertop = insertNodeStatement.Generate(DocumentDBConnection);
                                Insertop.Next();
                            }
                            else if (insertSpecification.Target.ToString() == "Edge")
                            {
                                var insertEdgeStatement = new WInsertEdgeSpecification(insertSpecification);
                                var Insertop = insertEdgeStatement.Generate(DocumentDBConnection);
                                Insertop.Next();
                            }
                        }
                        else if (statement is WDeleteSpecification)
                        {
                            var deletespecification = statement as WDeleteSpecification;

                            if (deletespecification is WDeleteEdgeSpecification)
                            {
                                var deleteEdgeStatement = deletespecification as WDeleteEdgeSpecification;
                                var Deleteop = deleteEdgeStatement.Generate(DocumentDBConnection);
                                Deleteop.Next();
                            }
                            else if (deletespecification.Target.ToString() == "Node")
                            {
                                var deleteNodeStatement = new WDeleteNodeSpecification(deletespecification);
                                var Deleteop = deleteNodeStatement.Generate(DocumentDBConnection);
                                Deleteop.Next();
                            }
                        }
                    }
                }

                return 0;
            }
            catch (DocumentClientException e)
            {
                throw new SqlExecutionException("An error occurred when executing the query", e);
            }
        }
        public void Dispose()
        {
            Command.Dispose();
        }
    }
}
