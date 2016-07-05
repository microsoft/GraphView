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
        public SqlParameterCollection Parameters
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

        public SqlDataReader ExecuteReader()
        {
            try
            {
                if (CommandType == CommandType.StoredProcedure)
                {
                    if (Tx != null)
                    {
                        Command.Transaction = Tx;
                    }
                    Command.CommandText = CommandText;
                    return Command.ExecuteReader();
                }

                var sr = new StringReader(CommandText);
                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var script = parser.Parse(sr, out errors) as WSqlScript;
                if (errors.Count > 0)
                    throw new SyntaxErrorException(errors);

                if (Tx == null)
                {
                    var translationConnection = GraphViewConnection.TranslationConnection;

                    using (SqlTransaction translationTx = translationConnection.BeginTransaction(System.Data.IsolationLevel.RepeatableRead))
                    {
                        var visitor = new TranslateMatchClauseVisitor(translationTx);
                        visitor.Invoke(script);

                        // Executes translated SQL 
                        Command.CommandText = script.ToString();
#if DEBUG
                        // For debugging
                        OutputResult(CommandText, Command.CommandText);
                        //throw new GraphViewException("No Execution");
#endif
                        var reader = Command.ExecuteReader();
                        translationTx.Commit();
                        return reader;
                    }
                }
                else
                {
                    var visitor = new TranslateMatchClauseVisitor(Tx);
                    visitor.Invoke(script);
                    // Executes translated SQL 
                    Command.CommandText = script.ToString();
#if DEBUG
                    // For debugging
                    OutputResult(CommandText, Command.CommandText);
                    //throw new GraphViewException("No Execution");
#endif
                    var reader = Command.ExecuteReader();
                    return reader;
                }
            }
            catch (SqlException e)
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
                
                var DocDB_conn = GraphViewConnection;

                DocDB_conn.createclient();

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
                        
                        if (statement is WSelectStatement)
                        {
                            var selectStatement = (statement as WSelectStatement);
                            var res = "";
                            var Query = selectStatement.QueryExpr as WSelectQueryBlock;
                            Query.Generate(DocDB_conn).Next();
                            Console.WriteLine(res);
                            DocDB_conn.DocDB_finish = true;
                        }
                        if (statement is WInsertSpecification)
                        {
                            var insertSpecification = (statement as WInsertSpecification);

                            if (insertSpecification.Target.ToString() == "Node")
                            {
                                var insertNodeStatement = new WInsertNodeSpecification(insertSpecification);
                                insertNodeStatement.RunDocDbScript(DocDB_conn);
                            }
                            else if (insertSpecification.Target.ToString() == "Edge")
                            {
                                var insertEdgeStatement = new WInsertEdgeSpecification(insertSpecification);
                                insertEdgeStatement.RunDocDbScript(DocDB_conn);
                            }
                        }
                        else if (statement is WDeleteSpecification)
                        {
                            var deletespecification = statement as WDeleteSpecification;

                            if (deletespecification is WDeleteEdgeSpecification)
                            {
                                var deleteEdgeStatement = deletespecification as WDeleteEdgeSpecification;
                                deleteEdgeStatement.RunDocDbScript(DocDB_conn);
                            }
                            else if (deletespecification.Target.ToString() == "Node")
                            {
                                var deleteNodeStatement = new WDeleteNodeSpecification(deletespecification);
                                deleteNodeStatement.RunDocDbScript(DocDB_conn);
                            }
                        }
                        //keep waiting until finish.
                        while (DocDB_conn.DocDB_finish == false)
                        {
                            System.Threading.Thread.Sleep(100);
                        }

                    }
                }

                return 0;
            }
            catch (SqlException e)
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
