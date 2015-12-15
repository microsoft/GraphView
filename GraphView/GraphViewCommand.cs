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

namespace GraphView
{
    public partial class GraphViewCommand : IDisposable
    {
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
            var visitor = new TranslateMatchClauseVisitor(Connection.Conn);
            visitor.Invoke(script);
            // Executes translated SQL 
            return script.ToString(); ;

        }

        public CommandType CommandType
        {
            get { return Command.CommandType; }
            set { Command.CommandType = value; }
        }
        public GraphViewConnection Connection { get; set; }
        
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
            Connection = connection;
            Command = Connection.Conn.CreateCommand();
        }

        public GraphViewCommand(string commandText, GraphViewConnection connection, SqlTransaction transaction)
        {
            CommandText = commandText;
            Connection = connection;
            Command = Connection.Conn.CreateCommand();
            Command.Transaction = transaction;
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
                    Command.CommandText = CommandText;
                    return Command.ExecuteReader();
                }

                var sr = new StringReader(CommandText);
                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var script = parser.Parse(sr, out errors) as WSqlScript;
                if (errors.Count > 0)
                    throw new SyntaxErrorException(errors);

                // Translation and Check CheckInvisibleColumn
                var visitor = new TranslateMatchClauseVisitor(Connection.Conn);
                visitor.Invoke(script);
                // Executes translated SQL 
                Command.CommandText = script.ToString();
#if DEBUG
                // For debugging
                OutputResult(CommandText, Command.CommandText);
                // For debugging
                //if (!File.Exists(@"D:\GraphView Patter Matching Exp\SqlScript\Test.sql"))
                //{
                //    File.Create(@"D:\GraphView Patter Matching Exp\SqlScript\Test.sql");
                //}
                //FileStream file = new FileStream(@"D:\GraphView Patter Matching Exp\SqlScript\Test.sql", FileMode.Append, FileAccess.Write);
                //StreamWriter sw = new StreamWriter(file, Encoding.UTF8, 20480);
                //sw.WriteLine();
                //sw.WriteLine("go");
                //sw.Flush();
                //sw.WriteLine(cmd.CommandText);
                //sw.WriteLine();
                //sw.Flush();


                //throw new GraphViewException("No Execution");
#endif


                var reader = Command.ExecuteReader();
                return reader;
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
                if (CommandType == CommandType.StoredProcedure)
                {
                    Command.CommandText = CommandText;
                    return Command.ExecuteNonQuery();
                }

                var sr = new StringReader(CommandText);
                var parser = new GraphViewParser();
                IList<ParseError> errors;
                var script = parser.Parse(sr, out errors) as WSqlScript;
                if (errors.Count > 0)
                    throw new SyntaxErrorException(errors);

                // Translation
                var modVisitor = new TranslateDataModificationVisitor(Connection.Conn);
                modVisitor.Invoke(script);
                var matchVisitor = new TranslateMatchClauseVisitor(Connection.Conn);
                matchVisitor.Invoke(script);

                Command.CommandText = script.ToString();
#if DEBUG
                // For debugging
                OutputResult(CommandText, Command.CommandText);
#endif
                return Command.ExecuteNonQuery();
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
