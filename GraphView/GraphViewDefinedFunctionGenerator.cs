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
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.CSharp;
using System.Text;
using System.IO;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Authentication.ExtendedProtection;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    //Generate code for user defined function
    interface IGraphViewCodeTemplateStrategy
    {
        string TransformText();
    }

    //Generate sql Script for UDF
    interface IGraphViewScriptTemplateStrategy
    {
        string TransformText();
        string TransformText(string path);
    }

    //Register class for UDF
    internal abstract class GraphViewDefinedFunctionRegister
    {
        protected IGraphViewCodeTemplateStrategy CSharpTemplate;
        protected IGraphViewScriptTemplateStrategy ScriptTemplate;
        protected string _exceptionMessage;

        private static CompilerResults Compile(string code)
        {
            var codeProvider = new CSharpCodeProvider();
            var parameters = new CompilerParameters
            {
                GenerateExecutable = false,
#if DEBUG
                IncludeDebugInformation = true,
#else
                IncludeDebugInformation = false,
#endif
            };
            parameters.ReferencedAssemblies.Add("System.dll");
            parameters.ReferencedAssemblies.Add("System.Data.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.dll");
            parameters.ReferencedAssemblies.Add("System.Core.dll");
            parameters.CompilerOptions = "/optimize";
            var result = codeProvider.CompileAssemblyFromSource(parameters, code);

            return result;
        }

        public void Register(SqlConnection conn, SqlTransaction tx)
        {
            string script;

            //Complies C# code and generate compile result
            if (CSharpTemplate != null)
            {
                var code = CSharpTemplate.TransformText();
                var result = Compile(code);
                if (result.Errors.Count > 0)
                    throw new GraphViewException(_exceptionMessage);
                script = ScriptTemplate.TransformText(result.PathToAssembly);
            }
            else
            {
                script = ScriptTemplate.TransformText(null);
            }

            //Generates Sql Server script to register UDF
            var query = script.Split(new string[] {"GO"}, StringSplitOptions.None);
            var command = conn.CreateCommand();

            command.Connection = conn;
            command.Transaction = tx;

            foreach (var s in query)
            {
                if (s == null) continue;
                command.CommandText = s;
                command.ExecuteNonQuery();
            }
        }
    }
    
}
