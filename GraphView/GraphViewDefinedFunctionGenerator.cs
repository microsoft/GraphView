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
using System.Security.Authentication.ExtendedProtection;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{

    partial class MetaGraphViewDefinedFunctionTemplate
    {
    }

    partial class NodeTableGraphViewDefinedFunctionTemplate
    {
        /// <summary>
        /// Dictionary of edge information. For each key-value pair, the key stores edge column name,
        /// the value stores list of edge properties. Each property consists of two strings, indicating
        /// its name and type respectively.
        /// </summary>
        public List<Tuple<string, long, List<Tuple<string, string>>>> EdgeList { get; set; }
    }

    partial class EdgeViewGraphViewDefinedFunctionTemplate
    {
        public string EdgeName { get; set; }
        public Dictionary<string, string> AttributeTypeDict { get; set; }
        public Dictionary<Tuple<string, string>, List<Tuple<string, string>>> Mapping { get; set; }
        public Dictionary<Tuple<string, string>, long> ColumnId;
    }

    partial class DeployScriptTemplate
    {
        public string AssemblyName { get; set; }

        public string Path { get; set; }

        /// <summary>
        /// Dictionary of edge information. For each key-value pair, the key stores edge column name,
        /// the value stores list of edge properties. Each property consists of two strings, indicating
        /// its name and type respectively.
        /// </summary>
        public List<Tuple<string, long, List<Tuple<string, string>>>> EdgeList { get; set; }

        public int InputCount { get; set; }

        public int Type;

        public string NodeTable { get; set; }
    }

    partial class EdgeViewBfsScriptTemplate
    {
        public string Schema { get; set; }
        public string NodeName { get; set; }
        public string EdgeName { get; set; }
        public IList<Tuple<string, string>> Attribute { get; set; }//Edge view attribute list <name, type>
        public List<Tuple<string, string>> EdgeColumn { get; set;}//Edge column list <table, edge column> 
    }


    public static class GraphViewDefinedFunctionGenerator
    {
        private static string GenerateMetaGraphViewDefinedDecoderFunction()
        {
            var template = new MetaGraphViewDefinedFunctionTemplate
            {
            };
            return template.TransformText();
        }

        private static string GenerateNodeTableGraphViewDefinedFunction(List<Tuple<string, long, List<Tuple<string, string>>>> edgeList)
        {
            var template = new NodeTableGraphViewDefinedFunctionTemplate
            {
                EdgeList = edgeList
            };
            return template.TransformText();
        }

        private static string GenerateEdgeViewGraphViewDefinedEdgeFunction(string edgeViewName, Dictionary<string, string> attributetypeDictionary,
            Dictionary<Tuple<string, string>, List<Tuple<string, string>>>  edgesAttributeMappingDictionary, Dictionary<Tuple<string, string>, long> edgeColumnToColumnId)
        {
            var template = new EdgeViewGraphViewDefinedFunctionTemplate
            {
                EdgeName = edgeViewName,
                AttributeTypeDict = attributetypeDictionary,
                Mapping = edgesAttributeMappingDictionary,
                ColumnId = edgeColumnToColumnId
            };
            return template.TransformText();
        }

        /// <summary> Encodes the binary bits of the assembly DLL into a 
        /// string containing a hexadecimal number. </summary>
        /// <param name="fullPathToAssembly"
        /// >Full directory path plus the file name, to the .DLL file.</param>
        /// <returns>A string containing a hexadecimal number that encodes
        /// the binary bits of the .DLL file.</returns>
        static private string ObtainHexStringOfAssembly
            (string fullPathToAssembly)
        {
            StringBuilder sbuilder = new StringBuilder("0x");
            using (FileStream fileStream = new FileStream(
                fullPathToAssembly,
                FileMode.Open, FileAccess.Read, FileShare.Read)
                )
            {
                int byteAsInt;
                while (true)
                {
                    byteAsInt = fileStream.ReadByte();
                    if (-1 >= byteAsInt) { break; }
                    sbuilder.Append(byteAsInt.ToString
                        ("X2", CultureInfo.InvariantCulture));
                }
            }
            return sbuilder.ToString();
        }

        private static string GenerateRegisterScript(string assemblyName, string path, int type, string nodeTable = null,
            List<Tuple<string, long, List<Tuple<string, string>>>> edgeList = null,  int inputCount = 1)
        {
            var template = new DeployScriptTemplate
            {
                AssemblyName = assemblyName,
                EdgeList = edgeList,
                Path = ObtainHexStringOfAssembly(path),
                InputCount = inputCount,
                Type = type,
                NodeTable = nodeTable
            };
            return template.TransformText();
        }

        private static string GenerateEdgeViewBFSRegisterScript(string schema, string tableName, string edge,
            IList<Tuple<string, string>> attribute, List<Tuple<string, string>> edgeColumn)
        {
            var template = new EdgeViewBfsScriptTemplate()
            {
                Schema = schema,
                NodeName = tableName,
                EdgeName = edge,
                Attribute = attribute,
                EdgeColumn = edgeColumn,
            };
            return template.TransformText();
        }

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
            var result = codeProvider.CompileAssemblyFromSource(parameters, code);
            
            return result;
        }

        public static void MetaRegister(
            string assemblyName,
            SqlConnection conn,
            SqlTransaction tx
            )
        {
            var code = GenerateMetaGraphViewDefinedDecoderFunction();
            var result = Compile(code);
            if (result.Errors.Count > 0)
                throw new GraphViewException("Failed to compile meta GraphView defined function");
            var script = GenerateRegisterScript(assemblyName, result.PathToAssembly, 2);

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

        public static void NodeTableRegister(
            string assemblyName, string nodeTable,
            List<Tuple<string, long, List<Tuple<string, string>>>> edgeList,
            SqlConnection conn,
            SqlTransaction tx
            )
        {

            var code = GenerateNodeTableGraphViewDefinedFunction(edgeList);
            var result = Compile(code);
            if (result.Errors.Count > 0)
                throw new GraphViewException("Failed to compile nodetable Graph View defined function");
            var script = GenerateRegisterScript(assemblyName, result.PathToAssembly, 0, nodeTable, edgeList);

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

        public static void EdgeViewRegister(string suppernode, string schema, string edgeViewName, Dictionary<string, string> attributetypeDictionary,
            Dictionary<Tuple<string, string>, List<Tuple<string, string>>> edgesAttributeMappingDictionary, Dictionary<Tuple<string, string>, long> edgeColumnToColumnId,
            SqlConnection conn, SqlTransaction tx)
        {

            var code = GenerateEdgeViewGraphViewDefinedEdgeFunction(edgeViewName, attributetypeDictionary,
                edgesAttributeMappingDictionary, edgeColumnToColumnId);
            var result = Compile(code);
            if (result.Errors.Count > 0)
                throw new GraphViewException("Failed to compile function");

            var edgeDictionary = new List<Tuple<string, long, List<Tuple<string, string>>>>
            {
                new Tuple<string, long, List<Tuple<string, string>>>(edgeViewName, 0, attributetypeDictionary.Select(x => Tuple.Create(x.Key, x.Value)).ToList())
            };
            var script = GenerateRegisterScript(schema + '_' + suppernode, result.PathToAssembly, 1, edgeViewName, edgeDictionary, edgesAttributeMappingDictionary.Count());
            //var script = GenerateRegisterScript(schema, result.PathToAssembly, 1, edgeViewName, edgeDictionary, edgesAttributeMappingDictionary.Count());

            var query = script.Split(new string[] {"GO"}, StringSplitOptions.None);
            var command = conn.CreateCommand();

            command.Connection = conn;
            command.Transaction = tx;

            int i = 0;
            foreach (var s in query)
            {
                if (s == null) continue;
                command.CommandText = s;
                command.ExecuteNonQuery();
                i++;
            }
        }

        public static void EdgeViewBfsRegister(string schema, string tableName, string edge,
            IList<Tuple<string, string>> attribute, List<Tuple<string, string>> edgeColumn,
            SqlConnection conn, SqlTransaction tx)
        {
            
            var script = GenerateEdgeViewBFSRegisterScript(schema, tableName, edge, attribute, edgeColumn);
            var command = conn.CreateCommand();
            command.Connection = conn;
            command.Transaction = tx;
            command.CommandText = script;
            command.ExecuteNonQuery();
        }
    }
}
