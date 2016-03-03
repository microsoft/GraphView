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

    #region  implementation for  IGraphViewCodeTemplateStrategy
    partial class MetaGraphViewCodeTemplate : IGraphViewCodeTemplateStrategy
    {
    }

    partial class NodeTableGraphViewCodeTemplate :IGraphViewCodeTemplateStrategy
    {
        /// <summary>
        /// Dictionary of edge information. For each key-value pair, the key stores edge column name,
        /// the value stores list of edge properties. Each property consists of two strings, indicating
        /// its name and type respectively.
        /// </summary>
        public List<Tuple<string, long, List<Tuple<string, string>>>> EdgeList { get; set; }
    }

    partial class EdgeViewGraphViewCodeTemplate : IGraphViewCodeTemplateStrategy
    {
        public string EdgeName { get; set; }
        public Dictionary<string, string> AttributeTypeDict { get; set; }
        public Dictionary<Tuple<string, string>, List<Tuple<string, string>>> Mapping { get; set; }
        public Dictionary<Tuple<string, string>, long> ColumnId;
    }

    #endregion

    #region implementation for IGraphViewScriptTemplateStrategy 
    partial class DeployScriptTemplate : IGraphViewScriptTemplateStrategy 
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

        public string UserId { get; set; }

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
        public string TransformText(string path)
        {
            Path = ObtainHexStringOfAssembly(path);
            return TransformText();
        }
    }

    partial class EdgeViewBfsScriptTemplateStrategyTemplate: IGraphViewScriptTemplateStrategy
    {
        public string Schema { get; set; }
        public string NodeName { get; set; }
        public string EdgeName { get; set; }
        public string NodeId { get; set; }
        public IList<Tuple<string, string>> Attribute { get; set; }//Edge view attribute list <name, type>
        public List<Tuple<string, string>> EdgeColumn { get; set;}//Edge column list <table, edge column> 

        public string TransformText(string path)
        {
           return TransformText(); 
        }
    }
    #endregion

    #region implementation for  GraphViewDefinedFunctionRegister
    internal class MetaFunctionRegister : GraphViewDefinedFunctionRegister
    {
        public MetaFunctionRegister(string assemblyName)
        {
            CSharpTemplate = new MetaGraphViewCodeTemplate();
            ScriptTemplate = new DeployScriptTemplate
            {
                AssemblyName = assemblyName,
                Type = 2,
                NodeTable = null,
                EdgeList = null,
                InputCount = 1,
                UserId = null
            };
            _exceptionMessage = "Failed to compile meta GraphView defined function";
        }
    }

    internal class NodeTableRegister : GraphViewDefinedFunctionRegister
    {
        public NodeTableRegister(string assemblyName, string nodeTable,
            List<Tuple<string, long, List<Tuple<string, string>>>> edgeList, string userId)
        {
            CSharpTemplate = new NodeTableGraphViewCodeTemplate
            {
               EdgeList =  edgeList
            };
            ScriptTemplate = new DeployScriptTemplate
            {
                AssemblyName = assemblyName,
                Type = 0,
                NodeTable = nodeTable,
                EdgeList = edgeList,
                InputCount = 1,
                UserId = userId
            };
            _exceptionMessage = "Failed to compile nodetable Graph View defined function";
        }
    }

    internal class EdgeViewRegister : GraphViewDefinedFunctionRegister
    {
        public EdgeViewRegister(string nodeName, string schema, string edgeViewName,
            Dictionary<string, string> attributeTypeDictionary,
            Dictionary<Tuple<string, string>, List<Tuple<string, string>>> edgesAttributeMappingDictionary,
            Dictionary<Tuple<string, string>, long> edgeColumnToColumnId)
        {
            CSharpTemplate = new EdgeViewGraphViewCodeTemplate
            {
                EdgeName = edgeViewName,
                AttributeTypeDict = attributeTypeDictionary,
                Mapping = edgesAttributeMappingDictionary,
                ColumnId = edgeColumnToColumnId
            };

            var edgeDictionary = new List<Tuple<string, long, List<Tuple<string, string>>>>
            {
                new Tuple<string, long, List<Tuple<string, string>>>(edgeViewName, 0, attributeTypeDictionary.Select(x => Tuple.Create(x.Key, x.Value)).ToList())
            };
            ScriptTemplate = new DeployScriptTemplate
            {
                AssemblyName = schema + '_' + nodeName,
                Type = 1,
                NodeTable = edgeViewName,
                EdgeList = edgeDictionary,
                InputCount = edgesAttributeMappingDictionary.Count,
                UserId = null
            };

            _exceptionMessage = "Failed to compile EdgeView function";
        }
    }

    internal class EdgeViewBfsRegister : GraphViewDefinedFunctionRegister
    {
        internal EdgeViewBfsRegister(string schema, string tableName, string edge,
            IList<Tuple<string, string>> attribute, List<Tuple<string, string>> edgeColumn)
        {
            CSharpTemplate = null; 
            ScriptTemplate = new EdgeViewBfsScriptTemplateStrategyTemplate()
            {
                Schema = schema,
                NodeName = tableName,
                EdgeName = edge,
                Attribute = attribute,
                EdgeColumn = edgeColumn,
            };
            _exceptionMessage = "";
        }
    }
    #endregion 
}
