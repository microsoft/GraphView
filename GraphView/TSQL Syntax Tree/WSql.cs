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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public abstract partial class WSqlFragment 
    {
        public int FirstTokenIndex { get; set; }
        public int LastTokenIndex { get; set; }

        internal void UpdateTokenInfo(WSqlFragment fragment)
        {
            if (fragment == null)
                return;
            UpdateTokenInfo(fragment.FirstTokenIndex, fragment.LastTokenIndex);
        }
        
        internal void UpdateTokenInfo(TSqlFragment fragment)
        {
            if (fragment == null)
                return;
            UpdateTokenInfo(fragment.FirstTokenIndex, fragment.LastTokenIndex);
        }

        internal void UpdateTokenInfo(int firstIndex, int lastIndex)
        {
            if (firstIndex < 0 || lastIndex < 0)
                return;
            if (firstIndex > lastIndex)
            {
                var num = firstIndex;
                firstIndex = lastIndex;
                lastIndex = num;
            }
            if (firstIndex < FirstTokenIndex || FirstTokenIndex == -1)
                FirstTokenIndex = firstIndex;
            if (lastIndex <= LastTokenIndex && LastTokenIndex != -1)
                return;
            LastTokenIndex = lastIndex;
        }

        internal virtual bool OneLine()
        {
            return false;
        }

        internal virtual string ToString(string indent)
        {
            return "";
        }

        public override string ToString()
        {
            return ToString("");
        }

        public virtual void Accept(WSqlFragmentVisitor visitor)
        {
        }

        public virtual void AcceptChildren(WSqlFragmentVisitor visitor)
        {
        }

        public virtual string ToDocDbScript(GraphViewConnection docDbConnection)
        {
            return "";
        }
        public virtual async Task RunDocDbScript(GraphViewConnection docDbConnection)
        {

        }

        public string DocDBScript_head(string EndpointUrl, string AuthorizationKey, string DatabaseID, string CollectionID)
        {
            string ans = @"
namespace ConsoleApplication1
{
    using System;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Collections;
    using System.IO;
    using System.Text;

    // Add DocumentDB references
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class Program
    {
        // Read the DocumentDB endpointUrl and authorizationKey from config file
        // WARNING: Never store credentials in source code
        // For more information, visit http://azure.microsoft.com/blog/2013/07/17/windows-azure-web-sites-how-application-strings-and-connection-strings-work/
        private const string EndpointUrl = """ + EndpointUrl + @""";
        private const string AuthorizationKey = """ + AuthorizationKey + @""";
        private static DocumentClient client;
        private static Database database;
        private static DocumentCollection documentCollection;

        public static void Main()
        {
            try
            {
                GetStartedDemo().Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine(""{0} error occurred: {1}, Message: {2}"", de.StatusCode, de.Message, baseException.Message);
                //return de.StatusCode +""---------------->""+ de.Message +""---------------->""+ baseException.Message;
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine(""Error: {0}, Message: {1}"", e.Message, baseException.Message);
                FileStream aFile = new FileStream(""D:\\file.txt"", FileMode.Create);
                StreamWriter File = new StreamWriter(aFile);
                File.Write(e.Message +""---------------->""+ baseException.Message);
                File.Close();
            }
            finally
            {
                //Console.WriteLine(""End of demo, press any key to exit."");
                //Console.ReadKey();
            }
        }

        public static void insert_reader(ref StringBuilder s1, JsonTextReader reader, ref JsonWriter writer)
            {

                switch (reader.TokenType)
                {
                    case JsonToken.StartArray:
                        writer.WriteStartArray();
                        break;
                    case JsonToken.EndArray:
                        writer.WriteEnd();
                        break;
                    case JsonToken.PropertyName:
                        writer.WritePropertyName(reader.Value.ToString());
                        break;
                    case JsonToken.String:
                        writer.WriteValue(reader.Value);
                        break;
                    case JsonToken.Integer:
                        writer.WriteValue(reader.Value);
                        break;
                    case JsonToken.Comment:
                        writer.WriteComment(reader.Value.ToString());
                        break;
                    case JsonToken.StartObject:
                        writer.WriteStartObject();
                        break;
                    case JsonToken.EndObject:
                        writer.WriteEndObject();
                        break;
                    case JsonToken.Null:
                        writer.WriteNull();
                        break;
                    case JsonToken.Float:
                        writer.WriteValue(reader.Value);
                        break;
                }
            }
            //insert s2 into s1 's end
            public static void insert_string(ref StringBuilder s1, string s2, ref JsonWriter writer, bool isObject)
            {
                JsonTextReader reader = new JsonTextReader(new StringReader(s2));

                while (reader.Read())
                {
                    switch (reader.TokenType)
                    {
                        case JsonToken.StartObject:
                            if (isObject) insert_reader(ref s1, reader, ref writer);
                            break;
                        case JsonToken.EndObject:
                            if (isObject) insert_reader(ref s1, reader, ref writer);
                            break;
                        default:
                            insert_reader(ref s1, reader, ref writer);
                            break;
                    }
                }
            }
            //insert json_str_s2 into json_str_s1 's  s3
            //and return the ans
            public static StringBuilder insert_array_element(string s1, string s2, string s3)
            {
                bool find = false;
                Stack sta = new Stack();


                StringBuilder sb = new StringBuilder();
                StringWriter sw = new StringWriter(sb);
                JsonWriter writer = new JsonTextWriter(sw);


                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.StartArray:
                            writer.WriteStartArray();
                            if (find)
                                sta.Push(1);
                            break;

                        case JsonToken.EndArray:
                            if (find)
                                sta.Pop();
                            if (find && sta.Count == 0)
                            {
                                insert_string(ref sb, s2, ref writer, false);
                                find = false;
                            }
                            writer.WriteEnd();
                            break;

                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == s3)
                                find = true;
                            Console.WriteLine(reader1.Value.ToString());
                            insert_reader(ref sb, reader1, ref writer);
                            break;


                        default:
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                    }
                }

                return sb;
            }
            //use json_str_s2 replace json_str_s1 's property s3 
            //if there is no property s3 , create one
            public static StringBuilder insert_property(string s1, string s2, string s3)
            {
                bool find = false;
                bool flag = false;
                Stack sta = new Stack();


                StringBuilder sb = new StringBuilder();
                StringWriter sw = new StringWriter(sb);
                JsonWriter writer = new JsonTextWriter(sw);


                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == s3)
                                find = flag = true;
                            insert_reader(ref sb, reader1, ref writer);
                            if (find)
                            {
                                find = false;
                                insert_string(ref sb, s2, ref writer, false);
                                reader1.Read();
                            }
                            break;
                        case JsonToken.EndObject:
                            sta.Pop();
                            if (!flag && sta.Count == 0)
                            {
                                writer.WritePropertyName(s3);
                                insert_string(ref sb, s2, ref writer, false);
                            }
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                        case JsonToken.StartObject:
                            sta.Push(1);
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                        default:
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                    }
                }

                return sb;
            }

            public static int get_reverse_edge_num(string s1)
            {
                bool flag = false;
                int now = 1;


                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == ""_reverse_edge"")
                                flag = true;
                            break;
                        case JsonToken.StartArray:
                            if (flag)
                            {
                                reader1.Read();
                                if (reader1.TokenType == JsonToken.EndArray)
                                    return 1;
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value == now)
                                    now++;
                                else
                                    return now;
                            }
                            break;
                        case JsonToken.EndArray:
                            if (flag)
                                return now;
                            break;
                        case JsonToken.StartObject:
                            if (flag)
                            {
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value == now)
                                    now++;
                                else
                                    return now;
                            }
                            break;
                    }
                }
                return 1;
            }
            public static int get_edge_num(string s1)
            {
                bool flag = false;
                int now = 1;


                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == ""_edge"")
                                flag = true;
                            break;
                        case JsonToken.StartArray:
                            if (flag)
                            {
                                reader1.Read();
                                if (reader1.TokenType == JsonToken.EndArray)
                                    return 1;
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value == now)
                                    now++;
                                else
                                    return now;
                            }
                            break;
                        case JsonToken.EndArray:
                            if (flag)
                                return now;
                            break;
                        case JsonToken.StartObject:
                            if (flag)
                            {
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value == now)
                                    now++;
                                else
                                    return now;
                            }
                            break;
                    }
                }
                return 1;
            }
            public static StringBuilder insert_reverse_edge(string s1, string s2, int num)
            {
                bool find = false;
                bool Write = false;

                StringBuilder sb = new StringBuilder();
                StringWriter sw = new StringWriter(sb);
                JsonWriter writer = new JsonTextWriter(sw);

                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == ""_reverse_edge"")
                                find = true;
                            insert_reader(ref sb, reader1, ref writer);
                            //if (find)
                            //{
                            //    find = false;
                            //    insert_string(ref sb, s2, ref writer);
                            //    reader1.Read();
                            //}
                            break;
                        case JsonToken.StartArray:
                            insert_reader(ref sb, reader1, ref writer);
                            if (find)
                            {
                                if (num == 1)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                            }
                            break;
                        case JsonToken.StartObject:
                            if (!find || Write)
                                insert_reader(ref sb, reader1, ref writer);
                            else
                            {
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value > num)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                                writer.WriteStartObject();
                                writer.WritePropertyName(""_ID"");
                                insert_reader(ref sb, reader1, ref writer);
                            }
                            break;
                        case JsonToken.EndArray:
                            if (find)
                            {
                                find = false;
                                if (!Write)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                            }
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                        default:
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                    }
                }

                return sb;
            }
            public static StringBuilder insert_edge(string s1, string s2, int num)
            {
                bool find = false;
                bool Write = false;

                StringBuilder sb = new StringBuilder();
                StringWriter sw = new StringWriter(sb);
                JsonWriter writer = new JsonTextWriter(sw);

                JsonTextReader reader1 = new JsonTextReader(new StringReader(s1));
                while (reader1.Read())
                {
                    switch (reader1.TokenType)
                    {
                        case JsonToken.PropertyName:
                            if (reader1.Value.ToString() == ""_edge"")
                                find = true;
                            insert_reader(ref sb, reader1, ref writer);
                            //if (find)
                            //{
                            //    find = false;
                            //    insert_string(ref sb, s2, ref writer);
                            //    reader1.Read();
                            //}
                            break;
                        case JsonToken.StartArray:
                            insert_reader(ref sb, reader1, ref writer);
                            if (find)
                            {
                                if (num == 1)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                            }
                            break;
                        case JsonToken.StartObject:
                            if (!find || Write)
                                insert_reader(ref sb, reader1, ref writer);
                            else
                            {
                                reader1.Read();
                                reader1.Read();
                                if ((long)reader1.Value > num)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                                writer.WriteStartObject();
                                writer.WritePropertyName(""_ID"");
                                insert_reader(ref sb, reader1, ref writer);
                            }
                            break;
                        case JsonToken.EndArray:
                            if (find)
                            {
                                find = false;
                                if (!Write)
                                {
                                    insert_string(ref sb, s2, ref writer, true);
                                    Write = true;
                                }
                            }
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                        default:
                            insert_reader(ref sb, reader1, ref writer);
                            break;
                    }
                }

                return sb;
            }

            public static async Task INSERT_EDGE(Object source , Object sink , string Edge, string sourceid, string sinkid)
            {
                string source_str = JsonConvert.SerializeObject(source);
                string sink_str = JsonConvert.SerializeObject(sink);
                var source_edge_num = get_edge_num(source_str);
                var source_reverse_edge_num = get_reverse_edge_num(source_str);
                var sink_edge_num = get_edge_num(sink_str);
                var sink_reverse_edge_num = get_reverse_edge_num(sink_str);

                Edge = insert_property(Edge, source_edge_num.ToString(), ""_ID"").ToString();
                Edge = insert_property(Edge, sink_reverse_edge_num.ToString(), ""_reverse_ID"").ToString();
                Edge = insert_property(Edge, '\""' + sinkid + '\""', ""_sink"").ToString();
                source_str = insert_edge(source_str, Edge, source_edge_num).ToString();
                var new_source = JObject.Parse(source_str);

                Edge = insert_property(Edge, sink_reverse_edge_num.ToString(), ""_ID"").ToString();
                Edge = insert_property(Edge, source_edge_num.ToString(), ""_reverse_ID"").ToString();
                Edge = insert_property(Edge, '\""' + sourceid + '\""', ""_sink"").ToString();
                sink_str = insert_reverse_edge(sink_str, Edge, sink_reverse_edge_num).ToString();
                var new_sink = JObject.Parse(sink_str);

                await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(database.Id, documentCollection.Id, sourceid), new_source);
                await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(database.Id, documentCollection.Id, sinkid), new_sink);
            }

            public static async Task Delete_Node(string id)
            {
                var docLink = string.Format(""dbs/{0}/colls/{1}/docs/{2}"",database.Id, documentCollection.Id, id);
                await client.DeleteDocumentAsync(docLink);
            }

        private static async Task GetStartedDemo()
        {
            // Create a new instance of the DocumentClient
            client = new DocumentClient(new Uri(EndpointUrl), AuthorizationKey);

            // Check to verify a database with the id=Graphview_DocDB does not exist
            database =
                client.CreateDatabaseQuery().Where(db => db.Id == """ + DatabaseID + @""").AsEnumerable().FirstOrDefault();


            // If the database does not exist, create a new database
            if (database == null)
            {
                database = await client.CreateDatabaseAsync(
                    new Database
                    {
                        Id = """ + DatabaseID + @"""
                    });

                Console.WriteLine(""Created dbs"");
            }

            // Check to verify a document collection with the id=GraphOne does not exist
            documentCollection =
                client.CreateDocumentCollectionQuery(""dbs/"" + database.Id)
                    .Where(c => c.Id == """ + CollectionID + @""")
                    .AsEnumerable()
                    .FirstOrDefault();

            // If the document collection does not exist, create a new collection
            if (documentCollection == null)
            {
                documentCollection = await client.CreateDocumentCollectionAsync(""dbs/"" + database.Id,
                    new DocumentCollection
                    {
                        Id = """ + CollectionID + @"""
                    });

                Console.WriteLine(""Created dbs/Graphview_DocDB/colls/GraphOne"");
            }
  
            ";
            return ans;
        }
        public string DocDBScript_tail()
        {
            string ans = @"
        }

    }
}
            ";
            return ans;
        }
    }

    public partial class WSqlScript : WSqlFragment
    {
        internal List<WSqlBatch> Batches { set; get; }

        internal override bool OneLine()
        {
            return Batches.Count == 1 && Batches[0].OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            for (var i = 0; i < Batches.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append("\r\n");
                }
                sb.Append(Batches[i].ToString(indent));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Batches != null)
            {
                var index = 0;
                for (var count = Batches.Count; index < count; ++index)
                    Batches[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSqlBatch : WSqlFragment
    {
        internal List<WSqlStatement> Statements { set; get; }

        internal override bool OneLine()
        {
            return Statements.Count == 1 && Statements[0].OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < Statements.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append("\r\n");
                }
                sb.Append(Statements[i].ToString(indent));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Statements != null)
            {
                var index = 0;
                for (var count = Statements.Count; index < count; ++index)
                    Statements[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }


    public abstract partial class WSqlStatement : WSqlFragment 
    {
        internal static string StatementListToString(IList<WSqlStatement> statementList, string indent)
        {
            if (statementList == null || statementList.Count == 0)
            {
                return "";
            }

            var sb = new StringBuilder(1024);

            for (var i = 0; i < statementList.Count; i++)
            {
                sb.AppendFormat("{0}", statementList[i].ToString(indent));

                if (sb[sb.Length - 1] != ';' && !(statementList[i] is WCommonTableExpression || 
                                                  statementList[i] is WMultiCommonTableExpression))
                {
                    sb.Append(';');
                }

                if (i < statementList.Count - 1)
                {
                    sb.Append("\r\n");
                }
            }

            return sb.ToString();
        }
    }

    /// <summary>
    /// Statements with optimization hints
    /// </summary>
    public abstract partial class WStatementWithCtesAndXmlNamespaces : WSqlStatement
    {
        public IList<OptimizerHint> OptimizerHints { get; set; }

        // Turns T-SQL OptimizerHint into string 
        internal static string OptimizerHintToString(OptimizerHint hint)
        {
            var sb = new StringBuilder(1024);
            // Literal hint
            if (hint is LiteralOptimizerHint)
            {
                sb.Append(TsqlFragmentToString.OptimizerHintKind(hint.HintKind));
                var loh = hint as LiteralOptimizerHint;

                // TODO: Only support numeric literal
                sb.AppendFormat(" {0}",loh.Value.Value);
            }
            // OptimizeFor hint
            else if (hint is OptimizeForOptimizerHint)
            {
                var ooh = hint as OptimizeForOptimizerHint;
                sb.AppendFormat("OPTIMIZE FOR ");
                if (ooh.IsForUnknown)
                    sb.Append("UNKNOWN");
                else
                {
                    sb.Append("(");
                    for (int i = 0; i < ooh.Pairs.Count; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");
                        sb.Append(ooh.Pairs[i].Variable.Name);

                        // TODO: Only support value expression
                        if (ooh.Pairs[i].Value != null && ooh.Pairs[i].Value is Literal)
                        {
                            if (ooh.Pairs[i].Value is StringLiteral)
                                sb.AppendFormat(" = '{0}'", ((Literal)ooh.Pairs[i].Value).Value);
                            else
                                sb.AppendFormat(" = {0}", ((Literal)ooh.Pairs[i].Value).Value);
                        }
                        else
                            sb.Append(" UNKNOWN");
                    }
                    sb.Append(")");

                }
            }
            // Table hint
            else if (hint is TableHintsOptimizerHint)
            {
                var toh = hint as TableHintsOptimizerHint;
                sb.Append("TABLE HINT ");
                sb.Append("(");
                sb.Append(TsqlFragmentToString.SchemaObjectName(toh.ObjectName));
                for (int i = 0; i < toh.TableHints.Count; i++)
                {
                    if (i > 0)
                        sb.Append(", ");
                    // TODO: Table hint in WSQL Syntax tree is incomplete
                    sb.AppendFormat("@{0}", toh.TableHints[i].HintKind.ToString());
                }
                sb.Append(")");

            }
            // Regular hint
            else
            {
                sb.Append(TsqlFragmentToString.OptimizerHintKind(hint.HintKind));
            }
            return sb.ToString();

        }

        // Tranlates optimizer hint list into string
        internal string OptimizerHintListToString(string indent="")
        {
            if (OptimizerHints == null || !OptimizerHints.Any())
                return "";
            var sb = new StringBuilder(1024);
            sb.Append("\r\n");
            sb.AppendFormat("{0}OPTION", indent);
            sb.Append("(");
            for (int i = 0; i < OptimizerHints.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.AppendFormat("{0}", OptimizerHintToString(OptimizerHints[i]));
            }
            sb.Append(")");

            return sb.ToString();
        }
    }

    /// <summary>
    /// This class represents all T-SQL statements not identified by the current parser.
    /// Unidentified statements are interpreted token by token. 
    /// </summary>
    public partial class WSqlUnknownStatement : WSqlStatement
    {
        public IList<TSqlParserToken> TokenStream { get; set; }
        public string Statement { get; set; }

        public WSqlUnknownStatement()
        {
            TokenStream = new List<TSqlParserToken>();
        }

        public WSqlUnknownStatement(string statement)
        {
            Statement = statement;
        }

        public WSqlUnknownStatement(TSqlFragment statement)
        {
            TokenStream = new List<TSqlParserToken>(statement.LastTokenIndex - statement.FirstTokenIndex + 1);

            for (var pos = statement.FirstTokenIndex; pos <= statement.LastTokenIndex; pos++)
            {
                TokenStream.Add(statement.ScriptTokenStream[pos]);
            }
        }

        internal override string ToString(string indent)
        {
            var newLine = true;

            if (Statement != null) return indent + Statement;
            var sb = new StringBuilder(TokenStream.Count * 8);
            foreach (var token in TokenStream)
            {
                if (newLine)
                {
                    sb.Append(indent);
                }
                sb.Append(token.Text);

                newLine = false;

                if (token.TokenType != TSqlTokenType.WhiteSpace)
                    continue;
                if (token.Text.Equals("\r\n") || token.Text.Equals("\n"))
                {
                    newLine = true;
                }
            }
            return sb.ToString();
        }
    }

    public partial class WBeginEndBlockStatement : WSqlStatement
    {
        internal IList<WSqlStatement> StatementList { set; get; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}BEGIN\r\n", indent);
            sb.Append(StatementListToString(StatementList, indent + "  "));
            sb.Append("\r\n");
            sb.AppendFormat("{0}END", indent);

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (StatementList != null)
            {
                var index = 0;
                for (var count = StatementList.Count; index < count; ++index)
                    StatementList[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WWhileStatement : WSqlStatement
    {
        internal WBooleanExpression Predicate;
        internal WSqlStatement Statement;
        internal override bool OneLine()
        {
            return false;
        }
        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}WHILE ", indent);

            if (Predicate.OneLine())
            {
                sb.Append(Predicate.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(Predicate.ToString(indent + "  "));
            }

            sb.Append("\r\n");
            sb.Append(Statement.ToString(indent));
 
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Predicate != null)
            {
                Predicate.Accept(visitor);
            }
            if (Statement != null)
            {
                Statement.Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WIfStatement : WSqlStatement
    {
        public WBooleanExpression Predicate { get; set; }
        public WSqlStatement ThenStatement { get; set; }
        public WSqlStatement ElseStatement { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(128);

            if (Predicate.OneLine())
            {
                sb.AppendFormat("{0}IF {1}\r\n", indent, Predicate.ToString(""));
            }
            else
            {
                sb.AppendFormat("{0}IF \r\n", indent);
                sb.Append(Predicate.ToString(indent + "  "));
                sb.Append("\r\n");
            }

            sb.Append(ThenStatement.ToString(indent + "  "));

            if (ElseStatement != null)
            {
                sb.Append("\r\n");
                sb.AppendFormat("{0}ELSE\r\n", indent);

                sb.Append(ElseStatement.ToString(indent + "  "));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Predicate != null)
            {
                Predicate.Accept(visitor);
                ThenStatement.Accept(visitor);
            }

            if (ElseStatement != null)
            {
                ElseStatement.Accept(visitor);
            }

            base.AcceptChildren(visitor);
        }
    }
}
