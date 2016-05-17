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
    partial class DocDBInsertNodeTemplate
    {
        public string EndpointUrl { get; set; }
        public string AuthorizationKey { get; set; }
        public string DatabaseID { get; set; }
        public string CollectionID { get; set; }
        public string json_str { get; set; }
    }

    public class DocDBSelectQuery
    {
        public string Key;
        public string Predicate;

        public DocDBSelectQuery(string key, string predicate)
        {
            Key = key;
            Predicate = predicate;
        }
    }
    partial class DocDBInsertEdgeTemplate
    {
        public string EndpointUrl { get; set; }
        public string AuthorizationKey { get; set; }
        public string DatabaseID { get; set; }
        public string CollectionID { get; set; }
        public string Edge { get; set; }
        public string source { get; set; }
        public string sink { get; set; }

        public IList<DocDBSelectQuery> SelectQuery { get; set; }
    }

    partial class DocDBDeleteNodeTemplate
    {
        public string EndpointUrl { get; set; }
        public string AuthorizationKey { get; set; }
        public string DatabaseID { get; set; }
        public string CollectionID { get; set; }
        public WBooleanExpression search { get; set; }
    }
}
