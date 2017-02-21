using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace JsMix
{
    public static class Program
    {
        private static readonly Dictionary<string, string[]> __allPartJs = new Dictionary<string, string[]>();

        private static readonly Regex __regexInclude = new Regex(@"^(?<INDENT>( |\t)*)""include( |\t|\r|\n)+(?<PARTNAME>[^ \t""]+)"";$",
                                                                 RegexOptions.ExplicitCapture | RegexOptions.Singleline | RegexOptions.Compiled);

        private static void ProcessFile(DirectoryInfo directory, string tmplFilePath)
        {
            string tmplFileName = Path.GetFileName(tmplFilePath);
            Trace.Assert(tmplFileName != null && tmplFileName.EndsWith(".template.js"));
            string mixFileName = tmplFileName.Replace(".template.js", ".js");

            string text = File.ReadAllText(tmplFilePath, Encoding.UTF8);
            string[] lines = text.Replace("\r\n", "\n").Replace('\r', '\n').Split(new[] { '\n' }, StringSplitOptions.None);

            StringBuilder result = new StringBuilder();
            for (int lineno = 0; lineno < lines.Length; lineno++) {
                string line = lines[lineno];
                Match match = __regexInclude.Match(line);
                if (!match.Success) {
                    result.AppendLine(line);
                    continue;
                }

                string indent = match.Groups["INDENT"].Value;
                string partFileName = match.Groups["PARTNAME"].Value;
                partFileName = partFileName.EndsWith(".snippet.js")
                                   ? partFileName
                                   : partFileName.EndsWith(".snippet")
                                       ? partFileName + ".js"
                                       : partFileName + ".snippet.js";

                string[] partLines;
                if (!__allPartJs.TryGetValue(partFileName, out partLines)) {
                    Console.WriteLine($"[JsMix] {tmplFileName}({lineno + 1}): Can't find snippet: {match.Groups["PARTNAME"].Value}");
                    Environment.Exit(1);
                }
                result.AppendLine($"\n{indent}//++++++++++++++++ BEGIN: {partFileName} ++++++++++++++++");
                foreach (string partLine in partLines) {
                    result.AppendLine($"{indent}{partLine}");
                }
                result.AppendLine($"{indent}//---------------- END: {partFileName} ----------------\n");
            }

            File.WriteAllText(Path.Combine(directory.FullName, "Generated", $"{mixFileName}"),
                              result.ToString(),
                              Encoding.UTF8);
            Console.WriteLine($"[JsMix] Generated: {mixFileName} (from {tmplFileName})");
        }

        public static void Main(string[] args)
        {
            Trace.Assert(args.Length == 1, "JavaScriptIntegration utility should take exactly 1 parameter: the JavaScript directory");

            Stopwatch watch = Stopwatch.StartNew();
            Console.WriteLine("[JsMix] Integrate JavaScript snippets to templates...");
            DirectoryInfo directory = new DirectoryInfo(args[0]);
            directory.CreateSubdirectory("Generated");

            // Load *.part.js
            foreach (FileInfo snippetJs in directory.CreateSubdirectory("Snippet").GetFiles("*.snippet.js")) {
                Console.WriteLine($"[JsMix] Find snippet: {snippetJs.Name}");
                string text = File.ReadAllText(snippetJs.FullName, Encoding.UTF8);
                string[] lines = text.Replace("\r\n", "\n").Replace('\r', '\n').Split(new[] { '\n' }, StringSplitOptions.None);
                __allPartJs.Add(snippetJs.Name, lines);
            }

            //Console.WriteLine("[JsMix] Now mixing...");
            (from tmplFile in directory.CreateSubdirectory("Template").GetFiles("*.template.js") select tmplFile)
                .AsParallel()
                .ForAll(f => ProcessFile(directory, f.FullName));
            Console.WriteLine($"[JsMix] Done in {watch.Elapsed}");
        }
    }
}
