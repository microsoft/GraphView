using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Configuration;
using System.Net;
using System.Net.Sockets;
using GraphView;

namespace GraphViewProgram
{
    public class Program
    {
        /// <summary>
        /// Two kinds of cmd.
        /// 1. Program.exe -file [serializationFilePath] [partitionFilePath] [outputPath].
        /// 2. Program.exe -str [serializationString] [partitionStr] [outputPath].
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                throw new GraphViewException($"The number of args of Main error. Now the number is {args.Length}.");
            }

            string serializationStr;
            string partitionStr;
            if (args[0] == "-file")
            {
                serializationStr = File.ReadAllText(args[1]);
                partitionStr = File.ReadAllText(args[2]);
            }
            else if(args[0] == "-str")
            {
                serializationStr = args[1];
                partitionStr = args[2];
            }
            else
            {
                throw new GraphViewException($"args[0] error. Now the args[0] is \"{args[0]}\".");
            }

            string outputContainerSas = args[3];

            List<string> result = GraphTraversal.ExecuteQueryByDeserialization(serializationStr, partitionStr);

            foreach (var r in result)
            {
                Console.WriteLine(r);
            }

            SaveOutput(result, outputContainerSas);
        }

        /// <summary>
        /// Save ouput in file.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="outputContainerSas"></param>
        private static void SaveOutput(List<string> result, string outputContainerSas)
        {
            string outputFile = $"output-{Environment.GetEnvironmentVariable("AZ_BATCH_JOB_ID")}-{Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")}";
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(outputFile))
            {
                // The redundant output is for Debug.
                file.WriteLine("------------------------------------------");
                foreach (var row in result)
                {
                    file.WriteLine(row);
                }

                file.WriteLine($"Task: {Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")}" +
                               $", Node: {Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID")}");
                file.WriteLine("------------------------------------------");
            }

            // Upload the output file to blob container in Azure Storage
            UploadFileToContainer(outputFile, outputContainerSas);
        }

        /// <summary>
        /// Uploads the specified file to the container represented by the specified
        /// container shared access signature (SAS).
        /// </summary>
        /// <param name="filePath">The path of the file to upload to the Storage container.</param>
        /// <param name="containerSas">The shared access signature granting write access to the specified container.</param>
        private static void UploadFileToContainer(string filePath, string containerSas)
        {
            string blobName = Path.GetFileName(filePath);

            // Obtain a reference to the container using the SAS URI.
            CloudBlobContainer container = new CloudBlobContainer(new Uri(containerSas));

            // Upload the file (as a new blob) to the container
            try
            {
                CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
                blob.UploadFromFile(filePath);

                Console.WriteLine("Write operation succeeded for SAS URL " + containerSas);
                Console.WriteLine();
            }
            catch (StorageException e)
            {

                Console.WriteLine("Write operation failed for SAS URL " + containerSas);
                Console.WriteLine("Additional error information: " + e.Message);
                Console.WriteLine();

                // Indicate that a failure has occurred so that when the Batch service sets the
                // CloudTask.ExecutionInformation.ExitCode for the task that executed this application,
                // it properly indicates that there was a problem with the task.
                Environment.ExitCode = -1;
            }
        }

        /// <summary>
        /// Use for Debug.
        /// </summary>
        private static void LoadModernGraphData()
        {
            string endpoint = "";
            string authKey = "";
            string databaseId = "GroupMatch";
            string collectionId = "Modern";
            bool TestUseReverseEdge = true;
            string TestPartitionByKey = "name";
            int TestSpilledEdgeThresholdViagraphAPI = 1;

            GraphViewConnection connection =  GraphViewConnection.ResetGraphAPICollection(endpoint, authKey, databaseId, collectionId,
                TestUseReverseEdge, TestSpilledEdgeThresholdViagraphAPI, TestPartitionByKey);

            using (GraphViewCommand graphCommand = new GraphViewCommand(connection))
            {
                graphCommand.g().AddV("person").Property("id", "dummy").Property("name", "marko").Property("age", 29).Next();
                graphCommand.g().AddV("person").Property("id", "特殊符号").Property("name", "vadas").Property("age", 27).Next();
                graphCommand.g().AddV("software").Property("id", "这是一个中文ID").Property("name", "lop").Property("lang", "java").Next();
                graphCommand.g().AddV("person").Property("id", "引号").Property("name", "josh").Property("age", 32).Next();
                graphCommand.g().AddV("software").Property("id", "中文English").Property("name", "ripple").Property("lang", "java").Next();
                graphCommand.g().AddV("person").Property("name", "peter").Property("age", 35).Next();  // Auto generate document id
                graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
                graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
                graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
                graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
                graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
                graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            }
        }
    }
}