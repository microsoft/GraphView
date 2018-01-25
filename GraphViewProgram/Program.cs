using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
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

            // for Debug
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
                //file.WriteLine("------------------------------------------");
                foreach (var row in result)
                {
                    file.WriteLine(row);
                }

                //file.WriteLine($"Task: {Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")}" +
                //               $", Node: {Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID")}");
                //file.WriteLine("------------------------------------------");
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

    }
}