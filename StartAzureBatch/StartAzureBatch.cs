using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using GraphView;

namespace StartAzureBatch
{
    public class GraphViewAzureBatchJob
    {
        internal GraphTraversal traversal;
        public GraphTraversal Traversal
        {
            get
            {
                return traversal;
            }
            set
            {
                traversal = value;
                this.jobId = Guid.NewGuid().ToString("N");
            }
        }

        internal string query;
        public String Query
        {
            get
            {
                return query;
            }
            set
            {
                query = value;
                this.jobId = Guid.NewGuid().ToString("N");
            }
        }

        public GraphViewCommand Command { get; }

        internal string jobId;

        internal readonly int parallelism;
        internal readonly List<NodePlan> nodePlans;
        internal readonly ParallelLevel parallelLevel;

        // CosmosDB account credentials
        internal readonly string docDBEndPoint;
        internal readonly string docDBKey;
        internal readonly string docDBDatabaseId;
        internal readonly string docDBCollectionId;
        internal readonly bool useReverseEdge;
        internal readonly string partitionByKey;
        internal readonly int spilledEdgeThresholdViagraphAPI;

        public int TimeLimit { get; set; } = 300; // Time limit of this query in seconds.
        internal bool IsSuccess { get; set; } = false;
        internal string Result { get; set; }
        // For benchmark. Not include the time of compiling and uploading file.
        public long UseTime { get; set; }

        public GraphViewAzureBatchJob(int parallelism, List<NodePlan> nodePlans, ParallelLevel parallelLevel,
            string docDBEndPoint, string docDBKey, string docDBDatabaseId, string docDBCollectionId,
            bool useReverseEdge, string partitionByKey, int spilledEdgeThresholdViagraphAPI)
        {
            Debug.Assert(nodePlans.Count == parallelism);
            this.parallelism = parallelism;
            this.nodePlans = nodePlans;
            this.parallelLevel = parallelLevel;

            this.docDBEndPoint = docDBEndPoint;
            this.docDBKey = docDBKey;
            this.docDBDatabaseId = docDBDatabaseId;
            this.docDBCollectionId = docDBCollectionId;
            this.useReverseEdge = useReverseEdge;
            this.partitionByKey = partitionByKey;
            this.spilledEdgeThresholdViagraphAPI = spilledEdgeThresholdViagraphAPI;

            GraphViewConnection connection = new GraphViewConnection(
                this.docDBEndPoint, this.docDBKey, this.docDBDatabaseId, this.docDBCollectionId,
                GraphType.GraphAPIOnly, this.useReverseEdge, this.spilledEdgeThresholdViagraphAPI, this.partitionByKey);
            this.Command = new GraphViewCommand(connection);
        }
    }

    public class AzureBatchJobManager
    {
        // Batch account credentials
        private readonly string batchAccountName;
        private readonly string batchAccountKey;
        private readonly string batchAccountUrl;

        // Storage account credentials
        private readonly string storageConnectionString;

        // Suppose that there is only one pool.
        private readonly string poolId;
        private readonly int virtualMachineNumber;
        private readonly string virtualMachineSize;

        private readonly string outputContainerNamePrefix;
        private readonly string appContainerNamePrefix;

        private readonly string denpendencyPath;
        private readonly string exeName;

        public AzureBatchJobManager(string batchAccountName, string batchAccountKey, string batchAccountUrl,
            string storageAccountName, string storageAccountKey,
            string poolId, int virtualMachineNumber, string virtualMachineSize)
        {
            this.batchAccountName = batchAccountName;
            this.batchAccountKey = batchAccountKey;
            this.batchAccountUrl = batchAccountUrl;

            this.storageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            this.poolId = poolId;

            this.outputContainerNamePrefix = "output";
            this.appContainerNamePrefix = "application";

            this.denpendencyPath = "..\\..\\..\\GraphViewProgram\\bin\\Debug\\";
            this.exeName = "Program.exe";

            this.virtualMachineNumber = virtualMachineNumber;
            this.virtualMachineSize = virtualMachineSize;

            this.CreatePoolIfNotExistAsync().Wait();
        }

        private async Task RunQueryAsync(GraphViewAzureBatchJob job)
        {
            Console.WriteLine($"Query start.");

            Console.WriteLine("[compile query] start");
            string compileStr = CompileQuery(job);
            Console.WriteLine("[compile query] finish");

            string compileResultPath = $"compileResult{job.jobId}";
            File.WriteAllText(compileResultPath, compileStr);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(this.storageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            string appContainerName = this.appContainerNamePrefix + job.jobId;
            await CreateContainerIfNotExistAsync(blobClient, appContainerName);

            string outputContainerName = this.outputContainerNamePrefix + job.jobId;
            await CreateContainerIfNotExistAsync(blobClient, outputContainerName);
            
            // Obtain a shared access signature that provides write access to the output container to which the tasks will upload their output.
            string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                //          IP   , AffinityId
                List<Tuple<string, string>> nodeInfo = this.AllocateComputeNode(batchClient, job);

                Console.WriteLine("[make partition plan] start");
                string partitionStr = MakePlan(nodeInfo, job);
                Console.WriteLine("[make partition plan] finish");

                string partitionPath = $"parititonPlan{job.jobId}";
                File.WriteAllText(partitionPath, partitionStr);

                // Paths to the executable and its dependencies that will be executed by the tasks
                List<string> applicationFilePaths = new List<string>
                {
                    Path.Combine(this.denpendencyPath, this.exeName), // Program.exe
                    Path.Combine(this.denpendencyPath, "Microsoft.WindowsAzure.Storage.dll"),
                    Path.Combine(this.denpendencyPath, "DocumentDB.Spatial.Sql.dll"),
                    Path.Combine(this.denpendencyPath, "GraphView.dll"),
                    Path.Combine(this.denpendencyPath, "JsonServer.dll"),
                    Path.Combine(this.denpendencyPath, "Microsoft.Azure.Documents.Client.dll"),
                    Path.Combine(this.denpendencyPath, "Microsoft.Azure.Documents.ServiceInterop.dll"),
                    Path.Combine(this.denpendencyPath, "Microsoft.SqlServer.TransactSql.ScriptDom.dll"),
                    Path.Combine(this.denpendencyPath, "Newtonsoft.Json.dll"),
                    Path.Combine(this.denpendencyPath, this.exeName + ".config"), // "Program.exe.config"
                    compileResultPath,
                    partitionPath,
                };

                List<ResourceFile> resourceFiles = await UploadFilesToContainerAsync(blobClient, appContainerName, applicationFilePaths);

                await this.CreateJobAsync(batchClient, job);
                
                string[] args = { "-file", compileResultPath, partitionPath, outputContainerSasUrl };
                await this.AddTasksAsync(batchClient, job, nodeInfo, resourceFiles, args);

                var watch = System.Diagnostics.Stopwatch.StartNew();

                job.IsSuccess = await MonitorTasks(batchClient, job.jobId, TimeSpan.FromSeconds(job.TimeLimit));

                watch.Stop();
                job.UseTime = watch.ElapsedMilliseconds;

                await this.DownloadAndAggregateOutputAsync(blobClient, outputContainerName, job);

                // For Debug. Print stdout and stderr
                // PrintTaskOutput(batchClient, job);

                await DeleteContainerAsync(blobClient, outputContainerName);
                await DeleteContainerAsync(blobClient, appContainerName);

                try
                {
                    await batchClient.JobOperations.DeleteJobAsync(job.jobId);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                
            }
        }

        private static string CompileQuery(GraphViewAzureBatchJob job)
        {
            if (job.Traversal != null)
            {
                return job.Traversal.CompileAndSerialize(job.parallelLevel);
            }
            else
            {
                // Not Support set parallelLevel temporarily.
                GraphViewCommand command = job.Command;
                command.CommandText = job.Query;
                return command.CompileAndSerialize();
            }
        }

        private List<Tuple<string, string>> AllocateComputeNode(BatchClient batchClient, GraphViewAzureBatchJob job)
        {
            List<Tuple<string, string>> nodeInfo = new List<Tuple<string, string>>();

            CloudPool pool = batchClient.PoolOperations.GetPool(this.poolId);
            foreach (ComputeNode node in pool.ListComputeNodes())
            {
                // todo : implement an algorithm to allocate node
                nodeInfo.Add(new Tuple<string, string>(node.IPAddress, node.AffinityId));

                if (nodeInfo.Count == job.parallelism)
                {
                    break;
                }
            }

            return nodeInfo;
        }

        private static string MakePlan(List<Tuple<string, string>> nodeInfo, GraphViewAzureBatchJob job)
        {
            for (int i = 0; i < nodeInfo.Count; i++)
            {
                job.nodePlans[i].SetNodeInfo(nodeInfo[i].Item1, 8000);
            }

            return NodePlan.SerializeNodePlans(job.nodePlans);
        }

        // For Debug
        private static void PrintTaskOutput(BatchClient batchClient, GraphViewAzureBatchJob job)
        {
            for (int i = 0; i < job.parallelism; i++)
            {
                CloudTask task = batchClient.JobOperations.GetTask(job.jobId, i.ToString());
                string stdOut = task.GetNodeFile(Constants.StandardOutFileName).ReadAsString();
                string stdErr = task.GetNodeFile(Constants.StandardErrorFileName).ReadAsString();
                Console.WriteLine("---- stdout.txt ----taskId: " + i);
                Console.WriteLine(stdOut);
                Console.WriteLine("---- stderr.txt ----taskId: " + i);
                Console.WriteLine(stdErr);
                Console.WriteLine("------------------------------------");
            }
        }

        /// <summary>
        /// Creates a container with the specified name in Blob storage, unless a container with that name already exists.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name for the new container.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.CreateIfNotExistsAsync())
            {
                Console.WriteLine("Container [{0}] created.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] exists, skipping creation.", containerName);
            }
        }

        /// <summary>
        /// Returns a shared access signature (SAS) URL providing the specified permissions to the specified container.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the container for which a SAS URL should be obtained.</param>
        /// <param name="permissions">The permissions granted by the SAS URL.</param>
        /// <returns>A SAS URL providing the specified access to the container.</returns>
        /// <remarks>The SAS URL provided is valid for 2 hours from the time this method is called. The container must
        /// already exist within Azure Storage.</remarks>
        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
        {
            // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };
            
            // Generate the shared access signature on the container, setting the constraints directly on the signature
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URL string for the container, including the SAS token
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }

        /// <summary>
        /// Uploads the specified files to the specified Blob container, returning a corresponding
        /// collection of <see cref="ResourceFile"/> objects appropriate for assigning to a task's
        /// <see cref="CloudTask.ResourceFiles"/> property.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="inputContainerName">The name of the blob storage container to which the files should be uploaded.</param>
        /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
        /// <returns>A collection of <see cref="ResourceFile"/> objects.</returns>
        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(await UploadFileToContainerAsync(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <returns>A <see cref="Microsoft.Azure.Batch.ResourceFile"/> instance representing the file within blob storage.</returns>
        private static async Task<ResourceFile> UploadFileToContainerAsync(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromFileAsync(filePath);
            
            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }

        /// <summary>
        /// Downloads all files from the specified blob storage container to the specified directory.
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container containing the files to download.</param>
        /// <param name="directoryPath">The full path of the local directory to which the files should be downloaded.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task DownloadBlobsFromContainerAsync(CloudBlobClient blobClient, string containerName, string directoryPath)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", containerName);

            // Retrieve a reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Get a flat listing of all the block blobs in the specified container
            foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            {
                // Retrieve reference to the current blob
                CloudBlob blob = (CloudBlob)item;

                // Save blob contents to a file in the specified folder
                string localOutputFile = Path.Combine(directoryPath, blob.Name);
                await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            }

            Console.WriteLine("All files downloaded to {0}", directoryPath);
        }

        /// <summary>
        /// Deletes the container with the specified name from Blob storage, unless a container with that name does not exist.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the container to delete.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task DeleteContainerAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.DeleteIfExistsAsync())
            {
                Console.WriteLine("Container [{0}] deleted.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] does not exist, skipping deletion.", containerName);
            }
        }

        /// <summary>
        /// Processes all exceptions inside an <see cref="AggregateException"/> and writes each inner exception to the console.
        /// </summary>
        /// <param name="aggregateException">The <see cref="AggregateException"/> to process.</param>
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Flatten the aggregate and iterate over its inner exceptions, printing each
            foreach (Exception exception in aggregateException.Flatten().InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }

        private async Task CreatePoolIfNotExistAsync()
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                CloudPool pool = null;
                try
                {
                    Console.WriteLine("Creating pool [{0}]...", this.poolId);

                    pool = batchClient.PoolOperations.CreatePool(
                        poolId: this.poolId,
                        targetLowPriorityComputeNodes: 0,
                        targetDedicatedComputeNodes: this.virtualMachineNumber,
                        virtualMachineSize: this.virtualMachineSize,
                        cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));   // Windows Server 2012 R2

                    // When internode communication is enabled, 
                    // nodes in Cloud Services Configuration pools can communicate with each other on ports greater than 1100, 
                    // and Virtual Machine Configuration pools do not restrict traffic on any port.
                    pool.InterComputeNodeCommunicationEnabled = true;

                    await pool.CommitAsync();
                }
                catch (BatchException be)
                {
                    // Swallow the specific error code PoolExists since that is expected if the pool already exists
                    if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
                    {
                        Console.WriteLine("The pool {0} already existed when we tried to create it", this.poolId);
                        pool = batchClient.PoolOperations.GetPool(this.poolId);
                        Console.WriteLine("TargetDedicatedComputeNodes: " + pool.TargetDedicatedComputeNodes);
                        Console.WriteLine("TargetLowPriorityComputeNodes :" + pool.TargetLowPriorityComputeNodes);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }
            }
        }

        /// <summary>
        /// Creates a job in the specified pool.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job to be created.</param>
        /// <param name="poolId">The id of the <see cref="CloudPool"/> in which to create the job.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private async Task CreateJobAsync(BatchClient batchClient, GraphViewAzureBatchJob job)
        {
            Console.WriteLine("Creating job [{0}]...", job.jobId);

            CloudJob cloudJob = batchClient.JobOperations.CreateJob();
            cloudJob.Id = job.jobId;
            cloudJob.PoolInformation = new PoolInformation { PoolId = this.poolId };

            await cloudJob.CommitAsync();
        }

        /// <summary>
        /// Creates tasks to process each of the specified input files, and submits them to the
        /// specified job for execution.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job to which the tasks should be added.</param>
        /// <param name="applicationFiles">A collection of <see cref="ResourceFile"/> objects representing the program 
        /// (with dependencies and serialization data) to be executed on the compute nodes.</param>
        /// <param name="args"></param>
        /// <returns>A collection of the submitted tasks.</returns>
        private async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, GraphViewAzureBatchJob job,
            List<Tuple<string, string>> nodeInfo, List<ResourceFile> applicationFiles, string[] args)
        {
            Console.WriteLine("Adding task to job [{0}]...", job.jobId);
            Debug.Assert(args.Length == 4);
            // Create a collection to hold the tasks that we'll be adding to the job
            List<CloudTask> tasks = new List<CloudTask>();

            for (int i = 0; i < job.parallelism; i++)
            {
                string taskCommandLine = $"cmd /c %AZ_BATCH_TASK_WORKING_DIR%\\{this.exeName} " +
                    $"\"{args[0]}\" \"{args[1]}\" \"{args[2]}\" \"{args[3]}\"";
                CloudTask task = new CloudTask(i.ToString(), taskCommandLine);
                task.ResourceFiles = new List<ResourceFile>(applicationFiles);
                
                // specify compute node
                task.AffinityInformation = new AffinityInformation(nodeInfo[i].Item2);

                // set partition-plan-index
                if (task.EnvironmentSettings == null)
                {
                    task.EnvironmentSettings = new List<EnvironmentSetting>();
                }
                task.EnvironmentSettings.Add(new EnvironmentSetting("TASK_INDEX", i.ToString()));

                // set task running in administrator level
                task.UserIdentity = new UserIdentity(new AutoUserSpecification(
                    elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Task));

                tasks.Add(task);
            }

            // Add the tasks as a collection opposed to a separate AddTask call for each. Bulk task submission
            // helps to ensure efficient underlying API calls to the Batch service.
            await batchClient.JobOperations.AddTaskAsync(job.jobId, tasks);

            return tasks;
        }

        private async Task DownloadAndAggregateOutputAsync(CloudBlobClient blobClient, string outputContainerName, GraphViewAzureBatchJob job)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", outputContainerName);

            CloudBlobContainer container = blobClient.GetContainerReference(outputContainerName);

            List<MemoryStream> streams = new List<MemoryStream>();
            foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            {
                CloudBlob blob = (CloudBlob)item;
                MemoryStream stream = new MemoryStream();
                streams.Add(stream);

                await blob.DownloadToStreamAsync(stream);
            }

            StringBuilder result = new StringBuilder();
            foreach (MemoryStream stream in streams)
            {
                stream.Position = 0;
                StreamReader stringReader = new StreamReader(stream);
                result.Append(stringReader.ReadToEnd());
            }

            //Console.WriteLine("Aggregate result is as follows:");
            job.Result = result.ToString();
            //Console.Write(job.Result);
        }

        /// <summary>
        /// Monitors the specified tasks for completion and returns a value indicating whether all tasks completed successfully
        /// within the timeout period.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job containing the tasks that should be monitored.</param>
        /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
        /// <returns><c>true</c> if all tasks in the specified job completed with an exit code of 0 within the specified timeout period, otherwise <c>false</c>.</returns>
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string successMessage = "All tasks reached state Completed.";
            const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

            // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
            // specify that only the "id" property of each task should be populated. Using a detail level for
            // all list operations helps to lower response time from the Batch service.
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(jobId, detail).ToListAsync();

            Console.WriteLine("Awaiting task completion, timeout in {0}...", timeout.ToString());

            // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
            // reach the Completed state.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                Console.WriteLine(failureMessage);
                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
            // or return a non-zero exit code.

            // Update the detail level to populate only the task id and executionInfo properties.
            // We refresh the tasks below, and need only this information for each task.
            detail.SelectClause = "id, executionInfo";

            foreach (CloudTask task in tasks)
            {
                // Populate the task's properties with the latest info from the Batch service
                await task.RefreshAsync(detail);

                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    // A task with failure information set indicates there was a problem with the task. It is important to note that
                    // the task's state can be "Completed," yet still have encountered a failure.

                    allTasksSuccessful = false;

                    Console.WriteLine("WARNING: Task [{0}] encountered a failure: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    if (task.ExecutionInformation.ExitCode != 0)
                    {
                        // A non-zero exit code may indicate that the application executed by the task encountered an error
                        // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                        // your implementation of error checking may differ from this example.

                        Console.WriteLine("WARNING: Task [{0}] returned a non-zero exit code - this may indicate task execution or completion failure.", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("Success! All tasks completed successfully within the specified timeout period.");
            }

            return allTasksSuccessful;
        }

        // For Debug
        public void DeletePool()
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                batchClient.PoolOperations.DeletePool(this.poolId);
            }
        }

        // For Debug
        public void DeleteJob(string jobId)
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                batchClient.JobOperations.DeleteJob(jobId);
            }
        }

        // For Debug
        public void ClearResource()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(this.storageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            foreach (CloudBlobContainer container in blobClient.ListContainers())
            {
                container.DeleteIfExists();
            }

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                foreach (CloudJob job in batchClient.JobOperations.ListJobs())
                {
                    job.Delete();
                }
            }
        }

        public List<string> TestQuery(GraphViewAzureBatchJob graphViewJob)
        {
            this.RunQueryAsync(graphViewJob).Wait();

            if (graphViewJob.IsSuccess)
            {
                List<string> result = graphViewJob.Result.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.RemoveEmptyEntries).ToList();

                graphViewJob.traversal = null;
                graphViewJob.query = null;

                // todo: If the result is set-type.(like aggregate/store). the result need be merged.
                // todo: If the result is global-map-type.(like groupCount('x').cap('x')). need remove redundant results.

                if (graphViewJob.Command.OutputFormat == OutputFormat.GraphSON)
                {
                    string mergeRes = "[";
                    foreach (string res in result)
                    {
                        if (res == "[]")
                        {
                            continue;
                        }
                        if (mergeRes.Length > 1)
                        {
                            mergeRes += ", ";
                        }
                        mergeRes += res.Substring(1, res.Length - 2);
                    }
                    mergeRes += "]";
                    return new List<string>() { mergeRes };
                }

                return result;
            }
            throw new GraphViewException("Run Query failed!");
        }
    }
}
