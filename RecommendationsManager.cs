﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Quartz.Util;
using Z.BulkOperations;

namespace RecommendationsManager
{
    public class RecommendationsManager
    {
        private string accountKey = null;
        private RecommendationsApiWrapper recommender = null;
        private string modelId = null;
        private long buildId = -1;

        /// <summary>
        /// Class to manage backend processes and data for recommendations on e-commerce site.
        /// </summary>
        /// <param name="args"></param>
        public RecommendationsManager(string _accountKey, RecommendationsApiWrapper _recommender, string _modelId, 
            long _buildId)
        {
            accountKey = _accountKey;
            recommender = _recommender;
            modelId = _modelId;
            buildId = _buildId;
        }

        /// <summary>
        /// Sets buildId member variable for current session.
        /// </summary>
        /// <param name="_buildId"></param>
        public void SetBuildId(long _buildId)
        {
            buildId = _buildId;
        }

        /// <summary>
        /// Sets modelId member variable for curren session.
        /// </summary>
        /// <param name="_modelId"></param>
        public void SetModelId(string _modelId)
        {
            modelId = _modelId;
        }

        /// <summary>
        /// Sets the ModelId and AccountKey in SQL for the worker role.
        /// </summary>
        /// <param name="_modelId"></param>
        /// <param name="_accountKey"></param>
        public void SetModelAndBuildIds(string _modelId, string _accountKey)
        {
            string recommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;
            try
            {
                using (SqlConnection connection = new SqlConnection(recommendationsConnString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand())
                    {
                        command.CommandType = CommandType.Text;
                        command.Connection = connection;
                        command.CommandText = "TRUNCATE TABLE MLInfo; ";
                        command.ExecuteNonQuery();
                    }
                    using (SqlCommand command = new SqlCommand())
                    {
                        command.CommandType = CommandType.Text;
                        command.Connection = connection;
                        command.CommandText = @"INSERT INTO MLInfo (AccountKey, ModelId) VALUES (@AccountKey, @ModelId); ";
                        SqlParameter parameter;
                        parameter = new SqlParameter("@AccountKey", SqlDbType.NVarChar, 50);
                        parameter.Value = _accountKey;
                        command.Parameters.Add(parameter);

                        parameter = new SqlParameter("@ModelId", SqlDbType.NVarChar, 50);
                        parameter.Value = _modelId;
                        command.Parameters.Add(parameter);

                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in SetModelAndBuildIds(): {0}", e);
            }

        }

        /// <summary>
        /// Manages insert operation.
        /// </summary>
        public void UsageToCSVManager()
        {
            string recommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (SqlConnection connection = new SqlConnection(recommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");
                //Get all new purchase data.
                List<string[]> purchaseData = GetPurchaseData(connection);
                //Delete processed purchase data.
                DeleteRawData();
                //Write purchase data to Usage CSV file.
                UsageToCSV(purchaseData);
            }
        }

        /// <summary>
        /// Gets raw purchase data and formats it for usage.csv file.
        /// </summary>
        /// <param name="connection"></param>
        public List<string[]> GetPurchaseData(SqlConnection connection)
        {
            //Store all new usage data in list.
            List<string[]> purchaseData = new List<string[]>();
            try
            {
                //Select new usage data and bulk merge into SQL.
                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = connection;//Set connection used by this instance of SqlCommand
                    command.CommandType = CommandType.Text;//SQL Text Command
                    command.CommandText = @"SELECT UserId, ProductId, Time
                                        FROM PurchaseDataRaw; ";

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            DateTime dt = reader.GetDateTime(2);//Format datetime into a string
                            string UserId = reader.GetString(0).Replace('@', '_').Replace('.', '_');
                            string ProductId = reader.GetString(1);
                            string dtString = string.Format("{0}/{1}/{2}T{3}:{4}:{5}", dt.Year.ToString("D4"), dt.Month.ToString("D2"),
                                dt.Day.ToString("D2"), dt.Hour.ToString("D2"), dt.Minute.ToString("D2"), dt.Second.ToString("D2"));
                            string[] temp = { UserId, ProductId, dtString, "Purchase" };
                            purchaseData.Add(temp);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in GetPurchaseData(): {0}", e.Message);
            }

            return purchaseData;
        }

        /// <summary>
        /// Deletes all purchase data from PurchaseDataRaw table in Recommendations DB.
        /// </summary>
        public void DeleteRawData()
        {
            string recommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (SqlConnection connection = new SqlConnection(recommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");

                try
                {
                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandType = CommandType.Text;
                        command.CommandText = @"TRUNCATE TABLE PurchaseDataRaw; ";
                        command.ExecuteNonQuery();
                        Console.WriteLine("Deleted all data in PurchaseDataRaw.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error in DeleteRawData(): {0}", ex.ToString());
                }
            }
        }

        /// <summary>
        /// Export purchase data to CSV file 'usage.csv' for training machine learning model.
        /// </summary>
        /// <param name="purchaseData"></param>
        public void UsageToCSV(List<string[]> purchaseData)
        {
            try
            {
                //Read all purchase data into usage.csv file.
                string resourcesDir = @"..\..\Resources";
                string filePath = Path.Combine(resourcesDir, "usage.csv");
                using (StreamWriter file = new StreamWriter(filePath))
                {
                    foreach (string[] purchase in purchaseData)
                    {
                        var tempLine = string.Format("{0},{1},{2},{3}", purchase[0], purchase[1], purchase[2], purchase[3]);
                        file.WriteLine(tempLine);
                        file.Flush();

                    }
                }
                //Verify file contents:
                Console.WriteLine("Printing content:");
                using (StreamReader reader = new StreamReader(filePath))
                {
                    string line = "";
                    while ((line = reader.ReadLine()) != null)
                    {
                        Console.WriteLine(line);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in UsageToCSV(): {0}", ex.ToString());
            }

            Console.WriteLine("CSV generated successfully.");
        }

        /// <summary>
        /// Export catalog information to CSV file.
        /// </summary>
        public void CatalogToCSV()
        {
            string con = ConfigurationManager.ConnectionStrings["CatalogCS"].ConnectionString;

            using (SqlConnection connection = new SqlConnection(con))
            {
                connection.Open();
                //Store catalog data in-memory.
                List<string[]> catalog = new List<string[]>();
                //Select product data.
                using (SqlCommand command = new SqlCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.Connection = connection;
                    command.CommandText = "SELECT ProductId, ProductName, Description, CategoryId FROM Products;";

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string ProductId = reader.GetInt32(0).ToString();
                            string ProductName = reader.GetString(1);
                            string Description = reader.GetString(2);
                            string CategoryId = reader.GetInt32(3).ToString();
                            string[] temp = { ProductId, ProductName, CategoryId, Description };
                            catalog.Add(temp);
                        }
                    }
                }

                //Select category data.
                Dictionary<int, string> categories = new Dictionary<int, string>();
                using (SqlCommand command = new SqlCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.Connection = connection;
                    command.CommandText = "SELECT CategoryID, CategoryName FROM Categories; ";

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            categories.Add(reader.GetInt32(0), reader.GetString(1));
                        }
                    }
                }

                //Aggregate product/catalog data and write to catalog CSV file.
                string resourcesDir = @"..\..\Resources";
                string filePath = Path.Combine(resourcesDir, "catalog.csv");
                Console.WriteLine("filePath: " + filePath);
                using (StreamWriter file = new StreamWriter(filePath))
                {
                    foreach (string[] row in catalog)
                    {
                        try
                        {
                            categories.TryGetValue(Convert.ToInt32(row[2]), out row[2]);
                            string tempLine = string.Format("{0},{1},{2},{3}", row[0], row[1], row[2], row[3]);
                            file.WriteLine(tempLine);
                            file.Flush();
                        }
                        catch (KeyNotFoundException)
                        {
                            Console.WriteLine("category id not found. breaking.");
                            break;
                        }
                    }
                }

                Console.WriteLine("Printing content:");
                string line = "";
                using (StreamReader reader = new StreamReader(filePath))
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        Console.WriteLine(line);
                    }
                }

                Console.WriteLine("CSV generated.");

            }
        }

        /// <summary>
        /// Deletes all models associated with specified Cognitive Services account.
        /// </summary>
        public void DeleteAllModels()
        {
            var modelInfoList = recommender.GetAllModels();
            try
            {
                foreach (var model in modelInfoList.Models)
                {
                    recommender.DeleteModel(model.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered: {0}", e);
            }

        }

        /// <summary>
        /// Prints all models in Cognitive Services account.
        /// </summary>
        public void PrintAllModels()
        {
            try
            {
                var modelInfoList = recommender.GetAllModels();
                foreach (var model in modelInfoList.Models)
                {
                    Console.WriteLine("Name: {0}, Id: {1}", model.Name, model.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered: {0}", e);
            }
        }

        /// <summary>
        /// Print all builds for a given model.
        /// </summary>
        /// <param name="modelId"></param>
        public void PrintAllBuilds(string modelId)
        {
            try
            {
                var buildInfoList = recommender.GetAllBuilds(modelId);
                foreach (var build in buildInfoList.Builds)
                {
                    Console.WriteLine("Id: {0}", build.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered: {0}", e);
            }
        }

        /// <summary>
        /// Creates a model.
        /// </summary>
        /// <param name="modelName"></param>
        /// <returns></returns>
        public string CreateModel(string modelName)
        {
            string modelId;
            Console.WriteLine("Creating a new model {0}...", modelName);
            ModelInfo modelInfo = recommender.CreateModel(modelName, "MSStore");
            modelId = modelInfo.Id;
            Console.WriteLine("Model '{0}' created with ID: {1}", modelName, modelId);
            return modelId;
        }

        /// <summary>
        /// Upload catalog and usage files and trigger a new build.
        /// </summary>
        /// <param name="modelId"></param>
        /// <param name="buildType"></param>
        /// <returns></returns>
        public long UploadDataAndTrainModel(string modelId, BuildType buildType)
        {
            long buildId = -1;

            // Import data to the model.
            UploadNewCatalog();
            UploadNewUsage();

            //Trigger a build.
            buildId = TriggerBuild(buildType);

            return buildId;
        }

        /// <summary>
        /// Triggers a new build for given machine learning model.
        /// </summary>
        /// <param name="buildType"></param>
        /// <returns></returns>
        public long TriggerBuild(BuildType buildType)
        {
            // Trigger a recommendation build.
            string operationLocationHeader;
            Console.WriteLine("Triggering build for model '{0}'. \nThis will take a few minutes...", modelId);
            if (buildType == BuildType.Recommendation)
                buildId = recommender.CreateRecommendationsBuild(modelId,
                    "Recommendation Build " + DateTime.UtcNow.ToString("yyyyMMddHHmmss"),
                    enableModelInsights: false,
                    operationLocationHeader: out operationLocationHeader);
            else
                buildId = recommender.CreateFbtBuild(modelId,
                    "Frequenty-Bought-Together Build " + DateTime.UtcNow.ToString("yyyyMMddHHmmss"),
                    enableModelInsights: false,
                    operationLocationHeader: out operationLocationHeader);

            // Monitor the build and wait for completion.
            Console.WriteLine("Monitoring build {0}", buildId);
            var buildInfo = recommender.WaitForOperationCompletion<BuildInfo>(operationLocationHeader);
            Console.WriteLine("Build {0} ended with status {1}.\n", buildId, buildInfo.Status);

            if (String.Compare(buildInfo.Status, "Succeeded", StringComparison.OrdinalIgnoreCase) != 0)
            {
                Console.WriteLine("Build {0} did not end successfully, the sample app will stop here.", buildId);
                Console.WriteLine("Press any key to end");
                Console.ReadKey();
                return -1;
            }

            // Waiting  in order to propagate the model updates from the build...
            Console.WriteLine("Waiting for 40 sec for propagation of the built model...");
            Thread.Sleep(TimeSpan.FromSeconds(40));

            // The below api is more meaningful when you want to give a certain build id to be an active build.
            // Currently this app has a single build which is already active.
            Console.WriteLine("Setting build {0} as active build.", buildId);
            recommender.SetActiveBuild(modelId, buildId);

            return buildId;
        }

        /// <summary>
        /// Uploads new usage data to machine learning model for retraining purposes.
        /// </summary>
        public void UploadNewUsage()
        {
            Console.WriteLine("Make sure you have uploaded all new purchase data to Resources/usage.csv");
            Console.WriteLine("Press any key to continue.");
            Console.ReadKey(true);
            //Upload CSV file to model.
            try
            {
                Console.WriteLine("Importing usage data...");
                string resourcesDir = "../../Resources";
                foreach (string usage in Directory.GetFiles(resourcesDir, "usage.csv"))
                {
                    FileInfo usageFile = new FileInfo(usage);
                    recommender.UploadUsage(modelId, usageFile.FullName, usageFile.Name);
                }
                //Clear all file contents.
                File.WriteAllText(Path.Combine(resourcesDir, "usage.csv"), string.Empty);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in UploadNewUsage(): " + e);
            }
        }

        /// <summary>
        /// Adds products in catalog file (filepath) to the machine learning model.
        /// </summary>
        public void UploadNewCatalog()
        {
            // Check that catalog CSV contains correct content.
            Console.WriteLine("Make sure /Resources/catalog.csv contains ONLY new products (not already in ML model).");
            Console.WriteLine("Follow CSV schema specified here: https://westus.dev.cognitive.microsoft.com/docs/services/Recommendations.V4.0/operations/56f316efeda5650db055a3e1 ");
            Console.WriteLine("Press any key to continue.");
            Console.ReadKey(true);

            // Import data to the model.            
            Console.WriteLine("Importing catalog files...");
            string resourcesDir = "../../Resources";
            foreach (string catalog in Directory.GetFiles(resourcesDir, "catalog.csv"))
            {
                FileInfo catalogFile = new FileInfo(catalog);
                recommender.UploadCatalog(modelId, catalogFile.FullName, catalogFile.Name);
            }
            //Clear all file contents.
            File.WriteAllText(Path.Combine(resourcesDir, "catalog.csv"), string.Empty);
        }

        /// <summary>
        /// Manages processes to get batch recommendations (calls other methods).
        /// </summary>
        public void BatchRecommendationsManager()
        {
            //Read all products into catalog.csv.
            CatalogToCSV();
            //Read each product id into a list.
            List<ProductList> requestIds = new List<ProductList>();
            string resourcesDir = @"..\..\Resources";
            string filePath = Path.Combine(resourcesDir, "catalog.csv");
            using (StreamReader reader = new StreamReader(filePath))
            {
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    if (!line.IsNullOrWhiteSpace())
                    {
                        string[] values = line.Split(',');
                        var list = new ProductList()
                        {
                            SeedItems = new List<string>() { values[0] }
                        };
                        requestIds.Add(list);
                    }
                }
            }

            List<ProductList> tempRequestIds = new List<ProductList>();
            //Get batch recommendations in sets of 10,000 (API limit), then store in SQL.
            int count = 0;
            while (count < requestIds.Count)
            {
                tempRequestIds.Add(requestIds[count]);
                count++;
                if (count % 10000 == 0)
                {
                    //Create input file for batch requests.
                    CreateBatchInputFile(tempRequestIds);
                    //Run batch recommendations job.
                    GetRecommendationsBatch();
                    //Parse batch output and store in SQL ([dbo].[ItemRecommendations]).
                    ParseBatchOutput();
                    //Empty temp table for next 10,000 products.
                    tempRequestIds.Clear();
                }
            }

        }

        /// <summary>
        /// Creates input file for batch recommendations.
        /// </summary>
        public void CreateBatchInputFile(List<ProductList> requests)
        {
            //Create BatchFile object and set list of requests (10,000 ids per instance). 
            var batchInput = new BatchFile();
            batchInput.requests = requests;
            //Serialize batch object into JSON
            string json = JsonConvert.SerializeObject(batchInput, Formatting.Indented);
            //Write to batchInput.json file.
            string resourcesDir = @"..\..\Resources";
            string filePath = Path.Combine(resourcesDir, "batchInput.json");
            using (StreamWriter file = File.CreateText(filePath))
            {
                file.WriteLine(json);
            }

            Console.WriteLine("Created batch input file.");
        }

        /// <summary>
        /// Uploads batch input file into blob.
        /// </summary>
        public void UploadInputBlob()
        {
            string connectionString = ConfigurationManager.AppSettings["BlobConnectionString"];
            try
            {
                //Retrieve storage account from conection string.
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                //Create the blob client.
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                //Retrieve reference to a container.
                CloudBlobContainer container = blobClient.GetContainerReference("batch");
                //Create the container if it doesn't already exist.
                container.CreateIfNotExists();
                //Retrieve reference to block blob.
                CloudBlockBlob blockBlob = container.GetBlockBlobReference("batchInput.json");
                //Get file path.
                var resourcesDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Resources");
                string inputFileName = "batchInput.json";
                //Create or overwrite "input" blob with contents from file.
                using (var fileStream = System.IO.File.OpenRead(Path.Combine(resourcesDir, inputFileName)))
                {
                    blockBlob.UploadFromStream(fileStream);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("You encountered an error: {0}", e);
            }
        }

        /// <summary>
        /// Shows how to get item-to-item recommendations in batch.
        /// Before you can use this method, you need to provide your blob account name, blob account key, and the input container name.
        /// </summary>
        public void GetRecommendationsBatch()
        {
            #region  setup
            // Set storage credentials and copy input file that defines items we want to get recommendations to the Blob Container.
            string blobStorageAccountName = "recommendationscache";
            const string containerName = "batch";

            string outputContainerName = containerName;
            string baseLocation = "https://" + blobStorageAccountName + ".blob.core.windows.net/";
            string inputFileName = "batchInput.json"; // the batch input
            string outputFileName = "batchOutput.json"; // the batch input
            string errorFileName = "batchError.json"; // the batch input

            string connectionString = ConfigurationManager.AppSettings["BlobConnectionString"];

            // Copy input file from resources directory to blob storage
            CloudStorageAccount sourceStorageAccount = CloudStorageAccount.Parse(connectionString);
            BlobHelper bh = new BlobHelper(sourceStorageAccount, containerName);
            string resourcesDir = @"..\..\Resources";
            bh.PutBlockBlob(containerName, inputFileName, File.ReadAllText(Path.Combine(resourcesDir, inputFileName)));

            var inputSas = BlobHelper.GenerateBlobSasToken(connectionString, containerName, inputFileName);
            var outputSas = BlobHelper.GenerateBlobSasToken(connectionString, outputContainerName, outputFileName);
            var errorSas = BlobHelper.GenerateBlobSasToken(connectionString, outputContainerName, errorFileName);

            // Now we need to define the batch job to perform.
            BatchJobsRequestInfo batchJobsRequestInfo = new BatchJobsRequestInfo
            {
                Input = new StorageBlobInfo
                {
                    AuthenticationType = "PublicOrSas",
                    BaseLocation = baseLocation,
                    RelativeLocation = containerName + "/" + inputFileName,
                    SasBlobToken = inputSas
                },

                Output = new StorageBlobInfo
                {
                    AuthenticationType = "PublicOrSas",
                    BaseLocation = baseLocation,
                    RelativeLocation = containerName + "/" + outputFileName,
                    SasBlobToken = outputSas
                },

                Error = new StorageBlobInfo
                {
                    AuthenticationType = "PublicOrSas",
                    BaseLocation = baseLocation,
                    RelativeLocation = containerName + "/" + errorFileName,
                    SasBlobToken = errorSas
                },

                // You may modify the information below to meet your request needs.
                // Note that currently only "ItemRecommend" is supported.
                Job = new JobInfo
                {
                    ApiName = "ItemRecommend",
                    ModelId = modelId, //staging model id for books
                    BuildId = buildId.ToString(),
                    NumberOfResults = "3",
                    IncludeMetadata = "false",
                    MinimalScore = "0"
                }
            };

            #endregion

            #region start the job, wait for completion 

            // kick start the batch job.
            string operationLocationHeader = "";
            var jobId = recommender.StartBatchJob(batchJobsRequestInfo, out operationLocationHeader);

            // Monitor the batch job and wait for completion.
            Console.WriteLine("Monitoring batch job {0}", jobId);
            var batchInfo = recommender.WaitForOperationCompletion<BatchJobInfo>(operationLocationHeader);
            Console.WriteLine("Batch {0} ended with status {1}.\n", jobId, batchInfo.Status);

            if (String.Compare(batchInfo.Status, "Succeeded", StringComparison.OrdinalIgnoreCase) != 0)
            {
                Console.WriteLine("Batch job {0} did not end successfully, the sample app will stop here.", jobId);
                Console.WriteLine("Press any key to end");
                Console.ReadKey();
                return;
            }
            else
            {
                // Copy the output file from blob starage into the local machine.
                Stream reader = bh.GetBlobReader(outputContainerName, outputFileName);
                string outputFullPath = Path.Combine(resourcesDir, outputFileName);
                using (var fileStream = File.Create(outputFullPath))
                {
                    reader.Seek(0, SeekOrigin.Begin);
                    reader.CopyTo(fileStream);
                }

                Console.WriteLine("The output of the blob operation has been saved to: {0}", outputFullPath);
            }
            #endregion
        }

        /// <summary>
        /// Parse output JSON, extract recommendations (ProductIds), batch upload to SQL table.
        /// </summary>
        public void ParseBatchOutput()
        {
            Console.WriteLine("Starting parsing of batch output.");

            //Data for reading batch output file.
            const string containerName = "batch";
            const string blobName = "batchOutput.json";
            string jsonOutput;
            string connectionString = ConfigurationManager.AppSettings["BlobConnectionString"];
            //Store parsed recommendations data.
            Dictionary<int, List<int>> recs = new Dictionary<int, List<int>>();
            int seedItem;
            List<int> seedRecs;

            try
            {
                //Read batch output file from blob storage.
                var sourceStorageAccount = CloudStorageAccount.Parse(connectionString);
                BlobHelper bh = new BlobHelper(sourceStorageAccount, containerName);
                if (!bh.GetBlob(containerName, blobName, out jsonOutput))
                {
                    Console.WriteLine("Failed to read blob - see error message.");
                    return;
                }
                //Parse batch output JSON and store recommendations.
                dynamic output = JObject.Parse(jsonOutput);
                foreach (var result in output.results)
                {
                    seedItem = result.request.seedItems[0];
                    seedRecs = new List<int>();
                    foreach (var recommendation in result.recommendations)
                    {
                        int itemId;
                        //Parse from JValue to int.
                        Int32.TryParse(recommendation.items[0].itemId.ToString(), out itemId);
                        seedRecs.Add(itemId);
                    }
                    if (recs.ContainsKey(seedItem))
                        Console.WriteLine("Error: You have duplicate ProductIds in your catalog. Please fix and create a new ML model.");
                    else
                        recs.Add(seedItem, seedRecs);
                    Console.WriteLine("seedItem: " + seedItem + ", seedRecs: " + seedRecs[0] + ", " + seedRecs[1] + ", " + seedRecs[2]);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in ParseBatchOutput(): " + e);
            }

            string recommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (var connection = new SqlConnection(recommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");

                try
                {
                    StoreBatchRecommendations(recs, connection);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception in StoreBatchRecommendations(): {0}", ex.ToString());
                }
            }
        }

        /// <summary>
        /// Upload batch recommendations into SQL (Recommendations.dbo.ItemRecommendations)
        /// </summary>
        /// <param name="recs"></param>
        /// <param name="connection"></param>
        public void StoreBatchRecommendations(Dictionary<int, List<int>> recs, SqlConnection connection)
        {
            DataTable table = new DataTable("ItemRecommendations");
            //Add columns for datatable schema.
            DataColumn[] cols =
            {
                new DataColumn("ProductId", typeof(string)) {Unique = true, AllowDBNull = false },
                new DataColumn("RecOne", typeof(string)) {AllowDBNull = true },
                new DataColumn("RecTwo", typeof(string)) {AllowDBNull = true },
                new DataColumn("RecThree", typeof(string)) {AllowDBNull = true }
            };
            table.Columns.AddRange(cols);
            DataColumn[] primaryKeyColumns = new DataColumn[1];
            primaryKeyColumns[0] = table.Columns["ProductId"];
            table.PrimaryKey = primaryKeyColumns;
            List<Object> rows = new List<Object>();
            //Add each row of data to datatable.
            foreach (KeyValuePair<int, List<int>> kvp in recs)
            {
                DataRow row = table.NewRow();
                row["ProductId"] = kvp.Key;
                row["RecOne"] = kvp.Value[0];
                row["RecTwo"] = kvp.Value[1];
                row["RecThree"] = kvp.Value[2];
                table.Rows.Add(row);
            }

            //Create BulkOperation (Nuget Package), and merge (upsert) data.
            var bulk = new BulkOperation(connection);
            bulk.DestinationTableName = "ItemRecommendations";
            bulk.RetryCount = 5;
            bulk.RetryInterval = new TimeSpan(100);
            bulk.BulkMerge(table);
        }

        /// <summary>
        /// For testing purposes - to get recommendations for a single ProductId.
        /// </summary>
        /// <param name="recommender"></param>
        /// <param name="buildId"></param>
        /// <param name="productId"></param>
        public void GetRecommendationsSingleRequest(RecommendationsApiWrapper recommender, long buildId, string productId)
        {
            // Get item to item recommendations. (I2I)
            Console.WriteLine();
            Console.WriteLine("Getting Item to Item for {0}", productId);
            string itemIds = productId;
            var itemSets = recommender.GetRecommendations(modelId, buildId, itemIds, 6);
            if (itemSets.RecommendedItemSetInfo != null)
            {
                foreach (RecommendedItemSetInfo recoSet in itemSets.RecommendedItemSetInfo)
                {
                    foreach (var item in recoSet.Items)
                    {
                        Console.WriteLine("Item id: {0} \n Item name: {1} \t (Rating  {2})", item.Id, item.Name, recoSet.Rating);
                    }
                }
            }
            else
            {
                Console.WriteLine("No recommendations found.");
            }

            // Now let's get a user recommendation (U2I)
            Console.WriteLine();
            Console.WriteLine("Getting User Recommendations for User: 0003BFFDC7118D12");
            string userId = "0003BFFDC7118D12";
            itemSets = recommender.GetUserRecommendations(modelId, buildId, userId, 6);
            if (itemSets.RecommendedItemSetInfo != null)
            {
                foreach (RecommendedItemSetInfo recoSet in itemSets.RecommendedItemSetInfo)
                {
                    foreach (var item in recoSet.Items)
                    {
                        Console.WriteLine("Item id: {0} \n Item name: {1} \t (Rating  {2})", item.Id, item.Name, recoSet.Rating);
                    }
                }
            }
            else
            {
                Console.WriteLine("No recommendations found.");
            }
        }

        /// <summary>
        /// Retrains model by generating a new build if new catalog or usage data has been uploaded.
        /// </summary>
        /// <param name="buildType"></param>
        public long RetrainModel(BuildType buildType)
        {
            //Gemerate new build.
            buildId = TriggerBuild(buildType);
            Console.WriteLine("{0} is your new active build.", buildId);

            //Set new build as active build for ML model.
            recommender.SetActiveBuild(modelId, buildId);

            //Delete old build(s).
            BuildInfoList buildInfoList = recommender.GetAllBuilds(modelId);
            foreach (var build in buildInfoList.Builds)
            {
                if (build.Id != buildId)
                    recommender.DeleteBuild(modelId, build.Id);
            }

            return buildId;
        }
    }
}
