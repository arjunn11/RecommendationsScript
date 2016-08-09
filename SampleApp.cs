/**************************************************************************************************
 * This sample shows how to use the Recommendations API. You can find more details on the 
 * Recommendations API and other Cognitive Services at http://go.microsoft.com/fwlink/?LinkID=759709.
 * 
 * The Recommendations API identifies consumption patterns from your transaction information 
 * in order to provide recommendations. These recommendations can help your customers more 
 * easily discover items that they may be interested in.  By showing your customers products that 
 * they are more likely to be interested in, you will, in turn, increase your sales.
 * 
 *  Before you run the application:
 *  1. Sign up for the Recommendations API service and get an API Key.
 *     (http://go.microsoft.com/fwlink/?LinkId=761106 )
 *     
 *  2. Set the AccountKey variable in the RecommendationsSampleApp to the key you got.
 *  
 *  3. Verify the endpoint Uri you got when you subscribed matches the BaseUri as it may 
 *     be different if you selected a different data center.
 *************************************************************************************************/

namespace Recommendations
{
    using AzureMLRecoSampleApp;
    using System;
    using System.IO;
    using System.Reflection;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.Configuration;
    using Newtonsoft.Json.Linq;
    using System.Collections.Generic;
    using System.Text;
    using System.Data;
    using System.Data.SqlClient;
    using Newtonsoft.Json;
    using SQLTestScript;

    public class RecommendationsSampleApp
    {
        private static string AccountKey = "22fe1376df4444f3b75712ecc208b028"; // <---  Set to your API key here.
        private const string BaseUri = "https://westus.api.cognitive.microsoft.com/recommendations/v4.0";
        private static RecommendationsApiWrapper recommender = null;
        
        /// <summary>
        /// Console interface to manage backend processes and data for recommendations.
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            if (String.IsNullOrEmpty(AccountKey))
            {
                Console.WriteLine("Please enter your Recommendations API Account key:");
                AccountKey = Console.ReadLine();
            }

            bool quit = false;
            string modelName;
            string modelId = null;
            long buildId = -1;
            recommender = new RecommendationsApiWrapper(AccountKey, BaseUri);

            while (true)
            {
                int input;
                Console.WriteLine("Enter 0 to quit");
                Console.WriteLine("Enter 1 to create a new model, upload data, and train model (create a recommendations build).");
                Console.WriteLine("Enter 2 to print all current models.");
                Console.WriteLine("Enter 3 to delete all current models.");
                Console.WriteLine("Enter 4 to delete a model by modelid.");
                Console.WriteLine("Enter 5 to print all builds for a model.");
                Console.WriteLine("Enter 6 to run batch recommendations job.");
                Console.WriteLine("Enter 7 to parse batch output.");
                Console.WriteLine("Enter '8' to aggregate all raw purchase data into usage table.");
                Console.WriteLine("Enter '9' to remove all raw and training data.");
                Console.WriteLine("Enter '10' to export all usage data into a CSV file ('/Resources/usage.csv').");
                Console.WriteLine("Enter '11' to export all product/category data to catalog CSV.");
                Console.WriteLine("Enter '12' to generate batchInput.json.");
                Console.WriteLine("Enter '13' to upload batchInput.json file into blob storage.");

                while (true)
                {
                    if (Int32.TryParse(Console.ReadLine(), out input))
                        break;
                    else
                        Console.WriteLine("Invalid input. Try again.");
                }
                try
                {
                    switch (input)
                    {
                        case 0:
                            quit = true;
                            break;
                        case 1:
                            Console.WriteLine("Enter model name");
                            modelName = Console.ReadLine();
                            modelId = CreateModel(modelName);
                            buildId = UploadDataAndTrainModel(modelId, BuildType.Recommendation);
                            break;
                        case 2:
                            PrintAllModels();
                            break;
                        case 3:
                            DeleteAllModels();
                            break;
                        case 4:
                            Console.WriteLine("enter a model id");
                            modelId = Console.ReadLine();
                            recommender.DeleteModel(modelId);
                            break;
                        case 5:
                            Console.WriteLine("enter a model id");
                            modelId = Console.ReadLine();
                            PrintAllBuilds(modelId);
                            break;
                        case 6:
                            /*Console.WriteLine("enter model id");
                            modelId = Console.ReadLine();
                            Console.WriteLine("enter build id");
                            if(!(long.TryParse(Console.ReadLine(), out buildId)))
                            {
                                Console.WriteLine("Invalid input. Try again.");
                                break;
                            }*/
                            modelId = "7412db41-df78-4801-ba58-aa1c6f93b091";
                            buildId = 1566426;
                            GetRecommendationsBatch(recommender, modelId, buildId);
                            break;
                        case 7:
                            ParseBatchOutput();
                            break;
                        case 8:
                            Console.WriteLine("What row to start aggregating data?");
                            int rowNum;
                            while (true)
                            {
                                if (Int32.TryParse(Console.ReadLine(), out rowNum))
                                    break;
                                else
                                    Console.WriteLine("Invalid input. Try again.");
                            }
                            AddTrainingData(rowNum);//Aggregates all raw purchase data into usage format.
                            break;
                        case 9: RemoveTrainingData(); break;
                        case 10: ExportToCSV(); break;
                        case 11: CatalogToCSV(); break;
                        case 12: CreateBatchFile(); break;
                        case 13: UploadInputBlob(); break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: {0}", e.Message);
                }
                Console.WriteLine("Finished operation(s). \n");
                if (quit) break;
            }
        }

        /// <summary>
        /// Deletes all models associated with specified Cognitive Services account.
        /// </summary>
        public static void DeleteAllModels()
        {
            recommender = new RecommendationsApiWrapper(AccountKey, BaseUri);
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
        public static void PrintAllModels()
        {
            recommender = new RecommendationsApiWrapper(AccountKey, BaseUri);
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
        public static void PrintAllBuilds(string modelId)
        {
            recommender = new RecommendationsApiWrapper(AccountKey, BaseUri);
            try
            {
                var buildInfoList = recommender.GetAllBuilds(modelId);
                foreach(var build in buildInfoList.Builds)
                {
                    Console.WriteLine("Name: {0}, Id: {1}", build.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered: {0}", e);
            }
        }

        /// <summary>
        /// Creates a model, upload catalog and usage file and trigger a build.
        /// Returns the Build ID of the trained build.
        /// </summary>
        /// <param name="recommender">Wrapper that maintains API key</param>
        /// <param name="modelId">The model Id</param>
        public static string CreateModel(string modelName)
        {
            string modelId;
            Console.WriteLine("Creating a new model {0}...", modelName);
            ModelInfo modelInfo = recommender.CreateModel(modelName, "MSStore");
            modelId = modelInfo.Id;
            Console.WriteLine("Model '{0}' created with ID: {1}", modelName, modelId);
            return modelId;
        }

        /// <summary>
        /// Creates a model, upload catalog and usage files and trigger a build.
        /// Returns the Build ID of the trained build.
        /// </summary>
        /// <param name="recommender">Wrapper that maintains API key</param>
        /// <param name="buildType">The type of build. (Recommendation or FBT)</param>
        /// <param name="modelId">The model Id</param>
        public static long UploadDataAndTrainModel(string modelId, BuildType buildType)
        {
            long buildId = -1;

            // Import data to the model.            
            var resourcesDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Resources");
            Console.WriteLine("Importing catalog files...");
            foreach (string catalog in Directory.GetFiles(resourcesDir, "catalog.csv"))
            {
                var catalogFile = new FileInfo(catalog);
                recommender.UploadCatalog(modelId, catalogFile.FullName, catalogFile.Name);
            }

            Console.WriteLine("Importing usage data...");
            foreach (string usage in Directory.GetFiles(resourcesDir, "usage.csv"))
            {
                var usageFile = new FileInfo(usage);
                recommender.UploadUsage(modelId, usageFile.FullName, usageFile.Name);
            }

            #region training
            // Trigger a recommendation build.
            string operationLocationHeader;
            Console.WriteLine("Triggering build for model '{0}'. \nThis will take a few minutes...", modelId);
            if (buildType == BuildType.Recommendation)
            {
                buildId = recommender.CreateRecommendationsBuild(modelId, "Recommendation Build " + DateTime.UtcNow.ToString("yyyyMMddHHmmss"),
                                                                     enableModelInsights: false,
                                                                     operationLocationHeader: out operationLocationHeader);
            }
            else
            {
                buildId = recommender.CreateFbtBuild(modelId, "Frequenty-Bought-Together Build " + DateTime.UtcNow.ToString("yyyyMMddHHmmss"),
                                                     enableModelInsights: false,
                                                     operationLocationHeader: out operationLocationHeader);
            }

            // Monitor the build and wait for completion.
            Console.WriteLine("Monitoring build {0}", buildId);
            var buildInfo = recommender.WaitForOperationCompletion(operationLocationHeader);
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
            #endregion

            return buildId;
        }

        /// <summary>
        /// Shows how to get item-to-item recommendations in batch.
        /// Before you can use this method, you need to provide your blob account name, blob account key, and the input container name.
        /// </summary>
        /// <param name="recommender">Wrapper that maintains API key</param>
        /// <param name="modelId">Model ID</param>
        /// <param name="buildId">Build ID</param>
        public static void GetRecommendationsBatch(RecommendationsApiWrapper recommender, string modelId, long buildId)
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
            var sourceStorageAccount = CloudStorageAccount.Parse(connectionString);
            BlobHelper bh = new BlobHelper(sourceStorageAccount, containerName);
            var resourcesDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Resources");
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

            var batchInfo = recommender.WaitForOperationCompletion(operationLocationHeader);
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
        public static void ParseBatchOutput()
        {
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
                    recs.Add(seedItem, seedRecs);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Error: " + e);
            }

            foreach (KeyValuePair<int, List<int>> kvp in recs)
            {
                Console.Write("Key = {0}, {1}, {2}, {3}", kvp.Key, kvp.Value[0], kvp.Value[1], kvp.Value[2]);
                /*foreach(var rec in kvp.Value)
                {
                    Console.Write(rec + " ");
                }*/
                Console.WriteLine();
            }

            string RecommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (var connection = new SqlConnection(RecommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");

                try
                {
                   StoreBatchRecommendations(recs);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("exception: {0}", ex.ToString());
                }
            }
        }

        public static void StoreBatchRecommendations(Dictionary<int, List<int>> recs)
        {
            DataTable table = new DataTable("ItemRecommendations");
            DataColumn[] cols =
            {
                new DataColumn("ProductId", typeof(string)),
                new DataColumn("RecOne", typeof(string)),
                new DataColumn("RecTwo", typeof(string)),
                new DataColumn("RecThree", typeof(string))
            };
            table.Columns.AddRange(cols);
            table.PrimaryKey = new DataColumn[] { table.Columns["ProductId"] };
            List<Object> rows = new List<Object>();
            foreach(KeyValuePair<int, List<int>> kvp in recs)
            {
                rows.Add(new Object[] { kvp.Key, kvp.Value[0], kvp.Value[1], kvp.Value[2] });
            }
            
        }

        /// <summary>
        /// Manages add operation.
        /// </summary>
        /// <param name="rowNum"></param>
        public static void AddTrainingData(int rowNum)
        {
            string RecommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (var connection = new SqlConnection(RecommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");

                try
                {
                    SelectData(connection, rowNum);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("exception: {0}", ex.ToString());
                }
            }
        }

        /// <summary>
        /// Selects raw purchase data and inserts each row.
        /// </summary>
        public static void SelectData(SqlConnection connection, int rowNum)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;//Set connection used by this instance of SqlCommand
                command.CommandType = CommandType.Text;//SQL Text Command
                command.CommandText = @"SELECT UserId, ProductId, Time
                                        FROM PurchaseDataRaw WHERE UniqueId >= (@StartRow); ";
                //Set start row:
                SqlParameter parameter;
                parameter = new SqlParameter("@StartRow", SqlDbType.Int);
                parameter.Value = rowNum;
                command.Parameters.Add(parameter);

                List<string[]> data = new List<string[]>();//Compile all new SQL data into data structure.

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        DateTime dt = reader.GetDateTime(2);//Format datetime into a string
                        string dtString = String.Format("{0}/{1}/{2}T{3}:{4}:{5}", dt.Year.ToString("D4"), dt.Month.ToString("D2"),
                            dt.Day.ToString("D2"), dt.Hour.ToString("D2"), dt.Minute.ToString("D2"), dt.Second.ToString("D2"));
                        string UserId = reader.GetString(0).Replace('@', '_').Replace('.', '_');
                        string[] temp = { UserId, reader.GetString(1), dtString };
                        data.Add(temp);
                        Console.WriteLine("{0}\t{1}\t{2}", UserId, reader.GetString(1), dtString);
                    }
                }

                //Insert each row of data.
                foreach (string[] row in data)
                {
                    InsertRow(connection, row);
                }
                Console.WriteLine("Inserted all rows of data");
            }
        }

        /// <summary>
        /// Insert a row into UsageData SQL table.
        /// </summary>
        public static void InsertRow(SqlConnection connection, string[] row)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = CommandType.Text;
                command.CommandText = @"INSERT INTO UsageData (UserId, ProductId, Time, EventType)
                                           VALUES (@UserId, @ProductId, @Time, @EventType); ";

                SqlParameter parameter;
                parameter = new SqlParameter("@UserId", SqlDbType.NVarChar, 50);
                parameter.Value = row[0];
                command.Parameters.Add(parameter);

                parameter = new SqlParameter("@ProductId", SqlDbType.NVarChar, 50);
                parameter.Value = row[1];
                command.Parameters.Add(parameter);

                parameter = new SqlParameter("@Time", SqlDbType.NVarChar, 50);
                parameter.Value = row[2];
                command.Parameters.Add(parameter);

                parameter = new SqlParameter("@EventType", SqlDbType.NVarChar, 10);
                parameter.Value = "Purchase";//MODIFY AFTER ADDING CLICK EVENTS
                command.Parameters.Add(parameter);

                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Manages remove operation.
        /// </summary>
        public static void RemoveTrainingData()
        {
            string RecommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (var connection = new SqlConnection(RecommendationsConnString))
            {

                connection.Open();
                Console.WriteLine("Connection opened.");

                try
                {
                    DeleteRawData(connection);
                    DeleteUsageData(connection);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("exception: {0}", ex.ToString());
                }
            }
        }

        /// <summary>
        /// Deletes all raw data from PurchaseDataRaw table in Recommendations DB.
        /// </summary>
        public static void DeleteRawData(SqlConnection connection)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.CommandType = CommandType.Text;
                command.CommandText = @"TRUNCATE TABLE PurchaseDataRaw; ";
                command.ExecuteNonQuery();
                Console.WriteLine("Deleted all data in PurchaseDataRaw.");
            }
        }

        /// <summary>
        /// Deletes all training data from UsageData table in Recommendations DB.
        /// </summary>
        public static void DeleteUsageData(SqlConnection connection)
        {
            using (var command = new SqlCommand())
            {
                command.Connection = connection;//Set connection used by this instance of SqlCommand
                command.CommandType = CommandType.Text;//SQL Text Command
                command.CommandText = @"TRUNCATE TABLE UsageData; ";
                command.ExecuteNonQuery();
                Console.WriteLine("Deleted all data in UsageData.");
            }
        }

        /// <summary>
        /// Export catalog information to CSV file.
        /// </summary>
        public static void CatalogToCSV()
        {
            string con = ConfigurationManager.ConnectionStrings["CatalogCS"].ConnectionString;

            using (var connection = new SqlConnection(con))
            {
                connection.Open();
                List<string[]> catalog = new List<string[]>();
                var csv = new StringBuilder();
                var filePath = @"C:\Users\t-arjun\Documents\Visual Studio 2015\Projects\CSVGenerationScript\SQLTestScript\Resources\catalog.csv";

                using (var command = new SqlCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.Connection = connection;
                    command.CommandText = "SELECT ProductId, ProductName, Description, CategoryId FROM Products;";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var ProductId = reader.GetInt32(0).ToString();
                            var ProductName = reader.GetString(1);
                            var Description = reader.GetString(2);
                            var CategoryId = reader.GetInt32(3).ToString();
                            string[] temp = { ProductId, ProductName, CategoryId, Description };
                            catalog.Add(temp);
                        }
                    }
                }

                Dictionary<int, string> categories = new Dictionary<int, string>();

                using (var command = new SqlCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.Connection = connection;
                    command.CommandText = "SELECT CategoryID, CategoryName FROM Categories; ";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            categories.Add(reader.GetInt32(0), reader.GetString(1));
                        }
                    }
                }

                using (var file = new StreamWriter(filePath))
                {
                    foreach (string[] row in catalog)
                    {
                        try
                        {
                            categories.TryGetValue(Convert.ToInt32(row[2]), out row[2]);
                            var tempLine = string.Format("{0},{1},{2},{3}", row[0], row[1], row[2], row[3]);
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
                using (var reader = new StreamReader(filePath))
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
        /// Export UsageData to CSV file.
        /// </summary>
        public static void ExportToCSV()
        {
            string RecommendationsConnString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;

            using (var connection = new SqlConnection(RecommendationsConnString))
            {
                connection.Open();
                Console.WriteLine("Connection opened.");
                using (var command = new SqlCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.Connection = connection;
                    command.CommandText = "SELECT UserId, ProductId, Time, EventType FROM UsageData; ";

                    var resourcesDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Resources");
                    string usageFileName = "usage.csv";

                    using (var file = new StreamWriter(Path.Combine(resourcesDir, usageFileName)))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var UserId = reader.GetString(0);
                                var ProductId = reader.GetString(1);
                                var Time = reader.GetString(2);
                                var EventType = reader.GetString(3);
                                var tempLine = string.Format("{0},{1},{2},{3}", UserId, ProductId, Time, EventType);
                                file.WriteLine(tempLine);
                                file.Flush();
                            }
                        }
                    }

                    Console.WriteLine("Printing content:");
                    string line = "";
                    using (var reader = new StreamReader(Path.Combine(resourcesDir, usageFileName)))
                    {
                        while ((line = reader.ReadLine()) != null)
                        {
                            Console.WriteLine(line);
                        }
                    }

                    Console.WriteLine("CSV generated.");
                }
            }
        }

        /// <summary>
        /// Creates input file for batch recommendations.
        /// </summary>
        public static void CreateBatchFile()
        {
            
            var batchInput = new BatchFile()
            {
                requests = new List<ProductList>() { }
            };
            //Read each id into batch object.
            var filePath = @"C:\Users\t-arjun\Documents\Visual Studio 2015\Projects\CSVGenerationScript\SQLTestScript\Resources\catalog.csv";
            using (var reader = new StreamReader(filePath))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');
                    var list = new ProductList()
                    {
                        SeedItems = new List<string>() { values[0] }
                    };
                    batchInput.requests.Add(list);
                }
            }
            //Serialize batch object into JSON
            string json = JsonConvert.SerializeObject(batchInput, Formatting.Indented);
            //Write to batchInput.json file.
            filePath = @"C:\Users\t-arjun\Documents\Visual Studio 2015\Projects\CSVGenerationScript\SQLTestScript\Resources\batchInput.json";
            using (StreamWriter file = File.CreateText(filePath))
            {
                file.WriteLine(json);
            }
        }

        /// <summary>
        /// Uploads batch input file into blob.
        /// </summary>
        public static void UploadInputBlob()
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
    }
}