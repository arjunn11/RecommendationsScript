using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecommendationsManager
{
    class ConsoleApp
    {
        private static string accountKey;
        private const string BaseUri = "https://westus.api.cognitive.microsoft.com/recommendations/v4.0";
        private static RecommendationsManager manager = null;
        private static RecommendationsApiWrapper recommender = null;
        private static string modelId = null;
        private static long buildId = -1;

        public static void Main(string[] args)
        {
            //---REMOVE AND INTEGRATE INTO GUI---
            modelId = "898ef0c9-1338-46a5-8b73-51db22ee78f2";
            buildId = 1568858;
            accountKey = "22fe1376df4444f3b75712ecc208b028";
            //---REMOVE AND INTEGRATE INTO GUI---

            if (string.IsNullOrEmpty(accountKey))
            {
                Console.WriteLine("Please enter your Recommendations API Account key:");
                accountKey = Console.ReadLine();
            }
            if (string.IsNullOrEmpty(modelId))
            {
                Console.WriteLine("enter model id");
                modelId = Console.ReadLine();
            }
            if (buildId == -1)
            {
                Console.WriteLine("enter build id");
                if (!(long.TryParse(Console.ReadLine(), out buildId)))
                {
                    Console.WriteLine("Invalid input. Try again.");
                }
            }

            bool quit = false;
            recommender = new RecommendationsApiWrapper(accountKey, BaseUri);
            manager = new RecommendationsManager(accountKey, recommender, modelId, buildId);

            while (true)
            {
                #region
                Console.WriteLine("Enter 1 to quit");

                //---Prepare/Manage Training Data---
                Console.WriteLine("Enter 2 to delete all purchase data.");
                Console.WriteLine("Enter 3 to export product data into catalog.csv file");
                Console.WriteLine("Enter 4 to export new purchase data into usage.csv file");

                //---Machine Learning Model Scripts---
                Console.WriteLine("Enter 5 to create a new model, upload data, and train model.");
                Console.WriteLine("Enter 6 to print all current models.");
                Console.WriteLine("Enter 7 to delete all current models.");
                Console.WriteLine("Enter 8 to delete a model by modelid.");
                Console.WriteLine("Enter 9 to print all builds for a model.");

                //---Get Recommendations from Machine Learning Model---
                Console.WriteLine("Enter 10 to generate & store batch recommendations for all products.");
                Console.WriteLine("Enter 11 to get recommendations for a single product.");

                //---Retrain Machine Learning Model with New Data---
                Console.WriteLine("Enter 12 to upload a new usage file and retrain ML model.");
                Console.WriteLine("Enter 13 to add new items to catalog and publish to ML model.");
                #endregion

                int input;
                while (true)
                {
                    if (Int32.TryParse(Console.ReadLine(), out input))
                        break;
                    else
                        Console.WriteLine("Invalid input. Try again.");
                }
                try
                {
                    #region
                    switch (input)
                    {
                        case 1: quit = true; break;
                        case 2: manager.DeleteRawData(); break;
                        case 3: manager.CatalogToCSV(); break;
                        case 4: manager.UsageToCSVManager(); break;
                        case 5:
                            Console.WriteLine("Enter model name:");
                            string modelName = Console.ReadLine();
                            modelId = manager.CreateModel(modelName);
                            buildId = manager.UploadDataAndTrainModel(modelId, BuildType.Recommendation);
                            manager.SetBuildId(buildId);
                            manager.SetModelId(modelId);
                            break;
                        case 6: manager.PrintAllModels(); break;
                        case 7: manager.DeleteAllModels(); break;
                        case 8:
                            Console.WriteLine("Enter a model id:");
                            modelId = Console.ReadLine();
                            recommender.DeleteModel(modelId);
                            break;
                        case 9:
                            Console.WriteLine("Enter a model id:");
                            modelId = Console.ReadLine();
                            manager.PrintAllBuilds(modelId);
                            break;
                        case 10: manager.BatchRecommendationsManager(); break;
                        case 11:
                            Console.WriteLine("Enter a productid:");
                            string productId = Console.ReadLine();
                            manager.GetRecommendationsSingleRequest(recommender, buildId, productId);
                            break;
                        case 12: manager.UploadNewUsage(); buildId = manager.RetrainModel(BuildType.Recommendation); break;
                        case 13: manager.UploadNewCatalog(); buildId = manager.RetrainModel(BuildType.Recommendation); break;
                    }
                    #endregion
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error in Main: {0}", e.Message);
                }
                Console.WriteLine("Finished operation(s). \n");
                if (quit) break;
            }
        }
    }
}
