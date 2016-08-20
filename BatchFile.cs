using System.Collections.Generic;

namespace RecommendationsManager
{
    /// <summary>
    /// Object that is serialized to input JSON file fpor batch recommendations.
    /// </summary>
    public class BatchFile
    {
        public List<ProductList> requests { get; set; }
    }

    public class ProductList
    {
        public List<string> SeedItems { get; set; }
    }
}
