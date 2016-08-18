using System.Collections.Generic;

namespace RecommendationsManager
{
    public class BatchFile
    {
        public List<ProductList> requests { get; set; }
    }

    public class ProductList
    {
        public List<string> SeedItems { get; set; }
    }
}
