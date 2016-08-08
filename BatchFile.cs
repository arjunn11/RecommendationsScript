using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLTestScript
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
