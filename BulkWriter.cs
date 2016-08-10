using Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.SqlAzure;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureMLRecoSampleApp
{
    public class BulkWriter
    {
        const int MaxRetry = 5;
        const int DelayMs = 100;

        private readonly string tableName;
        private readonly Dictionary<string, string> tableMap;
        private readonly string conString;

        /// <summary>
        /// Constructor: set table name (tableName), connection string (conString), and column mapping (tableMap).
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="tableMap"></param>
        public BulkWriter(string tableName,
                                    Dictionary<string, string> tableMap)
        {
            this.tableName = tableName;
            this.tableMap = tableMap;
            conString = ConfigurationManager.ConnectionStrings["RecommendationsCS"].ConnectionString;
        }

        public void WriteWithRetries(DataTable datatable)
        {
            TryWrite(datatable);
        }

        /// <summary>
        /// Create retry policy, attempt write.
        /// </summary>
        /// <param name="datatable"></param>
        private void TryWrite(DataTable datatable)
        {
            var policy = MakeRetryPolicy();
            try
            {
                policy.ExecuteAction(() => Write(datatable));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Trace.TraceError(ex.ToString());
                throw;
            }
        }

        /// <summary>
        /// Upload data to SQL using SqlBulkCopy object.
        /// </summary>
        /// <param name="datatable"></param>
        private void Write(DataTable datatable)
        {
            // connect to SQL
            using (var connection = new SqlConnection(conString))
            {
                var bulkCopy = MakeSqlBulkCopy(connection);
                connection.Open();
                using (var dataTableReader = new DataTableReader(datatable))
                {
                    bulkCopy.WriteToServer(dataTableReader);
                }
                connection.Close();
            }
        }

        /// <summary>
        /// Handle transient faults: https://msdn.microsoft.com/en-us/library/hh680901(v=pandp.50).aspx
        /// </summary>
        /// <returns></returns>
        private RetryPolicy<SqlAzureTransientErrorDetectionStrategy> MakeRetryPolicy()
        {
            var fromMilliseconds = TimeSpan.FromMilliseconds(DelayMs);
            var policy = new RetryPolicy<SqlAzureTransientErrorDetectionStrategy>
                (MaxRetry, fromMilliseconds);
            return policy;
        }

        /// <summary>
        /// Create SqlBulkCopy object and set parameters.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private SqlBulkCopy MakeSqlBulkCopy(SqlConnection connection)
        {
            var bulkCopy = new SqlBulkCopy
                       (
                       connection,
                       SqlBulkCopyOptions.TableLock |
                       SqlBulkCopyOptions.FireTriggers |
                       SqlBulkCopyOptions.UseInternalTransaction,
                       null
                       )
            {
                DestinationTableName = tableName,
                EnableStreaming = true
            };

            tableMap
                .ToList()
                .ForEach(kp =>
                {
                    bulkCopy
                .ColumnMappings
                .Add(kp.Key, kp.Value);
                });
            return bulkCopy;
        }
    }
}


