using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public class DataMigrator
    {
        public DataMigrator(int jobId) { JobId = jobId; }

        public int JobId { get; }

        public async static Task<DataMigrator> StartAsync(SqlConnection cn)
        {
            await CreateDbObjectsAsync(cn);
            int jobId = await CreateJobAsync(cn);

            var result = new DataMigrator(jobId);
            return result;
        }

        private static Task<int> CreateJobAsync(SqlConnection cn)
        {
            throw new NotImplementedException();
        }

        private static Task CreateDbObjectsAsync(SqlConnection cn)
        {
            throw new NotImplementedException();
        }

        public async Task ExecuteAsync(
            SqlConnection cn, string tableName, string criteria = null, object parameters = null, 
            Dictionary<string, string> setForeignKeys = null,
            Dictionary<string, object> setColumns = null)
        {
            throw new NotImplementedException();
        }
    }
}
