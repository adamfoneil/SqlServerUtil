using Dapper;
using SqlIntegration.Library.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    /// <summary>
    /// query helper that caches expensive query results in a temp table,
    /// automatically refreshing the underlying data when it becomes invalid
    /// </summary>
    public abstract class CacheManager<T> where T : ICacheRow
    {
        /// <summary>
        /// how do we get the latest data for a list of Ts
        /// </summary>
        protected abstract Task<IEnumerable<T>> GetUpdatesAsync(SqlConnection connection, IEnumerable<T> currentRows);

        protected abstract string BaseQuery { get; }

        protected abstract TimeSpan RecalcAfter { get; }

        public async Task<IEnumerable<T>> QueryAsync(SqlConnection connection, string criteria, object parameters)
        {
            var results = new List<T>();
            var queryResults = await connection.QueryAsync<T>(BaseQuery + " WHERE " + criteria, parameters);

            results.AddRange(queryResults.Where(row => !row.IsInvalid));

            var invalidRows = queryResults.Where(row => IsInvalid(row));
            if (invalidRows.Any())
            {
                var updates = await GetUpdatesAsync(connection, invalidRows);
            }
            
            return results;            
        }

        private bool IsInvalid(ICacheRow row)
        {
            if (row.IsInvalid)
            {
                if (row.LastRefreshed.HasValue && RecalcAfter > TimeSpan.Zero)
                {
                    return DateTime.UtcNow.Subtract(row.LastRefreshed.Value) > RecalcAfter;
                }                    
            }

            return false;
        }
    }
}
