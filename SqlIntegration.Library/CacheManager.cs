using SqlIntegration.Library.Interfaces;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public abstract class CacheManager<TModel> where TModel : ICacheRow
    {
        /// <summary>
        /// Implement this to execute the calculation you want to cache.
        /// Use this to call your functions, stored procedures, or other unique logic that's slow 
        /// </summary>
        protected abstract Task UpdateCacheAsync(IDbConnection connection, TModel row, IDbTransaction txn = null);

        /// <summary>
        /// Updates the IsValid flag and Timestamp in the database for a given TModel row
        /// </summary>
        protected abstract Task SetRowStatusAsync(IDbConnection connection, bool isValid, TModel row, IDbTransaction txn = null);

        /// <summary>
        /// Marks a row as invalid.
        /// Use this to indicate that a row is out of date, and should be updated the next time it's queried
        /// </summary>
        public async Task InvalidateRowAsync(IDbConnection connection, TModel row, IDbTransaction txn = null) => await SetRowStatusAsync(connection, false, row, txn);

        /// <summary>
        /// loops through TModel rows, updating the invalid rows, and returns all valid rows
        /// </summary>
        public async Task<IEnumerable<TModel>> UpdateAsync(IDbConnection connection, IEnumerable<TModel> rows, IDbTransaction txn = null)
        {
            List<TModel> results = new List<TModel>(rows.Where(row => row.IsValid));            
            
            foreach (var row in rows.Where(row => !row.IsValid))
            {
                await UpdateCacheAsync(connection, row, txn);
                await SetRowStatusAsync(connection, true, row, txn);
                results.Add(row);
            }

            return results;
        }
    }
}
