using SqlIntegration.Library.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public abstract class CacheManager<TModel> where TModel : ICacheRow
    {
        /// <summary>
        /// how much time can elapse on an invalid row before it must be updated?
        /// </summary>
        protected abstract TimeSpan AllowAging { get; }

        /// <summary>
        /// Implement this to execute the calculation you want to cache.
        /// This should perform an UPDATE or INSERT of data in the TModel target table.
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
        public async Task UpdateAsync(IDbConnection connection, IEnumerable<TModel> rows, IDbTransaction txn = null)
        {            
            foreach (var row in rows)
            {
                if (!row.IsValid && OutOfDate(row))
                {
                    await UpdateCacheAsync(connection, row, txn);
                    row.Timestamp = DateTime.UtcNow;
                    await SetRowStatusAsync(connection, true, row, txn);
                }                
            }

        }

        private bool OutOfDate(TModel row)
        {
            return DateTime.UtcNow.Subtract(row.Timestamp) > AllowAging;
        }
    }
}
