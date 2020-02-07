using Dapper;
using Dapper.CX.SqlServer;
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
        private readonly SqlServerIntCrudProvider _crudProvider = new SqlServerIntCrudProvider();

        /// <summary>
        /// how do we get the latest data for a list of Ts
        /// </summary>
        protected abstract Task<IEnumerable<T>> GetUpdatesAsync(SqlConnection connection, IEnumerable<T> currentRows);

        protected abstract (string, IEnumerable<object>) GetKeyCriteria(IEnumerable<T> rows);

        protected abstract string SourceView { get; }

        protected abstract string IntoTable { get; }

        protected abstract TimeSpan RecalcAfter { get; }

        public async Task<IEnumerable<T>> QueryAsync(SqlConnection connection, object criteria)
        {
            var results = new List<T>();
            
            var queryResults = await connection.QueryAsync<T>(_crudProvider.GetQuerySingleWhereStatement(typeof(T), criteria), criteria);

            results.AddRange(queryResults.Where(row => IsValid(row)));

            var updates = await UpdateInvalidRowsAsync(connection, queryResults);
            results.AddRange(updates);
            
            return results;            
        }

        private async Task<IEnumerable<T>> UpdateInvalidRowsAsync(SqlConnection connection, IEnumerable<T> queryResults)
        {
            var invalidRows = queryResults.Where(row => IsInvalid(row));
            
            if (invalidRows.Any())
            {
                var criteria = GetKeyCriteria(invalidRows);
                var updates = await GetUpdatesAsync(connection, invalidRows);
            }

        }

        private bool IsValid(ICacheRow row)
        {
            return !row.IsInvalid;
        }

        private bool IsInvalid(ICacheRow row)
        {
            if (row.IsInvalid)
            {
                if (row.LastRefreshed.HasValue && RecalcAfter > TimeSpan.Zero)
                {
                    return DateTime.UtcNow.Subtract(row.LastRefreshed.Value) > RecalcAfter;
                }

                return true;
            }

            return false;
        }
    }
}
