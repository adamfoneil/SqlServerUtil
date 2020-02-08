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
    public abstract class CacheManager<TRow, TKey> where TRow : ICacheRow
    {
        private readonly SqlServerIntCrudProvider _crudProvider = new SqlServerIntCrudProvider();

        /// <summary>
        /// how do we get the latest data for a list of Ts
        /// </summary>
        protected abstract Task<IEnumerable<TRow>> GetUpdatesAsync(SqlConnection connection, IEnumerable<TRow> currentRows);

        protected abstract (string, IEnumerable<object>) GetKeyCriteria(IEnumerable<TRow> rows);

        protected abstract string SourceView { get; }

        protected abstract string IntoTable { get; }

        protected abstract TimeSpan RecalcAfter { get; }

        public async Task InvalidateAsync(SqlConnection connection, TKey id)
        {
            throw new NotImplementedException();
        }

        public async Task InvalidRangeAsync(SqlConnection connection, IEnumerable<TKey> ids)
        {
            throw new NotImplementedException(); 
        }

        public async Task<TRow> GetAsync(SqlConnection connection, TKey id)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<TRow>> QueryAsync(SqlConnection connection, object criteria)
        {
            var results = new List<TRow>();
            
            var queryResults = await connection.QueryAsync<TRow>(_crudProvider.GetQuerySingleWhereStatement(typeof(TRow), criteria), criteria);

            results.AddRange(queryResults.Where(row => IsValid(row)));

            var updates = await UpdateInvalidRowsAsync(connection, queryResults);
            results.AddRange(updates);
            
            return results;            
        }

        private async Task<IEnumerable<TRow>> UpdateInvalidRowsAsync(SqlConnection connection, IEnumerable<TRow> queryResults)
        {
            var invalidRows = queryResults.Where(row => IsInvalid(row));
            
            if (invalidRows.Any())
            {
                var criteria = GetKeyCriteria(invalidRows);
                var updates = await GetUpdatesAsync(connection, invalidRows);
            }

            throw new NotImplementedException();
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
