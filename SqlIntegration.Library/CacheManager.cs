using SqlIntegration.Library.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public abstract class CacheManager<TKeyColumns, TResult> where TResult : ICacheRow
    {
        private readonly ChangeTrackingManager _ctm;
        private readonly string _objectName;

        public CacheManager(string schema, string tableName, string trackObjectName = null)
        {
            _ctm = new ChangeTrackingManager(schema, tableName);
            _objectName = trackObjectName ?? typeof(TResult).Name;
        }

        /// <summary>
        /// how much time can elapse on an invalid row before it must be updated?
        /// </summary>
        protected abstract TimeSpan AllowAging { get; }

        /// <summary>
        /// how do we query what's changed since the last cache update?
        /// This should use FROM CHANGETABLE(CHANGES *tablename*, @version) AS [changes]
        /// </summary>
        protected abstract Task<IEnumerable<TKeyColumns>> GetChangesAsync(SqlConnection connection, long version);

        /// <summary>
        /// how do we updathe cache value?
        /// </summary>
        protected abstract Task UpdateCacheAsync(SqlConnection connection, TKeyColumns key);

        public async Task UpdateAsync(SqlConnection connection)
        {
            await _ctm.InitializeAsync(connection);

            var version = await _ctm.GetVersionAsync(connection, _objectName);
            var changes = await GetChangesAsync(connection, version);

            foreach (TKeyColumns key in changes)
            {

            }

            await _ctm.SetVersionAsync(connection, _objectName);
        }
        

        private bool OutOfDate(TResult row)
        {
            return DateTime.UtcNow.Subtract(row.Timestamp) > AllowAging;
        }
    }
}
