using Dapper;
using SqlIntegration.Library.Extensions;
using SqlIntegration.Library.Queries;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public abstract class ViewMaterializer<TKeyColumns>
    {
        private readonly ChangeTrackingManager _ctm;

        public ViewMaterializer()
        {
            _ctm = new ChangeTrackingManager("vm", "SyncVersion");
        }
        
        protected abstract string SourceView { get; }
        protected abstract string IntoTable { get; }
        protected abstract Task<IEnumerable<TKeyColumns>> GetChangesAsync(SqlConnection connection, long version);

        /// <summary>
        /// Remove version history for this view.
        /// This is usually for unit testing only, do not use this without understanding implications.
        /// </summary>
        public async Task ClearVersionAsync(SqlConnection connection)
        {
            try { await _ctm.ClearAsync(connection, SourceView); } catch { /* do nothing */ }
        }

        public async Task ExecuteAsync(SqlConnection connection)
        {
            var sourceObj = DbObject.Parse(SourceView);
            var intoObj = DbObject.Parse(IntoTable);
            await InitializeAsync(connection, sourceObj, intoObj);

            var version = await _ctm.GetVersionAsync(connection, sourceObj.ToString());
            
            var changes = 
                (version != 0) ? await GetChangesAsync(connection, version) :
                await GetAllSourceRows(connection);

            string criteria = GetWhereClause();
            string columnList = await GetColumnListAsync(connection, sourceObj);

            foreach (var change in changes)
            {
                await connection.ExecuteAsync(
                    $"DELETE {intoObj.Delimited()} WHERE {criteria}", change);

                await connection.ExecuteAsync(
                    $@"INSERT INTO {intoObj.Delimited()} ({columnList}) 
                    SELECT {columnList} 
                    FROM {sourceObj.Delimited()}
                    WHERE {criteria}", change);
            }

            await _ctm.SetVersionAsync(connection, sourceObj.ToString());
        }

        private async Task<IEnumerable<TKeyColumns>> GetAllSourceRows(SqlConnection connection)
        {
            return await connection.QueryAsync<TKeyColumns>($"SELECT * FROM {DbObject.Delimited(SourceView)}");
        }

        private async Task InitializeAsync(SqlConnection connection, DbObject sourceView, DbObject intoTable)
        {
            if (sourceView.Equals(intoTable)) throw new InvalidOperationException("Source view and table cannot be the same.");

            if (!(await connection.ViewExistsAsync(sourceView))) throw new ArgumentException($"View not found: {sourceView}");

            if (!(await connection.SchemaExistsAsync(intoTable.Schema)))
            {
                await connection.ExecuteAsync($"CREATE SCHEMA [{intoTable.Schema}]");
            }

            if (!(await connection.TableExistsAsync(intoTable)))
            {
                var pkColumns = GetColumnNames();
                string createTable = await Util.GetViewAsTableDefinitionAsync(connection, sourceView, intoTable, pkColumns?.ToArray());
                await connection.ExecuteAsync(createTable);
            }

            await _ctm.InitializeAsync(connection);
        }

        private static async Task<string> GetColumnListAsync(SqlConnection connection, DbObject view)
        {            
            var columns = await new ViewColumns() { SchemaName = view.Schema, ViewName = view.Name }.ExecuteAsync(connection);
            return string.Join(", ", columns.Select(col => $"[{col.Name}]"));
        }

        private static IEnumerable<string> GetColumnNames()
        {
            var props = typeof(TKeyColumns).GetProperties();
            return props.Select(pi => pi.Name);
        }

        private static string GetWhereClause()
        {
            var props = typeof(TKeyColumns).GetProperties();
            return string.Join(" AND ", props.Select(pi => $"[{pi.Name}]=@{pi.Name}"));
        }

        /// <summary>
        /// This is for unit testing only to be able to prove that the source view and output (i.e reporting) table are the same
        /// </summary>
        public async Task<bool> SourceViewEqualsResultTable(SqlConnection cn)
        {
            var props = typeof(TKeyColumns).GetProperties();
            string orderBy = string.Join(", ", props.Select(pi => $"[{pi.Name}]"));

            var viewSource = await cn.QueryAsync<TKeyColumns>($"SELECT * FROM {DbObject.Delimited(SourceView)} ORDER BY {orderBy}");
            var tableData = await cn.QueryAsync<TKeyColumns>($"SELECT * FROM {DbObject.Delimited(IntoTable)} ORDER BY {orderBy}");
            return viewSource.SequenceEqual(tableData);
        }
    }
}
