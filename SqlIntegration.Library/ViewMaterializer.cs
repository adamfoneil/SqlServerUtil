using Dapper;
using Dapper.QX;
using SqlIntegration.Library.Extensions;
using SqlIntegration.Library.Queries;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public static class ViewMaterializer
    {
        public static async Task UpdateAsync<TKeyColumns>(SqlConnection connection, Query<TKeyColumns> changesQuery, string sourceView, string intoTable)
        {
            // get some validation, initialization out of the way

            var viewObj = DbObject.Parse(sourceView);
            var tableObj = DbObject.Parse(intoTable);

            if (viewObj.Equals(tableObj)) throw new InvalidOperationException("Source view and table cannot be the same.");

            if (!(await connection.ViewExistsAsync(DbObject.Parse(sourceView)))) throw new ArgumentException($"View not found: {sourceView}");

            if (! (await connection.TableExistsAsync(DbObject.Parse(intoTable))))
            {
                var pkColumns = GetColumnNames<TKeyColumns>();
                string createTable = await Util.GetViewAsTableDefinitionAsync(connection, viewObj, tableObj, pkColumns?.ToArray());
                await connection.ExecuteAsync(createTable);
            }

            // now get and apply the actual changes

            var changes = await changesQuery.ExecuteAsync(connection);

            string criteria = GetWhereClause<TKeyColumns>();
            string columnList = await GetColumnListAsync(connection, sourceView);

            foreach (var change in changes)
            {
                await connection.ExecuteAsync($"DELETE {intoTable} WHERE {criteria}", change);

                await connection.ExecuteAsync(
                    $@"INSERT INTO {DbObject.Delimited(intoTable)} ({columnList}) 
                    SELECT {columnList} 
                    FROM {DbObject.Delimited(sourceView)}
                    WHERE {criteria}", change);
            }
        }

        private static async Task<string> GetColumnListAsync(SqlConnection connection, string viewName)
        {
            var obj = DbObject.Parse(viewName);
            var columns = await new ViewColumns() { SchemaName = obj.Schema, ViewName = obj.Name }.ExecuteAsync(connection);
            return string.Join(", ", columns.Select(col => $"[{col.Name}]"));
        }

        private static IEnumerable<string> GetColumnNames<TKeyColumns>()
        {
            var props = typeof(TKeyColumns).GetProperties();
            return props.Select(pi => pi.Name);
        }

        private static string GetWhereClause<TKeyColumns>()
        {
            var props = typeof(TKeyColumns).GetProperties();
            return string.Join(" AND ", props.Select(pi => $"[{pi.Name}]=@{pi.Name}"));
        }
    }
}
