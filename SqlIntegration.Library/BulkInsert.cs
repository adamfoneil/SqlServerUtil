using Dapper;
using DataTables.Library;
using SqlIntegration.Library.Classes;
using SqlIntegration.Library.Internal;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public static class BulkInsert
    {
        public static async Task OffsetExecuteAsync(
            SqlConnection sourceConnection, DbObject sourceObject, string orderBy, int offsetSize,
            SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await TruncateFirstAsync(destConnection, destObject, options);

            int page = 0;

            do
            {
                DataTable data = await GetOffsetDataAsync(sourceConnection, sourceObject, orderBy, offsetSize, page);
                if (data.Rows.Count == 0) break;
                await ExecuteInnerAsync(destConnection, destObject, batchSize, options, data, page);
                page++;
            } while (true);
        }

        public static async Task ExecuteModuloAsync(
            SqlConnection sourceConnection, DbObject sourceObject, string moduloColumn, int moduloCount,
            SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            bool disableIndexes = options?.DisableIndexes ?? false;

            if (options != null)
            {
                if (options.TruncateFirst)
                {
                    await TruncateFirstAsync(destConnection, destObject, options);
                }

                // truncate only the first time
                options.TruncateFirst = false;
                // don't try to disable indexes
                options.DisableIndexes = false;
            }

            try
            {
                if (disableIndexes)
                {
                    await ToggleIndexesAsync(destConnection, destObject, "DISABLE", options?.CommandTimeout ?? 30);
                }

                for (int chunk = 0; chunk < moduloCount; chunk++)
                {
                    string query = $"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}] WHERE [{moduloColumn}] % {moduloCount} = {chunk}";
                    await ExecuteAsync(sourceConnection, query, destConnection, destObject, batchSize, options);
                }
            }
            finally
            {
                if (disableIndexes)
                {
                    await ToggleIndexesAsync(destConnection, destObject, "REBUILD", options?.CommandTimeout ?? 30);
                }
            }
        }

        public static async Task ExecuteAsync(
            SqlConnection sourceConnection, string sourceQuery,
            SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await TruncateFirstAsync(destConnection, destObject, options);

            var data = await sourceConnection.QueryTableAsync(sourceQuery);

            await ExecuteInnerAsync(destConnection, destObject, batchSize, options, data, 0);
        }

        public static async Task ExecuteAsync(
            SqlConnection sourceConnection, DbObject sourceObject,
            SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await ExecuteAsync(
                sourceConnection, $"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}]",
                destConnection, destObject, batchSize, options);
        }

        public static async Task ExecuteAsync(
            SqlConnection sourceConnection, string sourceObject,
            SqlConnection destConnection, string destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await ExecuteAsync(
                sourceConnection, DbObject.Parse(sourceObject), destConnection,
                DbObject.Parse(destObject), batchSize, options);
        }

        public static async Task ExecuteAsync(
            DataTable sourceData, SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await ExecuteInnerAsync(destConnection, destObject, batchSize, options, sourceData, 0);
        }

        public static async Task ExecuteAsync(
            DataTable sourceData, SqlConnection destConnection, string destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            await ExecuteInnerAsync(destConnection, DbObject.Parse(destObject), batchSize, options, sourceData, 0);
        }

        private static async Task<DataTable> GetOffsetDataAsync(SqlConnection connection, DbObject dbObject, string orderBy, int offsetSize, int page)
        {
            int offset = page * offsetSize;
            string query = $"SELECT * FROM [{dbObject.Schema}].[{dbObject.Name}] ORDER BY {orderBy} OFFSET {offset} ROWS FETCH NEXT {offsetSize} ROWS ONLY";
            return await connection.QueryTableAsync(query);
        }

        private static async Task TruncateFirstAsync(SqlConnection destConnection, DbObject destObject, BulkInsertOptions options)
        {
            if (options?.TruncateFirst ?? false)
            {
                await destConnection.ExecuteAsync($"TRUNCATE TABLE [{destObject.Schema}].[{destObject.Name}]");
            }
        }

        private static async Task ExecuteInnerAsync(SqlConnection destConnection, DbObject destObject, int batchSize, BulkInsertOptions options, DataTable data, int page)
        {
            try
            {
                if (options?.DisableIndexes ?? false)
                {
                    await ToggleIndexesAsync(destConnection, destObject, "DISABLE", options?.CommandTimeout ?? 30);
                }

                int totalRows = data.Rows.Count;
                MultiValueInsert mvi = new MultiValueInsert();
                do
                {
                    if (options?.CancellationToken.IsCancellationRequested ?? false) break;
                    mvi = await GetMultiValueInsertAsync(destObject, data, mvi.StartRow, batchSize, destConnection, options);
                    if (mvi.RowsInserted == 0) break;
                    await destConnection.ExecuteAsync(mvi.Sql);
                    options?.Progress?.Report(new RowOperationProgress() { TotalRows = totalRows, RowsCompleted = mvi.StartRow + mvi.RowsInserted, CurrentOffset = page });
                } while (true);
            }
            finally
            {
                if (options?.DisableIndexes ?? false)
                {
                    await ToggleIndexesAsync(destConnection, destObject, "REBUILD", options?.CommandTimeout ?? 30);
                }
            }
        }

        private static async Task ToggleIndexesAsync(SqlConnection connection, DbObject dbObject, string command, int commandTimeout)
        {
            await connection.ExecuteAsync($"ALTER INDEX ALL ON [{dbObject.Schema}].[{dbObject.Name}] {command}", commandTimeout: commandTimeout);
        }

        /// <summary>
        /// Generates a multi-value INSERT statement from a DataTable and returns the number of rows included
        /// </summary>
        private static async Task<MultiValueInsert> GetMultiValueInsertAsync(
            DbObject intoTable, DataTable dataTable, int startRow, int batchSize, SqlConnection connection, BulkInsertOptions options = null)
        {
            if (dataTable.Rows.Count == 0) return new MultiValueInsert() { RowsInserted = 0 };

            var columns = dataTable.Columns.OfType<DataColumn>().Where(col => !col.ColumnName.Equals(options?.SkipIdentityColumn ?? string.Empty));

            string baseCmd =
                $@"INSERT INTO [{intoTable.Schema}].[{intoTable.Name}] (
                    {string.Join(", ", columns.Select(col => $"[{col.ColumnName}]"))}
                ) VALUES ";

            List<string> values = new List<string>();
            int rows = 0;
            for (int row = startRow; row < startRow + batchSize; row++)
            {
                if (row > dataTable.Rows.Count - 1) break;

                var dataRow = dataTable.Rows[row];

                bool hasPredicate = options?.IncludeRowCallback != null;

                bool includeRow = true;
                if (hasPredicate)
                {
                    includeRow = await options.IncludeRowCallback.Invoke(connection, dataRow);
                }

                if (includeRow)
                {
                    values.Add(ValueClause(columns, dataRow));
                    rows++;
                }
            }

            return new MultiValueInsert()
            {
                Sql = baseCmd + string.Join(", ", values),
                RowsInserted = rows,
                StartRow = startRow + rows
            };
        }

        /// <summary>
        /// Converts values from a data row into a VALUES clause, escaping and delimiting strings where appropriate
        /// </summary>
        private static string ValueClause(IEnumerable<DataColumn> columns, DataRow dataRow)
        {
            return "(" + string.Join(", ", columns.Select(col => ToSqlLiteral(col, dataRow))) + ")\r\n";
        }

        private static string ToSqlLiteral(DataColumn col, DataRow dataRow)
        {
            if (dataRow.IsNull(col)) return "null";

            string result = dataRow[col].ToString();

            string quote(string input)
            {
                return "'" + input + "'";
            };

            if (col.DataType.Equals(typeof(bool)))
            {
                result = ((bool)dataRow[col]) ? "1" : "0";
            }

            if (col.DataType.Equals(typeof(string)))
            {
                result = result.Replace("'", "''");
                result = quote(result);
            }

            if (col.DataType.Equals(typeof(DateTime)))
            {
                result = quote(result);
            }

            return result;
        }

    }
}
