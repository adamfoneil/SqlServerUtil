using AdoUtil;
using Dapper;
using Postulate.Integration.SqlServer.Classes;
using Postulate.Integration.SqlServer.Internal;
using Postulate.SqlServer;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Postulate.Integration.SqlServer
{
    public static class BulkInsert
    {
        public static async Task ExecuteAsync(
            SqlConnection sourceConnection, string sourceQuery,
            SqlConnection destConnection, DbObject destObject,
            int batchSize, BulkInsertOptions options = null)
        {
            if (options?.TruncateFirst ?? false)
            {
                await destConnection.ExecuteAsync($"TRUNCATE TABLE [{destObject.Schema}].[{destObject.Name}]");
            }

            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, destObject.Schema, destObject.Name);
            var data = sourceConnection.QueryTable(sourceQuery);

            MultiValueInsert mvi = new MultiValueInsert();
            do
            {
                mvi = await GetMultiValueInsert(destObject, data, mvi.StartRow, batchSize, destConnection, options);
                await destConnection.ExecuteAsync(mvi.Sql);
            } while (mvi.RowsInserted > 0);
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

        /// <summary>
        /// Generates a multi-value INSERT statement from a DataTable and returns the number of rows included
        /// </summary>
        private static async Task<MultiValueInsert> GetMultiValueInsert(
            DbObject intoTable, DataTable dataTable, int startRow, int batchSize, SqlConnection connection, BulkInsertOptions options = null)
        {
            var columns = dataTable.Columns.OfType<DataColumn>().Where(col => !col.ColumnName.Equals(options?.SkipIdentityColumn ?? string.Empty));

            string baseCmd =
                $@"INSERT INTO [{intoTable.Schema}].[{intoTable.Name}] (
                    {string.Join(", ", columns.Select(col => $"[{col.ColumnName}]"))}
                ) VALUES ";

            List<string> values = new List<string>();
            int rows = 0;
            for (int row = startRow; row < startRow + batchSize; row++)
            {
                if (row > dataTable.Rows.Count) break;

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
