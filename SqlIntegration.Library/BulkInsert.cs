using Dapper;
using DataTables.Library;
using SqlIntegration.Library.Classes;
using SqlIntegration.Library.Internal;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

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

        public static async Task ChunkExecuteAsync(
            SqlConnection sourceConnection, DbObject sourceObject, string chunkColumn, int chunkCount,
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

                for (int chunk = 0; chunk < chunkCount; chunk++)
                {
                    string query = $"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}] WHERE [{chunkColumn}] % {chunkCount} = {chunk}";
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

        public static async Task ExecuteAsync<T>(IEnumerable<T> sourceData, SqlConnection destConnection, DbObject destObject, 
            int batchSize, params Expression<Func<T, object>>[] columns)
        {
            var inserts = GetMultiValueInserts(destObject, sourceData, batchSize, columns).ToArray();
            foreach (var mvi in inserts)
            {
                await destConnection.ExecuteAsync(mvi.Sql);
            }
        }        

        public static async Task<StringBuilder> GetSqlStatementsAsync(string intoTable, DataTable dataTable, BulkInsertOptions options = null)
        {
            var obj = DbObject.Parse(intoTable);
            return await GetSqlStatementsAsync(obj, dataTable, options);
        }

        public static async Task<StringBuilder> GetSqlStatementsAsync(DbObject intoTable, DataTable dataTable, BulkInsertOptions options = null)
        {
            StringBuilder result = new StringBuilder();

            bool identityInsert = options?.IdentityInsert ?? false && string.IsNullOrEmpty(options?.SkipIdentityColumn);

            if (identityInsert)
            {
                result.AppendLine($"SET IDENTITY_INSERT [{intoTable.Schema}].[{intoTable.Name}] ON\r\n");
            }

            var mvi = await GetMultiValueInsertAsync(intoTable, dataTable, 0, dataTable.Rows.Count, options: options);
            result.AppendLine(mvi.InsertStatement);
            result.Append(string.Join(",", mvi.Values));

            if (identityInsert)
            {
                result.AppendLine($"\r\nSET IDENTITY_INSERT [{intoTable.Schema}].[{intoTable.Name}] OFF");
            }

            return result;
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

                if (options?.IdentityInsert ?? false)
                {
                    await destConnection.ExecuteAsync($"SET IDENTITY_INSERT [{destObject.Schema}].[{destObject.Name}] ON\r\n");
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

                if (options?.IdentityInsert ?? false)
                {
                    await destConnection.ExecuteAsync($"SET IDENTITY_INSERT [{destObject.Schema}].[{destObject.Name}] OFF");
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
            DbObject intoTable, DataTable dataTable, int startRow, int batchSize, SqlConnection connection = null, BulkInsertOptions options = null)
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
                if (hasPredicate && connection != null)
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
                StartRow = startRow + rows,
                InsertStatement = baseCmd,
                Values = values
            };
        }

        public static IEnumerable<MultiValueInsert> GetMultiValueInserts<T>(
            DbObject intoTable, IEnumerable<T> rows, int batchSize, params Expression<Func<T, object>>[] columns)
        {
            var columnNames = columns.Select(col => PropertyNameFromLambda(col));

            string baseCmd =
                $@"INSERT INTO [{intoTable.Schema}].[{intoTable.Name}] (
                    {string.Join(", ", columnNames.Select(col => $"[{col}]"))}
                ) VALUES ";

            int skip = 0;                        

            do
            {
                var batch = rows.Skip(skip).Take(batchSize);
                if (!batch.Any()) break;

                var dataTable = batch.ToDataTable();
                RemoveExtraColumns(dataTable, columnNames);

                List<string> values = new List<string>();
                foreach (DataRow row in dataTable.Rows)
                {
                    values.Add(ValueClause(dataTable.Columns.OfType<DataColumn>(), row, columnNames));
                }

                yield return new MultiValueInsert()
                {
                    Sql = baseCmd + string.Join(", ", values),
                    RowsInserted = batch.Count(),
                    StartRow = skip,
                    InsertStatement = baseCmd,
                    Values = values
                };

                skip += batchSize;
            } while (true);

            void RemoveExtraColumns(DataTable dataTable, IEnumerable<string> keepColumns)
            {
                var removeColumns = dataTable.Columns.OfType<DataColumn>().Select(col => col.ColumnName).Except(keepColumns).ToArray();
                foreach (var col in removeColumns) dataTable.Columns.Remove(col);
            }
        }

        private static string PropertyNameFromLambda(Expression expression)
        {
            // thanks to http://odetocode.com/blogs/scott/archive/2012/11/26/why-all-the-lambdas.aspx
            // thanks to http://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression

            LambdaExpression le = expression as LambdaExpression;
            if (le == null) throw new ArgumentException("expression");

            MemberExpression me = null;
            if (le.Body.NodeType == ExpressionType.Convert)
            {
                me = ((UnaryExpression)le.Body).Operand as MemberExpression;
            }
            else if (le.Body.NodeType == ExpressionType.MemberAccess)
            {
                me = le.Body as MemberExpression;
            }

            if (me == null) throw new ArgumentException("expression");

            return me.Member.Name;
        }

        /// <summary>
        /// Converts values from a data row into a VALUES clause, escaping and delimiting strings where appropriate
        /// </summary>
        private static string ValueClause(IEnumerable<DataColumn> columns, DataRow dataRow, IEnumerable<string> columnOrder = null)
        {
            IEnumerable<DataColumn> useColumns = columns;

            if (columnOrder != null)
            {
                var useOrder = columnOrder.Select((item, index) => new
                {
                    Item = item,
                    Index = index
                });

                useColumns = from col in columns
                             join order in useOrder on col.ColumnName equals order.Item
                             orderby order.Index
                             select col;
            }
                
            return "(" + string.Join(", ", useColumns.Select(col => ToSqlLiteral(col, dataRow))) + ")\r\n";
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
