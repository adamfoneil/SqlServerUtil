using AdoUtil;
using Dapper;
using Postulate.Base;
using Postulate.Integration.SqlServer.Classes;
using Postulate.Integration.SqlServer.Internal;
using Postulate.SqlServer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Postulate.Integration.SqlServer
{
    public class Migrator
    {
        public async Task MergeRowsAsync<TIdentity>(
            SqlConnection sourceConnection, string sourceObject, SqlConnection destConnection, 
            string destTable, string identityColumn, IEnumerable<string> keyColumns, 
            bool truncateFirst)
        {
            await MergeRowsAsync<TIdentity>(
                sourceConnection, DbObject.Parse(sourceObject), destConnection, 
                DbObject.Parse(destTable), identityColumn, keyColumns, 
                truncateFirst);
        }

        public async Task MergeRowsAsync<TIdentity>(
            SqlConnection sourceConnection, DbObject sourceObject, 
            SqlConnection destConnection, DbObject destObject, string identityColumn, IEnumerable<string> keyColumns, 
            bool truncateFirst)
        {
            if (truncateFirst)
            {
                await destConnection.ExecuteAsync($"TRUNCATE TABLE [{destObject.Schema}].[{destObject.Name}]");
            }

            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, destObject.Schema, destObject.Name, keyColumns);
            cmd.IdentityInsert = true;
            if (string.IsNullOrEmpty(cmd.IdentityColumn) && !string.IsNullOrEmpty(identityColumn)) cmd.IdentityColumn = identityColumn;

            var data = sourceConnection.QueryTable($"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}]");

            foreach (DataRow row in data.Rows)
            {
                cmd.BindDataRow(row);
                await cmd.MergeAsync<TIdentity>(destConnection);
            }
        }

        public async Task BulkInsertAsync(
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
                mvi = GetMultiValueInsert(destObject, data, mvi.StartRow, batchSize);
                await destConnection.ExecuteAsync(mvi.Sql);
            } while (mvi.RowsInserted > 0);
        }

        public async Task BulkInsertAsync(
            SqlConnection sourceConnection, DbObject sourceObject, 
            SqlConnection destConnection, DbObject destObject, 
            int batchSize, BulkInsertOptions options = null)
        {
            await BulkInsertAsync(
                sourceConnection, $"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}]", 
                destConnection, destObject, batchSize, options);
        }

        public async Task BulkInsertAsync(
            SqlConnection sourceConnection, string sourceObject, 
            SqlConnection destConnection, string destObject, 
            int batchSize, BulkInsertOptions options = null)
        {
            await BulkInsertAsync(
                sourceConnection, DbObject.Parse(sourceObject), destConnection, 
                DbObject.Parse(destObject), batchSize, options);
        }

        /// <summary>
        /// Generates a multi-value INSERT statement from a DataTable and returns the number of rows included
        /// </summary>
        private MultiValueInsert GetMultiValueInsert(DbObject intoTable, DataTable dataTable, int startRow, int batchSize, string skipIdentityColumn = null)
        {
            var columns = dataTable.Columns.OfType<DataColumn>().Where(col => !col.ColumnName.Equals(skipIdentityColumn ?? string.Empty));

            string baseCmd =
                $@"INSERT INTO [{intoTable.Schema}].[{intoTable.Name}] (
                    {string.Join(", ", columns.Select(col => $"[{col.ColumnName}]"))}
                ) VALUES ";

            List<string> values = new List<string>();
            int rows = 0;
            for (int row = startRow; row < startRow + batchSize; row++)
            {
                if (row > dataTable.Rows.Count) break;
                values.Add(ValueClause(columns, dataTable.Rows[row]));
                rows++;
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
        private string ValueClause(IEnumerable<DataColumn> columns, DataRow dataRow)
        {            
            return "(" + string.Join(", ", columns.Select(col => ToSqlLiteral(col, dataRow))) + ")\r\n";
        }

        private string ToSqlLiteral(DataColumn col, DataRow dataRow)
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
