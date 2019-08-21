using AdoUtil;
using Postulate.SqlServer;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Postulate.Integration.SqlServer
{
    public class ViewMigrator
    {
        public async Task MergeAsync<TIdentity>(SqlConnection sourceConnection, string sourceView, SqlConnection destConnection, string destTable, IEnumerable<string> keyColumns)
        {
            await MergeAsync<TIdentity>(sourceConnection, DbObject.Parse(sourceView), destConnection, DbObject.Parse(destTable), keyColumns);
        }

        public async Task MergeAsync<TIdentity>(SqlConnection sourceConnection, DbObject source, SqlConnection destConnection, DbObject dest, IEnumerable<string> keyColumns)
        {
            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, dest.Schema, dest.Name, keyColumns);            

            var data = sourceConnection.QueryTable($"SELECT * FROM [{source.Schema}].[{source.Name}]");

            foreach (DataRow row in data.Rows)
            {
                foreach (DataColumn col in row.Table.Columns)
                {
                    cmd[col.ColumnName] = row[col.ColumnName];
                }

                await cmd.MergeAsync<TIdentity>(destConnection);
            }
        }
    }
}
