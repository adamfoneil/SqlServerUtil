using AdoUtil;
using Postulate.SqlServer;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Postulate.Integration.SqlServer
{
    public class ViewMigrator
    {
        public async Task MergeAsync<TIdentity>(SqlConnection sourceConnection, string sourceView, SqlConnection destConnection, string destTable, string identityColumn, IEnumerable<string> keyColumns)
        {
            await MergeAsync<TIdentity>(sourceConnection, DbObject.Parse(sourceView), destConnection, DbObject.Parse(destTable), identityColumn, keyColumns);
        }

        public async Task MergeAsync<TIdentity>(SqlConnection sourceConnection, DbObject source, SqlConnection destConnection, DbObject dest, string identityColumn, IEnumerable<string> keyColumns)
        {
            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, dest.Schema, dest.Name, keyColumns);
            cmd.IdentityInsert = true;
            if (string.IsNullOrEmpty(cmd.IdentityColumn) && !string.IsNullOrEmpty(identityColumn)) cmd.IdentityColumn = identityColumn;

            var data = sourceConnection.QueryTable($"SELECT * FROM [{source.Schema}].[{source.Name}]");

            foreach (DataRow row in data.Rows)
            {
                cmd.BindDataRow(row);
                await cmd.MergeAsync<TIdentity>(destConnection);
            }
        }
    }
}
