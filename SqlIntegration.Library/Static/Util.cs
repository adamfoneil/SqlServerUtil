using SqlIntegration.Library.Queries;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public static class Util
    {
        public static async Task<string> GetViewAsTableDefinitionAsync(
            SqlConnection connection, DbObject sourceView, DbObject destTable,
            params string[] keyColumns)
        {
            return await GetViewAsTableDefinitionAsync(connection, sourceView.Schema, sourceView.Name, destTable.Schema, destTable.Name, keyColumns);
        }

        public static async Task<string> GetViewAsTableDefinitionAsync(
            SqlConnection connection,
            string sourceSchema, string sourceView, string destSchema, string destTable,
            params string[] keyColumns)
        {
            var columns = await new ViewColumns() { SchemaName = sourceSchema, ViewName = sourceView }.ExecuteAsync(connection);

            List<string> members = new List<string>();
            members.AddRange(columns.Select(col => col.GetSyntax()));

            if (keyColumns?.Any() ?? false)
            {
                members.Add($"CONSTRAINT [PK_{destSchema}_{destTable}] PRIMARY KEY ({string.Join(", ", keyColumns.Select(col => $"[{col}]"))})");
            }

            return $"CREATE TABLE [{destSchema}].[{destTable}] (\r\n\t" +
                string.Join(",\r\n\t", members) +
                "\r\n)";
        }
    }
}
