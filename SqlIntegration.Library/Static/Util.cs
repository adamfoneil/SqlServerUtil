using SqlIntegration.Library.Queries;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public static class Util
    {
        public static async Task<string> GetViewAsTableDefinitionAsync(
            SqlConnection connection, 
            string sourceSchema, string sourceView, string destSchema, string destTable)
        {
            var columns = await new ViewColumns() { SchemaName = sourceSchema, ViewName = sourceView }.ExecuteAsync(connection);

            return $"CREATE TABLE [{destSchema}].[{destTable}] (\r\n\t" +
                string.Join(",\r\n\t", columns.Select(col => col.GetSyntax())) +
                "\r\n)";
        }        
    }
}
