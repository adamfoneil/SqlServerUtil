using Dapper.CX.Extensions;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlIntegration.Library.Extensions
{
    public static class SqlConnectionExtensions
    {
        public static async Task<bool> SchemaExistsAsync(this SqlConnection connection, string schemaName)
        {
            return await connection.RowExistsAsync("[sys].[schemas] WHERE [name]=@name", new { name = schemaName });
        }

        public static async Task<bool> TableExistsAsync(this SqlConnection connection, DbObject dbObject)
        {
            return await TableExistsAsync(connection, dbObject.Schema, dbObject.Name);
        }

        public static async Task<bool> TableExistsAsync(this SqlConnection connection, string schema, string tableName)
        {
            return await connection.RowExistsAsync("[sys].[tables] WHERE SCHEMA_NAME([schema_id])=@schema AND [name]=@tableName", new { schema, tableName });
        }
    }
}
