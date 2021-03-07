using Dapper;
using Dapper.CX.Extensions;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
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

        public static async Task<bool> ViewExistsAsync(this SqlConnection connection, DbObject dbObject)
        {
            return await connection.ViewExistsAsync(dbObject.Schema, dbObject.Name);
        }

        public static async Task<bool> ViewExistsAsync(this SqlConnection connection, string schema, string viewName)
        {
            return await connection.RowExistsAsync("[sys].[views] WHERE SCHEMA_NAME([schema_id])=@schema AND [name]=@viewName", new { schema, viewName });
        }

        /// <summary>
        /// adapted from https://github.com/adamfoneil/SqlSchema/blob/master/SqlSchema.SqlServer/SqlServerAnalyzer_Tables.cs#L97-L110
        /// </summary>
        public static async Task<IEnumerable<string>> GetKeyColumnsAsync(this SqlConnection connection, string constraintName, IDbTransaction txn = null) => 
            await connection.QueryAsync<string>(
                @"SELECT
	                [col].[name]
                FROM
	                [sys].[index_columns] [xcol]
	                INNER JOIN [sys].[indexes] [x] ON [xcol].[object_id]=[x].[object_id] AND [xcol].[index_id]=[x].[index_id]
	                INNER JOIN [sys].[columns] [col] ON [xcol].[object_id]=[col].[object_id] AND [xcol].[column_id]=[col].[column_id]
	                INNER JOIN [sys].[tables] [t] ON [x].[object_id]=[t].[object_id]
                WHERE	
	                [x].[name]=@constraintName", new { constraintName }, txn);


        /// <summary>
        /// adapted from https://github.com/adamfoneil/SqlSchema/blob/master/SqlSchema.SqlServer/SqlServerAnalyzer_ForeignKeys.cs#L30-L44
        /// </summary>
        public static async Task<IEnumerable<string>> GetForeignKeyColumnsAsync(this SqlConnection connection, string constraintName, IDbTransaction txn = null) =>
            await connection.QueryAsync<string>(
                @"SELECT						                
	                [child_col].[name]
                FROM
	                [sys].[foreign_keys] [fk]
	                INNER JOIN [sys].[foreign_key_columns] [fkcol] ON [fk].[object_id]=[fkcol].[constraint_object_id]
	                INNER JOIN [sys].[tables] [child_t] ON [fkcol].[parent_object_id]=[child_t].[object_id]
	                INNER JOIN [sys].[columns] [child_col] ON
		                [child_t].[object_id]=[child_col].[object_id] AND
		                [fkcol].[parent_column_id]=[child_col].[column_id]
                WHERE
	                [fk].[name]=@constraintName", new { constraintName }, txn);        
    }
}
