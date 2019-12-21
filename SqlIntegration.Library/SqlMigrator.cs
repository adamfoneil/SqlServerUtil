using Dapper;
using Dapper.CX.SqlServer;
using SqlIntegration.Library.Extensions;
using SqlIntegration.Library.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public class SqlMigrator<TIdentity>
    {
        private const string Schema = "migrate";
        private const string JobTable = "Job";
        private const string KeyMapTable = "KeyMap";

        public SqlMigrator(int jobId) { JobId = jobId; }

        public int JobId { get; }

        private static Dictionary<Type, KeyMapTableInfo> SupportedIdentityTypes
        {
            get 
            {
                return new Dictionary<Type, KeyMapTableInfo>()
                {
                    { typeof(int), new KeyMapTableInfo() { SqlTypeName = "int", MapTableSuffix = "int" } },
                    { typeof(long), new KeyMapTableInfo() { SqlTypeName = "bigint", MapTableSuffix = "long" } },
                    { typeof(Guid), new KeyMapTableInfo() { SqlTypeName = "uniqueidentifier", MapTableSuffix = "guid" } }
                };
            }
        }

        public async static Task<SqlMigrator<TIdentity>> StartJobAsync(SqlConnection cn)
        {
            await CreateDbObjectsAsync(cn);

            int jobId = await new SqlServerCmd($"{Schema}.{JobTable}", "Id", new string[] { "DateCompleted" }).InsertAsync<int>(cn);
            
            var result = new SqlMigrator<TIdentity>(jobId);
            return result;
        }

        private static async Task CreateDbObjectsAsync(SqlConnection cn)
        {
            if (!(await cn.SchemaExistsAsync(Schema)))
            {
                await cn.ExecuteAsync($"CREATE SCHEMA [{Schema}]");
            }

            if (!(await cn.TableExistsAsync(Schema, JobTable)))
            {
                await cn.ExecuteAsync($@"CREATE TABLE [{Schema}].[{JobTable}] (
                    [Id] int identity(1,1) PRIMARY KEY,
                    [DateCreated] datetime NOT NULL DEFAULT (getutcdate()),
                    [DateCompleted] datetime NULL
                )");
            }
            
            string tableName = GetTableName(out KeyMapTableInfo info);            
            if (!(await cn.TableExistsAsync(Schema, tableName)))
            {
                await cn.ExecuteAsync($@"CREATE TABLE [{Schema}].[{tableName}] (
                    [JobId] int NOT NULL,
                    [Timestamp] datetime NOT NULL DEFAULT (getutcdate()),
                    [TableName] nvarchar(100) NOT NULL,
                    [SourceId] {info.SqlTypeName} NOT NULL,
                    [NewId] {info.SqlTypeName} NOT NULL,
                    CONSTRAINT [PK_KeyMap_{info.MapTableSuffix}] PRIMARY KEY ([TableName], [SourceId]),
                    CONSTRAINT [U_KeyMap_{info.MapTableSuffix}_NewId] UNIQUE ([TableName], [NewId]),
                    CONSTRAINT [FK_KeyMap_{info.MapTableSuffix}_Job] FOREIGN KEY ([JobId]) REFERENCES [{Schema}].[{JobTable}] ([Id])
                )");
            }
        }

        private static string GetTableName()
        {
            return GetTableName(out _);
        }

        private static string GetTableName(out KeyMapTableInfo info)
        {
            info = SupportedIdentityTypes[typeof(TIdentity)];
            return $"{KeyMapTable}_{info.MapTableSuffix}";
        }

        public async Task CopySelfAsync(
            SqlConnection connection, string schema, string tableName, string criteria = null, object parameters = null, 
            Dictionary<string, string> setForeignKeys = null,
            Dictionary<string, object> setColumns = null)
        {
            throw new NotImplementedException();
        }

        public async Task CopyToAsync(
            SqlConnection fromConnection, SqlConnection toConnection, string schema, string tableName, string criteria = null, object parameters = null,
            Dictionary<string, string> setForeignKeys = null,
            Dictionary<string, object> setColumns = null)
        {
            throw new NotImplementedException();
        }
    }
}
