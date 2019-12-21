using Dapper;
using Dapper.CX.SqlServer;
using SqlIntegration.Library.Extensions;
using SqlIntegration.Library.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public class SqlMigrator<TIdentity>
    {
        private const string Schema = "migrate";
        private const string JobTable = "Job";
        private const string KeyMapTable = "KeyMap";

        public SqlMigrator(int jobId) 
        { 
            JobId = jobId; 
        }

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
                    [Schema] nvarchar(100) NOT NULL,
                    [TableName] nvarchar(100) NOT NULL,
                    [SourceId] {info.SqlTypeName} NOT NULL,
                    [NewId] {info.SqlTypeName} NOT NULL,
                    CONSTRAINT [PK_KeyMap_{info.MapTableSuffix}] PRIMARY KEY ([Schema], [TableName], [SourceId]),
                    CONSTRAINT [U_KeyMap_{info.MapTableSuffix}_NewId] UNIQUE ([Schema], [TableName], [NewId]),
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
            try
            {
                info = SupportedIdentityTypes[typeof(TIdentity)];
                return $"{KeyMapTable}_{info.MapTableSuffix}";
            }
            catch (Exception exc)
            {
                string supportedTypes = string.Join(", ", SupportedIdentityTypes.Keys.Select(k => k.Name));
                throw new Exception($"Error getting KeyMap table name: {exc.Message}. Make sure you're using a supported type: {supportedTypes}");
            }
        }

        public async Task CopyRowsAsync(
            SqlConnection connection, 
            string schema, string tableName, DataTable dataTable, string identityColumn, 
            Dictionary<string, string> mapForeignKeys = null,
            Dictionary<string, object> setColumns = null)
        {
            var mappingCmd = await SqlServerCmd.FromTableSchemaAsync(connection, Schema, GetTableName());
            mappingCmd["JobId"] = JobId;
            mappingCmd["Schema"] = schema;
            mappingCmd["TableName"] = tableName;

            var cmd = await SqlServerCmd.FromTableSchemaAsync(connection, schema, tableName);
            
            await ValidateForeignKeyMappingAsync(connection, mapForeignKeys);
            ValidateSetColumns(setColumns, cmd);            
            EnsureNoColumnNameOverlap(mapForeignKeys, setColumns);

            foreach (DataRow dataRow in dataTable.Rows)
            {
                TIdentity sourceId = dataRow.Field<TIdentity>(identityColumn);
                
                // has this row already been mapped?
                if (await RowIsMappedAsync(connection, schema, tableName, sourceId)) continue;
                
                cmd.BindDataRow(dataRow);
                if (mapForeignKeys != null) MapForeignKeys(dataRow, mapForeignKeys);
                if (setColumns != null) SetColumns(dataRow, setColumns);

                try
                {
                    // copy the source row to the destination connection
                    TIdentity newId = await cmd.InsertAsync<TIdentity>(connection);

                    try
                    {
                        mappingCmd["SourceId"] = sourceId;
                        mappingCmd["NewId"] = newId;
                        await mappingCmd.InsertAsync<TIdentity>(connection);
                    }
                    catch
                    {
                        // if there's a mapping problem, we need to quit right away and diagnose
                        throw;
                    }
                }
                catch (Exception exc)
                {
                    // insert errors should be logged with the job
                    throw;
                }
            }
        }

        private void EnsureNoColumnNameOverlap(Dictionary<string, string> mapForeignKeys, Dictionary<string, object> setColumns)
        {
            if (mapForeignKeys == null) return;
            if (setColumns == null) return;

            var overlap = from fk in mapForeignKeys.Keys
                          join col in setColumns.Keys on fk.ToLower() equals col.ToLower()
                          select fk;

            if (overlap.Any())
            {
                string keyList = string.Join(", ", overlap);
                throw new Exception($"Can't have overlapping foreign keys and columns: {keyList}");
            }
        }

        private void ValidateSetColumns(Dictionary<string, object> setColumns, SqlServerCmd cmd)
        {
            throw new NotImplementedException();
        }

        private async Task ValidateForeignKeyMappingAsync(SqlConnection connection, Dictionary<string, string> mapForeignKeys)
        {
            if (mapForeignKeys == null) return;

            var validSources = await connection.QueryAsync<string>(
                $@"SELECT [Schema] + '.' + [TableName] AS [Table]
                FROM {GetTableName()}
                GROUP BY [Schema], [TableName]");

            var invalid = mapForeignKeys.Keys.Except(validSources);
            if (invalid.Any())
            {
                string invalidKeys = string.Join(", ", invalid);
                throw new Exception($"There are one or more unrecognized foreign key mappings: {invalidKeys}");
            }
        }

        private void SetColumns(DataRow dataRow, Dictionary<string, object> setColumns)
        {
            throw new NotImplementedException();
        }

        private void MapForeignKeys(DataRow dataRow, Dictionary<string, string> mapForeignKeys)
        {
            throw new NotImplementedException();
        }

        private Task<bool> RowIsMappedAsync(SqlConnection connection, string schema, string tableName, TIdentity idValue)
        {
            throw new NotImplementedException();
        }

        public async Task CopyAcrossAsync(
            SqlConnection fromConnection, SqlConnection toConnection, string schema, string tableName, string criteria = null, object parameters = null,
            Dictionary<string, string> setForeignKeys = null,
            Dictionary<string, object> setColumns = null)
        {
            throw new NotImplementedException();
        }
    }
}
