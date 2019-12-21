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
        private const string KeyMapTableName = "KeyMap";

        private SqlMigrator() 
        {             
        }        

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

        public async static Task<SqlMigrator<TIdentity>> InitializeAsync(SqlConnection cn)
        {
            await CreateDbObjectsAsync(cn);
            return new SqlMigrator<TIdentity>();
        }

        private static async Task CreateDbObjectsAsync(SqlConnection cn)
        {
            if (!(await cn.SchemaExistsAsync(Schema)))
            {
                await cn.ExecuteAsync($"CREATE SCHEMA [{Schema}]");
            }

            string tableName = GetTableName(out KeyMapTableInfo info);            
            if (!(await cn.TableExistsAsync(Schema, tableName)))
            {
                await cn.ExecuteAsync($@"CREATE TABLE [{Schema}].[{tableName}] (
                    [Id] bigint identity(1,1) PRIMARY KEY,
                    [Timestamp] datetime NOT NULL DEFAULT (getutcdate()),
                    [Schema] nvarchar(100) NOT NULL,
                    [TableName] nvarchar(100) NOT NULL,
                    [SourceId] {info.SqlTypeName} NOT NULL,
                    [NewId] {info.SqlTypeName} NOT NULL,
                    CONSTRAINT [U_KeyMap_{info.MapTableSuffix}_SourceId] UNIQUE ([Schema], [TableName], [SourceId]),
                    CONSTRAINT [U_KeyMap_{info.MapTableSuffix}_NewId] UNIQUE ([Schema], [TableName], [NewId])                    
                )");
            }
        }

        public DbObject KeyMapTable { get { return new DbObject(Schema, GetTableName()); } }

        private static string GetTableName()
        {
            return GetTableName(out _);
        }

        private static string GetTableName(out KeyMapTableInfo info)
        {
            try
            {
                info = SupportedIdentityTypes[typeof(TIdentity)];
                return $"{KeyMapTableName}_{info.MapTableSuffix}";
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
            mappingCmd["Schema"] = schema;
            mappingCmd["TableName"] = tableName;

            var cmd = await SqlServerCmd.FromTableSchemaAsync(connection, schema, tableName);
            
            await ValidateForeignKeyMappingAsync(connection, mapForeignKeys);
            ValidateSetColumns(setColumns, cmd);            
            EnsureNoColumnNameOverlap(mapForeignKeys, setColumns);

            foreach (DataRow dataRow in dataTable.Rows)
            {
                TIdentity sourceId = dataRow.Field<TIdentity>(identityColumn);
                
                // if this row has already been copied, then skip
                if (await IsRowMappedAsync(connection, new DbObject(schema, tableName), sourceId)) continue;
                
                cmd.BindDataRow(dataRow);
                await MapForeignKeysAsync(connection, dataRow, mapForeignKeys, cmd);
                SetColumns(dataRow, setColumns, cmd);

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
                    catch (Exception exc)
                    {
                        // if there's a mapping problem, we need to quit right away and diagnose
                        throw new Exception($"Error mapping source Id {sourceId} to new Id {newId}: {exc.Message}");
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
            if (setColumns == null) return;

            var invalid = setColumns.Keys.Except(cmd.Keys);
            if (invalid.Any())
            {
                string invalidCols = string.Join(", ", invalid);
                throw new Exception($"There are one or more unrecognized set columns: {invalidCols}");
            }
        }

        private async Task ValidateForeignKeyMappingAsync(SqlConnection connection, Dictionary<string, string> mapForeignKeys)
        {
            if (mapForeignKeys == null) return;

            var validSources = await connection.QueryAsync<string>(
                $@"SELECT [Schema] + '.' + [TableName] AS [Table]
                FROM {GetTableName()}
                GROUP BY [Schema], [TableName]");

            var invalid = mapForeignKeys.Values.Except(validSources);
            if (invalid.Any())
            {
                string invalidKeys = string.Join(", ", invalid);
                throw new Exception($"There are one or more unrecognized foreign key mappings: {invalidKeys}");
            }
        }

        private void SetColumns(DataRow dataRow, Dictionary<string, object> setColumns, SqlServerCmd cmd)
        {
            throw new NotImplementedException();
        }

        private async Task<TIdentity> GetNewIdAsync(SqlConnection cn, DbObject dbObject, TIdentity sourceId)
        {
            try
            {
                return await cn.QuerySingleAsync<TIdentity>(
                    $@"SELECT [NewId] FROM [{Schema}].[{GetTableName()}] WHERE [Schema]=@schema AND [TableName]=@tableName AND [SourceId]=@sourceId",
                    new { schema = dbObject.Schema, tableName = dbObject.Name, sourceId });
            }
            catch (Exception exc)
            {
                throw new Exception($"Error getting new Id for {dbObject.ToString()} from source Id {sourceId}: {exc.Message}");
            }
        }

        private async Task MapForeignKeysAsync(SqlConnection connection, DataRow dataRow, Dictionary<string, string> mapForeignKeys, SqlServerCmd cmd)
        {
            if (mapForeignKeys == null) return;
            
            foreach (var kp in mapForeignKeys)
            {
                if (!dataRow.IsNull(kp.Key))
                {
                    TIdentity sourceId = dataRow.Field<TIdentity>(kp.Key);
                    cmd[kp.Key] = await GetNewIdAsync(connection, DbObject.Parse(kp.Value), sourceId);
                }                
            }
        }

        private async Task<bool> IsRowMappedAsync(SqlConnection connection, DbObject dbObject, TIdentity idValue)
        {
            try
            {
                var newId = await GetNewIdAsync(connection, dbObject, idValue);
                return true;
            }
            catch 
            {
                return false;
            }
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
