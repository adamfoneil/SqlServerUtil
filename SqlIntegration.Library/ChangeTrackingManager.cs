using Dapper;
using Dapper.CX.SqlServer;
using SqlIntegration.Library.Extensions;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlIntegration.Library
{
    public class ChangeTrackingManager
    {
        private readonly string _schema;
        private readonly string _tableName;

        public ChangeTrackingManager(string schema, string tableName)
        {
            _schema = schema;
            _tableName = tableName;
        }

        public async Task InitializeAsync(SqlConnection connection)
        {
            if (!(await connection.SchemaExistsAsync(_schema)))
            {
                await connection.ExecuteAsync($"CREATE SCHEMA [{_schema}]");
            }

            if (!(await connection.TableExistsAsync(_schema, _tableName)))
            {
                await connection.ExecuteAsync(
                    $@"CREATE TABLE [{_schema}].[{_tableName}] (
                        [ObjectName] nvarchar(255) NOT NULL,
                        [LatestVersion] bigint NOT NULL,
                        [Id] int identity(1,1),
                        CONSTRAINT [PK_{_tableName}_{_tableName}] PRIMARY KEY ([ObjectName]),
                        CONSTRAINT [U_{_schema}_{_tableName}] UNIQUE ([Id])
                    )");
            }
        }

        public async Task ClearAsync(SqlConnection connection, string objectName)
        {
            await connection.ExecuteAsync($"DELETE [{_schema}].[{_tableName}] WHERE [ObjectName]=@objectName", new { objectName });
        }

        private async Task<long> GetCurrentVersionAsync(SqlConnection connection)
        {
            try
            {
                return await connection.QuerySingleAsync<long>("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
            }
            catch
            {
                return 0;
            }
        }

        public async Task SetVersionAsync(SqlConnection connection, string objectName)
        {
            long version = await GetCurrentVersionAsync(connection);

            await new SqlServerCmd($"{_schema}.{_tableName}", "Id")
            {
                { "#ObjectName", objectName },
                { "LatestVersion", version }
            }.MergeAsync<int>(connection);
        }

        public async Task<long> GetVersionAsync(SqlConnection connection, string objectName)
        {
            return await connection.QuerySingleOrDefaultAsync<long>($"SELECT [LatestVersion] FROM [{_schema}].[{_tableName}] WHERE [ObjectName]=@objectName", new { objectName });
        }
    }
}
