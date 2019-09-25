using AdoUtil;
using Dapper;
using Postulate.SqlServer;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Postulate.Integration.SqlServer
{
    public class Migrator
    {
        public async Task MergeRowsAsync<TIdentity>(
            SqlConnection sourceConnection, string sourceObject, SqlConnection destConnection, 
            string destTable, string identityColumn, IEnumerable<string> keyColumns, 
            bool truncateFirst)
        {
            await MergeRowsAsync<TIdentity>(
                sourceConnection, DbObject.Parse(sourceObject), destConnection, 
                DbObject.Parse(destTable), identityColumn, keyColumns, 
                truncateFirst);
        }

        public async Task MergeRowsAsync<TIdentity>(
            SqlConnection sourceConnection, DbObject sourceObject, 
            SqlConnection destConnection, DbObject destObject, string identityColumn, IEnumerable<string> keyColumns, 
            bool truncateFirst)
        {
            if (truncateFirst)
            {
                await destConnection.ExecuteAsync($"TRUNCATE TABLE [{destObject.Schema}].[{destObject.Name}]");
            }

            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, destObject.Schema, destObject.Name, keyColumns);
            cmd.IdentityInsert = true;
            if (string.IsNullOrEmpty(cmd.IdentityColumn) && !string.IsNullOrEmpty(identityColumn)) cmd.IdentityColumn = identityColumn;

            var data = sourceConnection.QueryTable($"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}]");

            foreach (DataRow row in data.Rows)
            {
                cmd.BindDataRow(row);
                await cmd.MergeAsync<TIdentity>(destConnection);
            }
        }

        public async Task MergeAsync(
            SqlConnection sourceConnection, DbObject sourceObject, 
            SqlConnection destConnection, DbObject destObject, 
            bool truncateFirst)
        {
            if (truncateFirst)
            {
                await destConnection.ExecuteAsync($"TRUNCATE TABLE [{destObject.Schema}].[{destObject.Name}]");
            }

            var cmd = await SqlServerCmd.FromTableSchemaAsync(destConnection, destObject.Schema, destObject.Name);
            var data = sourceConnection.QueryTable($"SELECT * FROM [{sourceObject.Schema}].[{sourceObject.Name}]");

            using (var adapter = new SqlDataAdapter())
            {
                adapter.UpdateBatchSize = 100;
                adapter.InsertCommand = cmd.GetInsertCommand(destConnection) as SqlCommand;
                adapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;
                int paramIndex = 0;
                foreach (SqlParameter p in adapter.InsertCommand.Parameters)
                {
                    p.SourceColumn = p.ParameterName;
                    paramIndex++;
                    p.ParameterName = $"Parameter{paramIndex}";                    
                }
                if (!truncateFirst) adapter.UpdateCommand = cmd.GetUpdateCommand(destConnection) as SqlCommand;
                foreach (DataRow row in data.Rows) row.SetAdded();
                adapter.Update(data);
            }
        }

        public async Task MergeAsync(
            SqlConnection sourceConnection, string sourceObject, SqlConnection destConnection, string destObject, bool truncateFirst)
        {
            await MergeAsync(sourceConnection, DbObject.Parse(sourceObject), destConnection, DbObject.Parse(destObject), truncateFirst);
        }
    }
}
