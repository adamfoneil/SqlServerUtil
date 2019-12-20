using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace SqlIntegration.Library.Extensions
{
    public static class SqlConnectionExtensions
    {
        public static async Task<bool> SchemaExistsAsync(this SqlConnection connection, string schemaName)
        {
            throw new NotImplementedException();
        }
    }
}
