using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlIntegration.Library.Classes
{
    public class BulkInsertOptions
    {
        public bool TruncateFirst { get; set; }
        public string SkipIdentityColumn { get; set; }
        public bool IdentityInsert { get; set; }
        public Func<SqlConnection, DataRow, Task<bool>> IncludeRowCallback { get; set; }
        public IProgress<RowOperationProgress> Progress { get; set; }
        public CancellationToken CancellationToken { get; set; }
        public bool DisableIndexes { get; set; }
        public int CommandTimeout { get; set; } = 30;
    }
}
