using Dapper;
using SqlIntegration.Library;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace Testing.Classes
{
    public class SalesMaterializerResult
    {
        public string Region { get; set; }
        public int Year { get; set; }
        public string ItemClass { get; set; }

        public override bool Equals(object obj)
        {
            try
            {
                var test = obj as SalesMaterializerResult;
                return (test != null) ? test.Region.Equals(Region) && test.Year.Equals(Year) && test.ItemClass.Equals(ItemClass) : false;
            }
            catch 
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return (Region + Year.ToString() + ItemClass).GetHashCode();
        }
    }

    public class SalesMaterializer : ViewMaterializer<SalesMaterializerResult>
    {
        protected override string SourceView => "dbo.SalesHistoryTotals";
        protected override string IntoTable => "rpt.SalesHistoryTotals";

        protected override async Task<IEnumerable<SalesMaterializerResult>> GetChangesAsync(SqlConnection connection, long version)
        {
            return await connection.QueryAsync<SalesMaterializerResult>(
                @"SELECT 
	                [sh].[Region],
	                YEAR([sh].[Date]) AS [Year], 	
	                UPPER(LEFT([sh].[ItemNumber], 2)) AS [ItemClass]
                FROM 
	                CHANGETABLE(CHANGES dbo.SalesHistory, @version) AS [changes]
	                INNER JOIN [dbo].[SalesHistory] [sh] ON [changes].[Id]=[sh].[Id]
                GROUP BY
	                [sh].[Region],
	                YEAR([sh].[Date]),
	                UPPER(LEFT([sh].[ItemNumber], 2))", new { version });
        }
    }
}
