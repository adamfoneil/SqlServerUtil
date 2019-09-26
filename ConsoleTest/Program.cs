using Dapper;
using Microsoft.Extensions.Configuration;
using Postulate.Base.Extensions;
using Postulate.Integration.SqlServer;
using Postulate.Integration.SqlServer.Classes;
using System.Data.SqlClient;
using System.IO;

namespace ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.local.json", false)
                .AddJsonFile("config.azure.json", false)
                .Build();

            using (var cnLocal = new SqlConnection(config.GetConnectionString("Local")))
            {
                //var createTable = Util.GetViewAsTableDefinitionAsync(cnLocal, "bi", "AllDocuments", "dbo", "AllDocuments").Result;
                
                using (var cnRemote = new SqlConnection(config.GetConnectionString("Remote")))
                {
                    //cnRemote.Execute(createTable);

                    BulkInsert.ExecuteAsync(cnLocal, "dbo.Batch", cnRemote, "dbo.Batch", 100).Wait();

                    /*BulkInsert.ExecuteAsync(cnLocal, "bi.AllDocuments", cnRemote, "dbo.AllDocuments", 100, new BulkInsertOptions()
                    {
                        TruncateFirst = true
                    }).Wait();*/

                    /*BulkInsert.ExecuteAsync(cnLocal, "bi.AllDocuments", cnRemote, "dbo.AllDocuments", 75, new BulkInsertOptions()
                    {
                        IncludeRowCallback = async (cn, row) =>
                        {
                            var result = await cn.RowExistsAsync("[dbo].[AllDocuments] WHERE [Id]=@id", new { id = row["ID"] });
                            return !result;
                        }
                    }).Wait();*/

                    //BulkInsert.ExecuteAsync(cnLocal, "SELECT * FROM [bi].[AllDocuments] WHERE [ID]>411957", cnRemote, DbObject.Parse("dbo.AllDocuments"), 75).Wait();
                }
            }
        }
    }
}
