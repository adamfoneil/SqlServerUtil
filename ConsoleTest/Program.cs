using Microsoft.Extensions.Configuration;
using SqlIntegration.Library;
using SqlIntegration.Library.Classes;
using System;
using Microsoft.Data.SqlClient;
using System.IO;

namespace ConsoleTest
{
    class Program
    {
        static void Main1()
        {
            int done = 24;
            int total = 217;
            int percentDone = (int)(((double)done / (double)total) * 100);
            Console.WriteLine($"percent done = {percentDone}");
        }

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

                    //BulkInsert.ExecuteAsync(cnLocal, "dbo.DocumentField", cnRemote, "dbo.DocumentField", 100).Wait();

                    /*BulkInsert.OffsetExecuteAsync(cnLocal, DbObject.Parse("dbo.DocumentField"), "[ID]", 10000, cnRemote, DbObject.Parse("dbo.DocumentField"), 75, new BulkInsertOptions()
                    {
                        TruncateFirst = true,
                        Progress = new Progress<BulkInsertProgress>(WriteProgress)
                    }).Wait();*/

                    /*BulkInsert.ExecuteAsync(cnLocal, "SELECT TOP (10000) * FROM dbo.DocumentField", cnRemote, DbObject.Parse("dbo.DocumentField"), 75, new SqlIntegration.Library.Classes.BulkInsertOptions()
                    {
                        TruncateFirst = true,
                        Progress = new Progress<BulkInsertProgress>(WriteProgress)
                    }).Wait();*/

                    BulkInsert.ChunkExecuteAsync(cnLocal, DbObject.Parse("bi.AllDocuments"), "ID", 5, cnRemote, DbObject.Parse("dbo.AllDocuments"), 150, new BulkInsertOptions()
                    {
                        TruncateFirst = true,
                        DisableIndexes = true,
                        CommandTimeout = 200,
                        Progress = new Progress<RowOperationProgress>(WriteProgress)
                    }).Wait();

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

        private static void WriteProgress(RowOperationProgress obj)
        {
            int percentComplete = obj.PercentComplete();
            if (percentComplete % 5 == 0)
            {
                Console.WriteLine($"{percentComplete} percent done, page {obj.CurrentOffset}, rows {obj.RowsCompleted}");
            }
            
        }
    }
}
