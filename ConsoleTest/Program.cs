using Dapper;
using Microsoft.Extensions.Configuration;
using Postulate.Integration.SqlServer;
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

                    var vm = new Migrator();
                    //vm.MergeRowsAsync<int>(cnLocal, "bi.AllDocuments", cnRemote, "dbo.AllDocuments", "ID", new string[] { "LibraryId", "Filename" }, true).Wait();

                    vm.MergeAsync(cnLocal, "bi.AllDocuments", cnRemote, "dbo.AllDocuments", true).Wait();
                }
            }
        }
    }
}
