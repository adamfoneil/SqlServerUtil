using Dapper;
using DataTables.Library;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlIntegration.Library.Classes;
using SqlServer.LocalDb;
using System;
using System.IO;

namespace Testing
{
    [TestClass]
    public class BulkInsertTests
    {
        [TestMethod]
        public void GetSqlStatementsIdentityOff()
        {
            using (var cn = LocalDb.GetConnection("ViewMaterializer", ViewMaterializerTests.CreateObjects()))
            {
                ViewMaterializerTests.CreateRandomSalesData(cn, 20);

                var data = cn.QueryTable("SELECT TOP (20) * FROM [dbo].[SalesHistory]");
                var result = BulkInsert.GetSqlStatementsAsync("dbo.SalesHistory", data, new BulkInsertOptions()
                {
                    SkipIdentityColumn = "Id"
                }).Result;

                cn.Execute("TRUNCATE TABLE [dbo].[SalesHistory]");
                cn.Execute(result.ToString());
            }
        }

        [TestMethod]
        public void GetSqlStatementsIdentityOn()
        {
            using (var cn = LocalDb.GetConnection("ViewMaterializer", ViewMaterializerTests.CreateObjects()))
            {
                ViewMaterializerTests.CreateRandomSalesData(cn, 20);

                var data = cn.QueryTable("SELECT TOP (20) * FROM [dbo].[SalesHistory]");
                var result = BulkInsert.GetSqlStatementsAsync("dbo.SalesHistory", data, new BulkInsertOptions()
                {
                    IdentityInsert = true
                }).Result;

                //string outputFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "output.txt");
                //File.WriteAllText(outputFile, result.ToString());

                cn.Execute("TRUNCATE TABLE [dbo].[SalesHistory]");
                cn.Execute(result.ToString());
            }
        }

    }
}
