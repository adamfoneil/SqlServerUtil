using AdamOneilSoftware;
using Dapper;
using DataTables.Library;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlIntegration.Library.Classes;
using SqlServer.LocalDb;
using SqlServer.LocalDb.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Testing.Classes;
using Testing.Models;

namespace Testing
{
    [TestClass]
    public class ViewMaterializerTests
    {
        [TestMethod]
        public void ViewMaterializerBasics()
        {
            using (var cn = LocalDb.GetConnection("ViewMaterializer", CreateObjects()))
            {
                try { cn.Execute("DROP TABLE [vm].[SyncVersion]"); } catch { /* do nothing */ }
                DisableChangeTracking(cn);
                CreateRandomSalesData(cn);
                EnableChangeTracking(cn);

                // make sure we're starting with an empty reporting table
                try { cn.Execute("TRUNCATE TABLE [rpt].[SalesHistoryTotals]"); } catch { };

                // simulate a random-looking update of base table data
                cn.Execute("UPDATE [SalesHistory] SET [Quantity]=[Quantity]+1 WHERE [Id] % 113 = 0");

                var vm = new SalesMaterializer();

                // this will ensure that the whole view is merged to the reporting table.
                // you wouldn't do this in a real app because it would defeat the optimization
                vm.ClearVersionAsync(cn).Wait();

                vm.ExecuteAsync(cn).Wait();

                // simulate another random-looking update
                cn.Execute("UPDATE [SalesHistory] SET [Quantity]=[Quantity]+1 WHERE [Id] % 209 = 0");

                vm.ExecuteAsync(cn).Wait();

                // after a series of updates, the view data should be the same as the output table
                Assert.IsTrue(vm.SourceViewEqualsResultTable(cn).Result);
            }
        }

        private void DisableChangeTracking(SqlConnection cn)
        {
            try
            {
                cn.Execute(@"ALTER DATABASE [ViewMaterializer] SET CHANGE_TRACKING = OFF");
            }
            catch 
            {
                // do nothing
            }
        }

        private static void EnableChangeTracking(SqlConnection cn)
        {
            try
            {
                cn.Execute(@"ALTER DATABASE [ViewMaterializer] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)");
                cn.Execute(@"ALTER TABLE [dbo].[SalesHistory] ENABLE CHANGE_TRACKING");
            }
            catch
            {
                // do nothing, change tracking already on
            }
        }

        private IEnumerable<InitializeStatement> CreateObjects()
        {
            yield return new InitializeStatement(
                "dbo.SalesHistory", 
                "DROP TABLE %obj%",
                @"CREATE TABLE %obj% (
                    [Customer] nvarchar(100) NOT NULL,
                    [Region] nvarchar(50) NOT NULL,
                    [Date] date NOT NULL,
                    [ItemNumber] nvarchar(50) NOT NULL,
                    [Quantity] int NOT NULL,
                    [Price] money NOT NULL,
                    [Id] int identity(1,1) PRIMARY KEY
                )");

            yield return new InitializeStatement(
                "dbo.SalesHistoryTotals",
                "DROP VIEW %obj%",
                @"CREATE VIEW [dbo].[SalesHistoryTotals]
                AS
                SELECT
	                [Region], 
	                YEAR([Date]) AS [Year], 	
	                UPPER(LEFT([ItemNumber], 2)) AS [ItemClass], SUM([Quantity]) AS [TotalQuantity],
	                SUM([Quantity]*[Price]) AS [TotalRevenue]
                FROM
	                [dbo].[SalesHistory]
                GROUP BY
	                [Region],
	                YEAR([Date]),	
	                UPPER(LEFT([ItemNumber], 2))");
        }

        private void CreateRandomSalesData(SqlConnection cn)
        {
            string[] regions = new string[]
            {
                "North", "South", "East", "West"
            };

            var tdg = new TestDataGenerator() { BatchSize = 500 };
            tdg.Generate<SalesHistory>(10000, (row) =>
            {
                row.Customer = tdg.Random(Source.FirstName) + " " + tdg.Random(Source.LastName);                
                row.Region = tdg.Random(regions);
                row.Date = tdg.RandomInRange(0, 3000, i => DateTime.Today.AddDays(i * -1));
                row.ItemNumber = tdg.Random(Source.WidgetName) + tdg.RandomInRange(0, 100).ToString();
                row.Quantity = tdg.RandomInRange(1, 100).Value;
                row.Price = tdg.RandomInRange<decimal>(1, 100, (i) => i * 0.25m);
            }, (rows) =>
            {
                var dataTable = rows.ToDataTable();
                BulkInsert.ExecuteAsync(dataTable, cn, "dbo.SalesHistory", 500, new BulkInsertOptions()
                {
                    SkipIdentityColumn = "Id"
                }).Wait();
            });
        }
    }
}
