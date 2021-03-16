using Dapper;
using DataTables.Library;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlIntegration.Library.Classes;
using SqlServer.LocalDb;
using System;
using System.IO;
using System.Linq;
using Testing.Extensions;

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

        [TestMethod]
        public void GetSqlStatementsFromEnumerable()
        {
            var data = new SampleThing[]
            {
                new SampleThing() { FirstName = "Whoever", LastName = "Nobody", DateValue = new DateTime(2010, 1, 1) },
                new SampleThing() { FirstName = "Someone", LastName = "Indeed", DateValue = new DateTime(2011, 2, 2) },
                new SampleThing() { FirstName = "Yabba", LastName = "Dabba", DateValue = new DateTime(2012, 3, 3) },
                new SampleThing() { FirstName = "Captain", LastName = "Kirk", DateValue = new DateTime(2013, 4, 4) },
                new SampleThing() { FirstName = "Zorba", LastName = "Hurston", DateValue = new DateTime(2014, 5, 5) },
                new SampleThing() { FirstName = "Yitzak", LastName = "Rabin", DateValue = new DateTime(2015, 6, 6) }
            };

            var sqlCommands = BulkInsert.GetMultiValueInserts(
                DbObject.Parse("dbo.Whatever"), data, 3, 
                s => s.FirstName,
                s => s.LastName,
                s => s.DateValue).Select(mvi => mvi.Sql).ToArray();

            Assert.IsTrue(sqlCommands[0].ReplaceWhitespace().Equals(
                @"INSERT INTO [dbo].[Whatever] (
                    [FirstName], [LastName], [DateValue]
                ) VALUES 
                    ('Whoever', 'Nobody', '1/1/2010 12:00:00 AM') , 
                    ('Someone', 'Indeed', '2/2/2011 12:00:00 AM') , 
                    ('Yabba', 'Dabba', '3/3/2012 12:00:00 AM')".ReplaceWhitespace()));

            Assert.IsTrue(sqlCommands[1].ReplaceWhitespace().Equals(
                @"INSERT INTO [dbo].[Whatever] (
                    [FirstName], [LastName], [DateValue]
                ) VALUES 
                    ('Captain', 'Kirk', '4/4/2013 12:00:00 AM') , 
                    ('Zorba', 'Hurston', '5/5/2014 12:00:00 AM') , 
                    ('Yitzak', 'Rabin', '6/6/2015 12:00:00 AM')".ReplaceWhitespace()));
        }
    }

    public class SampleThing
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public DateTime DateValue { get; set; }
    }
}
