using AdoUtil;
using Dapper;
using Excel2SqlServer.Library;
using JsonSettings;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Postulate.Base.Extensions;
using Postulate.Integration.SqlServer;
using Postulate.SqlServer;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;

namespace Testing
{
    [TestClass]
    public class SqlServerValidation
    {
        [TestMethod]
        public void FindInvalidDates()
        {
            var userData = new SampleData[]
            {
                new SampleData() { RowNumber = 1, DateValue = "4/3/19" },
                new SampleData() { RowNumber = 2, DateValue = "5/1/17" },
                new SampleData() { RowNumber = 3, DateValue = "6/12/13" },
                new SampleData() { RowNumber = 4, DateValue = "9/33/12" }, // should fail
                new SampleData() { RowNumber = 5, DateValue = "7/1/11" },
                new SampleData() { RowNumber = 6, DateValue = "13/0" } // should fail
            }.ToDataTable(new SqlServerIntegrator());

            // in a real app, you would've done a query to get the dataTable of results,
            // but for test purposes, we can conjure one out of thin air with Postulate ToDataTable

            var results = Validation.ValidateColumnTypes(userData, "RowNumber",
                new TypeValidator("DateValue", typeof(DateTime)));

            Assert.IsTrue(results.Count() == 2);

            Assert.IsTrue(results.First().OffendingValue.Equals("9/33/12"));
            Assert.IsTrue(results.First().ReportValue.Equals("4"));
            Assert.IsTrue(results.Last().OffendingValue.Equals("13/0"));
            Assert.IsTrue(results.Last().ReportValue.Equals("6"));
        }

        [TestMethod]
        public void FindInvalidDatesInSpreadsheet()
        {
            // this requires a very specific spreadsheet that I don't want in source control.
            // be sure to rename ID column to ID2 or some such or the loader fails
            var fileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "ExcelImport\\LargeSample.xlsx");

            using (var cn = new SqlConnection("Server=(localdb)\\mssqllocaldb;Database=master;Integrated Security=true"))
            {
                try
                {
                    cn.Execute("CREATE DATABASE [ExcelImport]");
                }
                catch
                {
                    // do nothing, database already exists
                }

                try
                {
                    cn.Execute("DROP TABLE [dbo].[Sample]");
                }
                catch 
                {
                    // do nothing, table doesn't exist                    
                }
                                        
                using (var cnExcel = new SqlConnection("Server=(localdb)\\mssqllocaldb;Database=ExcelImport;Integrated Security=true"))
                {
                    var loader = new ExcelLoader();
                    loader.Save(fileName, cnExcel, "dbo", "Sample", truncateFirst: true);

                    var data = cnExcel.QueryTable("SELECT * FROM [dbo].[Sample] WHERE [Date] IS NOT NULL");

                    // note that sample file had a column named "Files Name"
                    //var results = Validation.ValidateColumnTypes(data, "Files Name", new TypeValidator("Date", typeof(DateTime)));

                    var results = Validation.ValidateSqlServerTypeConversionAsync<int>(cnExcel,
                        "dbo", "Sample", "Id", "Date", "datetime", "[Date] IS NOT NULL").Result;

                    string outputFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "ExcelImport\\Results.json");
                    JsonFile.Save(outputFile, results);

                    Assert.IsTrue(results.Count() == 7);
                }
            }
        }
    }    

    public class SampleData
    {
        public int RowNumber { get; set; }
        public string DateValue { get; set; }
    }
}
