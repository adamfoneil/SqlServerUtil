using AdoUtil;
using Dapper;
using Excel2SqlServer.Library;
using JsonSettings;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace Testing
{
    [TestClass]
    public class SqlServerValidation
    {
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
