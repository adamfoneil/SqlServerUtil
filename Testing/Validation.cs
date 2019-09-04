using Microsoft.VisualStudio.TestTools.UnitTesting;
using Postulate.Base.Extensions;
using Postulate.Integration.SqlServer;
using Postulate.SqlServer;
using System;
using System.Linq;

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
    }

    public class SampleData
    {
        public int RowNumber { get; set; }
        public string DateValue { get; set; }
    }
}
