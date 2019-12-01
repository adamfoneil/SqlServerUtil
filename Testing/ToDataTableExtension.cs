using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library.Extensions;
using System;
using System.Data;
using System.Linq;

namespace Testing
{
    [TestClass]
    public class ToDataTableExtension
    {
        [TestMethod]
        public void EnumerableToDataTableStringOnly()
        {
            string[] sample = new string[]
            {
                "this",
                "that",
                "other"
            };

            var dataTable = IEnumerableExtensions.ToDataTable(sample);

            Assert.IsTrue(dataTable.Columns[0].ColumnName.Equals("ValueType"));
            Assert.IsTrue(dataTable.Rows.Count == 3);
            Assert.IsTrue(dataTable.Rows[0][0].Equals("this"));
            Assert.IsTrue(dataTable.Rows[1][0].Equals("that"));
            Assert.IsTrue(dataTable.Rows[2][0].Equals("other"));
        }

        [TestMethod]
        public void EnumerableToDataTableComplexType()
        {
            var items = new[]
            {
                new { Message = "hello", Date = DateTime.Today, Value = 34 },
                new { Message = "goodbye", Date = DateTime.Today.AddDays(-20), Value = 56 },
                new { Message = "whatever", Date = DateTime.Today.AddDays(4), Value = 198 }
            };

            var dataTable = IEnumerableExtensions.ToDataTable(items);

            Assert.IsTrue(
                dataTable.Columns.OfType<DataColumn>().Select(col => col.ColumnName)
                .SequenceEqual(new string[] { "Message", "Date", "Value" }));

            Assert.IsTrue(dataTable.Rows.Count == 3);

            Assert.IsTrue(dataTable.Rows[1]["Message"].Equals("goodbye"));
            Assert.IsTrue(dataTable.Rows[2]["Value"].Equals(198));
            Assert.IsTrue(dataTable.Rows[0]["Date"].Equals(DateTime.Today));
        }
    }
}
