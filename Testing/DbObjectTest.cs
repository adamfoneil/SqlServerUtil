using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;

namespace Testing
{
    [TestClass]
    public class DbObjectTest
    {
        [TestMethod]
        public void DbObjectParse()
        {
            var obj = DbObject.Parse("Hello");
            Assert.IsTrue(obj.Equals(new DbObject("dbo", "Hello")));
        }

        [TestMethod]
        public void DbObjectDelimited()
        {
            string name = DbObject.Delimited("Hello");
            Assert.IsTrue(name.Equals("[dbo].[Hello]"));
        }

        [TestMethod]
        public void DbObjectParseWithSchema()
        {
            var obj = DbObject.Parse("xyz.Filibuster");
            Assert.IsTrue(obj.Equals(new DbObject("xyz", "Filibuster")));
        }

        [TestMethod]
        public void DbObjectDelimitedWithSchema()
        {
            var name = DbObject.Delimited("xyz.Filibuster");
            Assert.IsTrue(name.Equals("[xyz].[Filibuster]"));
        }
    }
}
