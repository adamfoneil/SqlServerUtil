using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlServer.LocalDb;

namespace Testing
{
    [TestClass]
    public class SqlMigratorTest
    {
        [TestMethod]
        public void InitializeMigrator()
        {
            LocalDb.TryDropDatabase("SqlMigrator", out _);

            using (var cn = LocalDb.GetConnection("SqlMigrator"))
            {
                var migrator = SqlMigrator<int>.StartJobAsync(cn).Result;
                Assert.IsTrue(migrator.JobId == 1);
            }
        }
    }
}
