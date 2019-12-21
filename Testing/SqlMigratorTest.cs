using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlServer.LocalDb;
using SqlIntegration.Library.Extensions;

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
                var migrator = SqlMigrator<int>.InitializeAsync(cn).Result;
                Assert.IsTrue(cn.TableExistsAsync(migrator.KeyMapTable).Result);
            }
        }
    }
}
