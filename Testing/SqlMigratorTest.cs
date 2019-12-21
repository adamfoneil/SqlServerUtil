using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlIntegration.Library;
using SqlServer.LocalDb;
using SqlIntegration.Library.Extensions;
using SqlServer.LocalDb.Models;
using System.Collections.Generic;
using System;
using AdamOneilSoftware;
using Testing.Models;
using Dapper;
using System.Linq;
using SqlIntegration.Library.Classes;
using System.Data.SqlClient;
using Dapper.CX.Classes;
using Dapper.CX.Extensions;

namespace Testing
{
    [TestClass]
    public class SqlMigratorTest
    {
        private const string dbName = "SqlMigrator";

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            LocalDb.TryDropDatabase(dbName, out _);
        }

        [TestMethod]
        public void InitializeMigrator()
        {           
            using (var cn = LocalDb.GetConnection(dbName))
            {
                var migrator = SqlMigrator<int>.InitializeAsync(cn).Result;
                Assert.IsTrue(cn.TableExistsAsync(migrator.KeyMapTable).Result);
            }
        }

        [TestMethod]
        public void CopyRowsSelf()
        {
            using (var cn = LocalDb.GetConnection(dbName, SampleModel()))
            {
                CreateRandomData(cn);
                
                var migrator = SqlMigrator<int>.InitializeAsync(cn).Result;

                var param = new { id = 3 };

                // copy one parent (arbitrary Id) with a new name that appends the word " - copy"
                migrator.CopyRowsSelfAsync(cn, "dbo", "Parent", "Id", "[Id]=@id", param, 
                    setColumns: new Dictionary<string, object>()
                {
                    { "Name", new SqlExpression("[Name] + ' - copy'") }
                }).Wait();

                // copy the child rows
                migrator.CopyRowsSelfAsync(cn, "dbo", "Child", "Id", "[ParentId]=@id", param, 
                    mapForeignKeys: new Dictionary<string, string>()
                {
                    { "ParentId", "dbo.Parent" }
                }).Wait();                
            }
        }

        private static void CreateRandomData(SqlConnection cn)
        {
            int suffix = 0;
            var tdg = new TestDataGenerator();
            tdg.Generate<Parent>(10, (p) =>
            {
                suffix++;
                p.Name = tdg.Random(Source.WidgetName) + "." + suffix.ToString();
            }, (rows) =>
            {
                var data = rows.ToDataTable();
                BulkInsert.ExecuteAsync(data, cn, "dbo.Parent", 100, new BulkInsertOptions()
                {
                    SkipIdentityColumn = "Id"
                }).Wait();
            });

            int[] parentIds = cn.Query<int>("SELECT [Id] FROM [dbo].[Parent]").ToArray();

            tdg.Generate<Child>(100, (c) =>
            {
                c.ParentId = tdg.Random(parentIds);
                suffix++;
                c.Name = tdg.Random(Source.WidgetName) + "." + suffix.ToString();
            }, (rows) =>
            {
                var data = rows.ToDataTable();
                BulkInsert.ExecuteAsync(data, cn, "dbo.Child", 25, new BulkInsertOptions()
                {
                    SkipIdentityColumn = "Id"
                }).Wait();
            });
        }

        private IEnumerable<InitializeStatement> SampleModel()
        {
            yield return new InitializeStatement(
                "dbo.Parent",
                "DROP TABLE %obj%",
                @"CREATE TABLE %obj% (
                    [Name] nvarchar(50) NOT NULL,
                    [Id] int identity(1,1) PRIMARY KEY
                )");

            // we don't need a real FK for this model
            yield return new InitializeStatement(
                "dbo.Child",
                "DROP TABLE %obj%",
                @"CREATE TABLE %obj% (
                    [ParentId] int NOT NULL,
                    [Name] nvarchar(50),                    
                    [Id] int identity(1,1) PRIMARY KEY,
                    CONSTRAINT [U_Child_Parent] UNIQUE ([ParentId], [Name])
                )");
        }
    }
}
