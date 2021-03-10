[![Nuget](https://img.shields.io/nuget/v/SqlServer.Util.Library)](https://www.nuget.org/packages/SqlServer.Util.Library/)

This project brings together several utility classes to do some nifty things with SQL Server.

Nuget package: **SqlServer.Util.Library**

- **BulkInsert** makes it easy to copy lots of rows within or across connections.\
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-BulkInsert) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/BulkInsert.cs)

- **SqlMigrator** provides cloning or "deep copy" functionality.\
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-SqlMigrator) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/SqlMigrator.cs) | [test](https://github.com/adamosoftware/SqlServerUtil/blob/master/Testing/SqlMigratorTest.cs#L42)

My [Zinger](https://github.com/adamfoneil/Postulate.Zinger) project uses this with its [Data Migrator feature](https://github.com/adamfoneil/Postulate.Zinger/wiki/Data-Migrator).

- **ViewMaterializer** provides a way to optimize slow views by leveraging SQL Server change tracking.\
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-ViewMaterializer) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/ViewMaterializer.cs) | [test](https://github.com/adamosoftware/SqlServerUtil/blob/master/Testing/ViewMaterializerTests.cs#L21)
