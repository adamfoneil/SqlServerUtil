This project brings together several utility classes to do some nifty things with SQL Server.

Nuget package: **SqlServer.Util.Library**

- **BulkInsert** makes it easy to copy rows lots of rows within or across connections.
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-BulkInsert) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/BulkInsert.cs)

- **SqlMigrator** provides cloning or "deep copy" functionality.
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-SqlMigrator) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/SqlMigrator.cs) | [test](https://github.com/adamosoftware/SqlServerUtil/blob/master/Testing/SqlMigratorTest.cs#L42)

- **ViewMaterializer** provides a way to optimize slow views by leveraging SQL Server change tracking.
[walkthrough](https://github.com/adamosoftware/SqlIntegration/wiki/Using-ViewMaterializer) | [source](https://github.com/adamosoftware/SqlIntegration/blob/master/SqlIntegration.Library/ViewMaterializer.cs) | [test](https://github.com/adamosoftware/SqlServerUtil/blob/master/Testing/ViewMaterializerTests.cs#L21)
