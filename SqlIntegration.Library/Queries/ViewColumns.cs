using Dapper.QX;

namespace SqlIntegration.Library.Queries
{
    public class ViewColumnsResult
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public int Size { get; set; }
        public bool AllowNull { get; set; }

        public string GetSyntax(bool forceNotNull = false)
        {
            string sizeText = (Size == -1) ? "max" : Size.ToString();
            string typeName = (Size != 0) ? $"{DataType}({sizeText})" : DataType;
            
            string nullable = 
                (forceNotNull) ? "NOT NULL" :
                (AllowNull) ? "NULL" : 
                "NOT NULL";

            return $"[{Name}] {typeName} {nullable}";
        }
    }

    public class ViewColumns : Query<ViewColumnsResult>
    {
        public ViewColumns() : base(
            @"SELECT 
                [col].[name] AS [Name], 
                TYPE_NAME([col].[system_type_id]) AS [DataType], 
                CASE 
                    WHEN TYPE_NAME([col].[system_type_id]) LIKE 'nvar%' THEN [col].[max_length]/2
                    WHEN TYPE_NAME([col].[system_type_id]) LIKE 'var%' THEN [col].[max_length]
                    ELSE 0
                END AS [Size], 
                [col].[is_nullable] AS [AllowNull]
            FROM
                [sys].[columns] [col]
                INNER JOIN [sys].[views] [v] ON [col].[object_id]=[v].[object_id]
            WHERE
                SCHEMA_NAME([v].[schema_id])=@schemaName AND
	            [v].[name]=@viewName")
        {
        }

        public string SchemaName { get; set; }
        public string ViewName { get; set; }
    }
}
