namespace SqlIntegration.Library.Classes
{
    public class BulkInsertProgress
    {
        public int TotalRows { get; set; }
        public int RowsCompleted { get; set; }

        public decimal PercentComplete()
        {
            return RowsCompleted / TotalRows;
        }
    }
}
