namespace SqlIntegration.Library.Classes
{
    public class BulkInsertProgress
    {
        public int TotalRows { get; set; }
        public int RowsCompleted { get; set; }

        public int PercentComplete()
        {
            return (int)((RowsCompleted / (double)TotalRows) * 100);
        }
    }
}
