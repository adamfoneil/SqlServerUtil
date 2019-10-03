namespace SqlIntegration.Library.Classes
{
    public class BulkInsertProgress
    {
        public int CurrentOffset { get; set; }
        public int TotalRows { get; set; }
        public int RowsCompleted { get; set; }

        public int PercentComplete()
        {
            if (RowsCompleted > TotalRows) return 100;
            return (int)((RowsCompleted / (double)TotalRows) * 100);
        }
    }
}
