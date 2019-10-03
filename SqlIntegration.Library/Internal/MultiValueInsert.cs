namespace Postulate.Integration.SqlServer.Internal
{
    internal class MultiValueInsert
    {
        public string Sql { get; set; }
        public int RowsInserted { get; set; }
        public int StartRow { get; set; }
    }
}
