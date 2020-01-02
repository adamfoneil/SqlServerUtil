using Dapper.QX;

namespace SqlIntegration.Library.Queries
{
    public class ChangeTrackingQuery<TKeyColumns> : Query<TKeyColumns>
    {
        public ChangeTrackingQuery(string sql) : base(sql)
        {
        }

        public long Version { get; set; }
    }
}
