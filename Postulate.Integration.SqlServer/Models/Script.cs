namespace Postulate.Integration.SqlServer.Models
{
    public class Script
    {
        //public Migration[] 
    }

    public class Migration
    {
        public string SourceConnection { get; set; }
        public string SourceObject { get; set; }

        public string DestConnection { get; set; }
        public string DestObject { get; set; }
        public bool TruncateFirst { get; set; }
    }
}
