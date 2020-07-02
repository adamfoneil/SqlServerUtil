using System.Collections.Generic;

namespace SqlIntegration.Library.Internal
{
    internal class MultiValueInsert
    {
        public string Sql { get; set; }
        public int RowsInserted { get; set; }
        public int StartRow { get; set; }
        public string InsertStatement { get; set; }
        public IEnumerable<string> Values { get; set; }
    }
}
