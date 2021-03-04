using System;
using System.Data;

namespace SqlIntegration.Library.Exceptions
{
    public class InsertException : Exception
    {
        public InsertException(DataRow dataRow, Exception innerException) : base(innerException.Message, innerException)
        {
            DataRow = dataRow;
        }

        public DataRow DataRow { get; }
    }
}
