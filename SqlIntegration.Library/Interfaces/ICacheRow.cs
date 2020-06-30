using System;

namespace SqlIntegration.Library.Interfaces
{
    /// <summary>
    /// implement on query result classes so that CacheManager can tell which rows are out of date (IsValid == false)
    /// </summary>
    public interface ICacheRow
    {
        bool IsValid { get; set; }
        DateTime Timestamp { get; }
    }
}
