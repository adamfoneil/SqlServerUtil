using System;

namespace SqlIntegration.Library.Interfaces
{
    /// <summary>
    /// describes a row that can be marked invalid
    /// </summary>
    public interface ICacheRow
    {
        /// <summary>
        /// if true, data is considered out of date and needs refreshing
        /// </summary>
        bool IsInvalid { get; }

        /// <summary>
        /// when was this row last updated? UTC time
        /// </summary>
        DateTime? LastRefreshed { get; }
    }
}
