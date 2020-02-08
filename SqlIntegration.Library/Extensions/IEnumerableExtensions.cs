using System.Collections.Generic;
using System.Linq;

namespace SqlIntegration.Library.Extensions
{
    public static class IEnumerableExtensions
    {
        /// <summary>
        /// divides an enumerable into a number of smaller enumerables of count chunkSize
        /// </summary>
        public static IEnumerable<IEnumerable<T>> ToSegments<T>(this IEnumerable<T> items, int segmentCount)
        {
            int skip = 0;
            do
            {
                var results = items.Skip(skip).Take(segmentCount);
                if (!results.Any()) break;
                yield return results;
                skip += segmentCount;
            } while (true);           
        }
    }
}
