using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace SqlIntegration.Library.Extensions
{
    public static class IEnumerableExtensions
    {
        /// <summary>
        /// adapted from https://www.codeproject.com/Articles/835519/Passing-Table-Valued-Parameters-with-Dapper
        /// </summary>
        public static DataTable ToDataTable<T>(this IEnumerable<T> enumerable)
        {
            DataTable dataTable = new DataTable();

            if (typeof(T).IsValueType || typeof(T).FullName.Equals("System.String"))
            {
                dataTable.Columns.Add("ValueType", typeof(T));
                foreach (T obj in enumerable) dataTable.Rows.Add(obj);
            }
            else
            {
                var properties = typeof(T)
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(pi => pi.CanRead)
                    .ToDictionary(item => item.Name);

                foreach (string name in properties.Keys) dataTable.Columns.Add(name, properties[name].PropertyType);

                foreach (T obj in enumerable)
                {
                    dataTable.Rows.Add(properties.Select(kp => kp.Value.GetValue(obj)).ToArray());
                }
            }

            return dataTable;
        }
    }
}
