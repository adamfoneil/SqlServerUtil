using System.Text.RegularExpressions;

namespace Testing.Extensions
{
    public static class StringExtensions
    {
        public static string ReplaceWhitespace(this string input)
        {
            return Regex.Replace(input, @"\s+", " ").Trim();
        }
    }
}
