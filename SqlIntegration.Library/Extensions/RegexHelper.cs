using System.Text.RegularExpressions;

namespace SqlIntegration.Library.Extensions
{
    public static class RegexHelper
    {
        /// <summary>
        /// help from https://stackoverflow.com/a/628563/2023653
        /// </summary>
        public static (bool isMatch, string quotedItem) ParseQuotedItem(string message, string cue)
        {
            const string quotedPattern = " \"(?<item>([^\"]+))\"";
            var match = Regex.Match(message, cue + quotedPattern);
            if (match.Success)
            {
                return (true, match.Groups["item"].Value);
            }
            else
            {
                return (false, null);
            }
        }
    }
}
