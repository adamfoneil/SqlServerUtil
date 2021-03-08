using System;
using System.Text.RegularExpressions;

namespace SqlIntegration.Library.Extensions
{
    public static class RegexHelper
    {
        public enum QuoteType
        {
            Single,
            Double
        }

        /// <summary>
        /// help from https://stackoverflow.com/a/628563/2023653
        /// </summary>
        public static (bool isMatch, string quotedItem) ParseQuotedItem(string message, string cue, QuoteType quoteType)
        {
            string quotedPattern =                 
               (quoteType == QuoteType.Double) ? " \"(?<item>([^\"]+))\"" :
               (quoteType == QuoteType.Single) ? " '(?<item>([^']+))'" :
               throw new ArgumentException($"Unknown quote type: {quoteType}");

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
