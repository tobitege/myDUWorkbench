// Helper Index:
// - SanitizeGeneratedFileName: Produces ASCII-only generated file names that avoid PowerShell wildcard characters.
namespace myDUWorkbench.Helpers;

using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

public static class FileNameHelper
{
    private static readonly Regex InvalidRunRegex = new("[^A-Za-z0-9._-]+", RegexOptions.Compiled);
    private static readonly Regex SeparatorRunRegex = new("[_.-]{2,}", RegexOptions.Compiled);

    public static string SanitizeGeneratedFileName(string? value, string fallback = "file")
    {
        string safeFallback = string.IsNullOrWhiteSpace(fallback) ? "file" : fallback.Trim();
        if (string.IsNullOrWhiteSpace(value))
        {
            return safeFallback;
        }

        string normalized = value.Normalize(NormalizationForm.FormKD);
        var ascii = new StringBuilder(normalized.Length);
        foreach (char c in normalized)
        {
            UnicodeCategory category = CharUnicodeInfo.GetUnicodeCategory(c);
            if (category is UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark or UnicodeCategory.EnclosingMark)
            {
                continue;
            }

            ascii.Append(c <= sbyte.MaxValue ? c : '_');
        }

        string sanitized = InvalidRunRegex.Replace(ascii.ToString(), "_");
        sanitized = SeparatorRunRegex.Replace(sanitized, "_");
        sanitized = sanitized.Trim(' ', '.', '_', '-');

        if (string.IsNullOrWhiteSpace(sanitized))
        {
            sanitized = safeFallback;
        }

        if (IsReservedWindowsDeviceName(sanitized))
        {
            sanitized += "_file";
        }

        return sanitized;
    }

    private static bool IsReservedWindowsDeviceName(string value)
    {
        return value.Equals("CON", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("AUX", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("NUL", StringComparison.OrdinalIgnoreCase) ||
               Regex.IsMatch(value, @"^(COM|LPT)[1-9]$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }
}
