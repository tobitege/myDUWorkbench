using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace myDUWorker.Helpers;

internal static class LuaSectionComponentOrder
{
    public static string NormalizeComponentKey(string component)
    {
        string normalized = (component ?? string.Empty).Trim().ToLowerInvariant();
        normalized = Regex.Replace(normalized, "\\s+", " ");
        return string.IsNullOrWhiteSpace(normalized) ? "misc" : normalized;
    }

    public static IReadOnlyList<string> OrderKeys(IEnumerable<string> componentKeys)
    {
        return componentKeys
            .Distinct(StringComparer.Ordinal)
            .OrderBy(GetComponentSortRank)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static int GetComponentSortRank(string key)
    {
        return key switch
        {
            "library" => 0,
            "system" => 1,
            "player" => 2,
            "construct" => 3,
            "unit" => 4,
            _ => GetSlotSortRank(key)
        };
    }

    private static int GetSlotSortRank(string key)
    {
        if (!key.StartsWith("slot", StringComparison.OrdinalIgnoreCase))
        {
            return 1000;
        }

        ReadOnlySpan<char> suffix = key.AsSpan(4).Trim();
        if (!int.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out int slotNumber) ||
            slotNumber <= 0)
        {
            return 1000;
        }

        return 100 + slotNumber;
    }
}
