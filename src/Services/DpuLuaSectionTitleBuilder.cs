using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace myDUWorker.Services;

internal static class DpuLuaSectionTitleBuilder
{
    private static readonly Regex SafeNameRegex = new("[^A-Za-z0-9._-]+", RegexOptions.Compiled);
    private static readonly Dictionary<string, string> BuiltInSlotNames = new(StringComparer.Ordinal)
    {
        ["-1"] = "unit",
        ["-2"] = "construct",
        ["-3"] = "player",
        ["-4"] = "system",
        ["-5"] = "library"
    };

    public static string BuildHandlerTitle(
        int index,
        string? key,
        string? slotKey,
        string? signature,
        IReadOnlyList<string> filterArgs,
        IReadOnlyDictionary<string, string> slotNameByKey)
    {
        string component = ResolveComponentName(slotKey, slotNameByKey);
        string eventLabel = BuildHandlerEventLabel(index, key, signature, filterArgs);
        return $"{component} / {eventLabel}";
    }

    public static string BuildMethodTitle(int index, string? methodName)
    {
        string name = SafeName(methodName ?? string.Empty);
        return name.Length == 0
            ? $"method_{index.ToString(CultureInfo.InvariantCulture)}"
            : $"method_{name}";
    }

    public static string BuildEventTitle(int index, string? eventName)
    {
        string name = SafeName(eventName ?? string.Empty);
        return name.Length == 0
            ? $"event_{index.ToString(CultureInfo.InvariantCulture)}"
            : $"event_{name}";
    }

    private static string BuildHandlerEventLabel(
        int index,
        string? key,
        string? signature,
        IReadOnlyList<string> filterArgs)
    {
        (string eventName, IReadOnlyList<string> signatureArgs) = ParseSignature(signature);
        if (string.IsNullOrWhiteSpace(eventName))
        {
            string keyPart = SafeName(key ?? string.Empty);
            eventName = keyPart.Length == 0
                ? $"handler_{index.ToString(CultureInfo.InvariantCulture)}"
                : $"handler_{keyPart}";
        }

        List<string> resolvedArgs = ResolveArgs(signatureArgs, filterArgs);
        return $"{eventName}[{string.Join(",", resolvedArgs)}]";
    }

    private static List<string> ResolveArgs(IReadOnlyList<string> signatureArgs, IReadOnlyList<string> filterArgs)
    {
        if (signatureArgs.Count == 0 && filterArgs.Count == 0)
        {
            return new List<string>();
        }

        int count = Math.Max(signatureArgs.Count, filterArgs.Count);
        var resolved = new List<string>(count);
        for (int i = 0; i < count; i++)
        {
            string signatureArg = i < signatureArgs.Count ? signatureArgs[i] : string.Empty;
            string filterArg = i < filterArgs.Count ? filterArgs[i] : string.Empty;
            bool useFilterArg = !string.IsNullOrWhiteSpace(filterArg) &&
                                !string.Equals(filterArg, "*", StringComparison.Ordinal);
            string chosen = useFilterArg ? filterArg : signatureArg;
            if (string.IsNullOrWhiteSpace(chosen))
            {
                chosen = useFilterArg ? filterArg : "*";
            }

            resolved.Add(chosen.Trim());
        }

        return resolved;
    }

    private static (string EventName, IReadOnlyList<string> Args) ParseSignature(string? signature)
    {
        string normalized = (signature ?? string.Empty).Trim().Replace("\r", string.Empty).Replace("\n", string.Empty);
        if (normalized.Length == 0)
        {
            return (string.Empty, Array.Empty<string>());
        }

        int openParen = normalized.IndexOf('(');
        int closeParen = normalized.LastIndexOf(')');
        if (openParen < 0 || closeParen <= openParen)
        {
            return (normalized, Array.Empty<string>());
        }

        string eventName = normalized[..openParen].Trim();
        string inner = normalized[(openParen + 1)..closeParen].Trim();
        if (inner.Length == 0)
        {
            return (eventName, Array.Empty<string>());
        }

        string[] args = inner
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .ToArray();
        return (eventName, args);
    }

    private static string ResolveComponentName(string? slotKey, IReadOnlyDictionary<string, string> slotNameByKey)
    {
        string key = (slotKey ?? string.Empty).Trim();
        if (key.Length > 0 &&
            slotNameByKey.TryGetValue(key, out string? mappedName) &&
            !string.IsNullOrWhiteSpace(mappedName))
        {
            return SanitizeTitleToken(mappedName);
        }

        if (BuiltInSlotNames.TryGetValue(key, out string? builtInName) &&
            !string.IsNullOrWhiteSpace(builtInName))
        {
            return builtInName;
        }

        if (int.TryParse(key, NumberStyles.Integer, CultureInfo.InvariantCulture, out int numericSlot) &&
            numericSlot >= 0)
        {
            return $"slot{(numericSlot + 1).ToString(CultureInfo.InvariantCulture)}";
        }

        return "unknown";
    }

    private static string SanitizeTitleToken(string value)
    {
        string normalized = (value ?? string.Empty).Replace("\r", string.Empty).Replace("\n", string.Empty).Trim();
        if (normalized.Contains("=====", StringComparison.Ordinal))
        {
            normalized = normalized.Replace("=====", "==", StringComparison.Ordinal);
        }

        return normalized;
    }

    private static string SafeName(string value)
    {
        string cleaned = SafeNameRegex.Replace((value ?? string.Empty).Trim(), "_").Trim('_');
        return cleaned;
    }
}
