using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using AvaloniaEdit.Document;

namespace MyDu.Helpers;

public sealed record LuaFoldRegion(int StartOffset, int EndOffset, string Title);

public static class LuaCodeFoldingBuilder
{
    private static readonly Regex StartKeywordRegex = new(
        @"^\s*(?<kw>function|if|for|while|do|repeat)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex EndKeywordRegex = new(
        @"^\s*(?<kw>end|until)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex CommentLineRegex = new(
        @"^\s*--",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static IReadOnlyList<LuaFoldRegion> BuildRegions(TextDocument document)
    {
        if (document is null || document.LineCount == 0)
        {
            return Array.Empty<LuaFoldRegion>();
        }

        var results = new List<LuaFoldRegion>();
        var stack = new Stack<(string Keyword, int StartOffset, int StartLine, string Title)>();

        for (int lineNumber = 1; lineNumber <= document.LineCount; lineNumber++)
        {
            DocumentLine line = document.GetLineByNumber(lineNumber);
            string lineText = document.GetText(line);

            if (CommentLineRegex.IsMatch(lineText))
            {
                continue;
            }

            Match startMatch = StartKeywordRegex.Match(lineText);
            if (startMatch.Success)
            {
                string keyword = startMatch.Groups["kw"].Value;
                stack.Push((keyword, line.Offset, lineNumber, BuildFoldTitle(keyword, lineText)));
                continue;
            }

            Match endMatch = EndKeywordRegex.Match(lineText);
            if (!endMatch.Success)
            {
                continue;
            }

            string endKeyword = endMatch.Groups["kw"].Value;
            if (endKeyword.Equals("until", StringComparison.Ordinal))
            {
                if (TryPopForUntil(stack, out var startForUntil) && startForUntil.StartLine < lineNumber)
                {
                    results.Add(new LuaFoldRegion(startForUntil.StartOffset, line.EndOffset, startForUntil.Title));
                }

                continue;
            }

            if (TryPopForEnd(stack, out var startForEnd) && startForEnd.StartLine < lineNumber)
            {
                results.Add(new LuaFoldRegion(startForEnd.StartOffset, line.EndOffset, startForEnd.Title));
            }
        }

        return results
            .Where(r => r.EndOffset > r.StartOffset)
            .OrderBy(r => r.StartOffset)
            .ThenBy(r => r.EndOffset)
            .ToList();
    }

    private static bool TryPopForUntil(
        Stack<(string Keyword, int StartOffset, int StartLine, string Title)> stack,
        out (string Keyword, int StartOffset, int StartLine, string Title) value)
    {
        if (stack.Count > 0 && string.Equals(stack.Peek().Keyword, "repeat", StringComparison.Ordinal))
        {
            value = stack.Pop();
            return true;
        }

        value = default;
        return false;
    }

    private static bool TryPopForEnd(
        Stack<(string Keyword, int StartOffset, int StartLine, string Title)> stack,
        out (string Keyword, int StartOffset, int StartLine, string Title) value)
    {
        while (stack.Count > 0)
        {
            var candidate = stack.Pop();
            if (!string.Equals(candidate.Keyword, "repeat", StringComparison.Ordinal))
            {
                value = candidate;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static string BuildFoldTitle(string keyword, string lineText)
    {
        string trimmed = (lineText ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            return keyword;
        }

        return trimmed.Length > 80 ? trimmed[..80] + "..." : trimmed;
    }
}
