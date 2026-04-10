using CommunityToolkit.Mvvm.ComponentModel;
using myDUWorkbench.Models;
using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    [ObservableProperty]
    private bool luaPrettyPrintEnabled;

    [ObservableProperty]
    private bool htmlRsPrettyPrintEnabled;

    public bool CanRefreshLuaDisplay => !IsBusy && _lastSnapshot is not null && IsDatabaseOnline();
    public bool CanRefreshHtmlRsDisplay => !IsBusy && _lastSnapshot is not null && IsDatabaseOnline();
    public bool CanPrettyPrintSelectedLua => !string.IsNullOrWhiteSpace(ResolveSelectedTreeRow(SelectedDpuyaml6Node)?.FullContent);
    public bool CanPrettyPrintSelectedHtmlRs => !string.IsNullOrWhiteSpace(ResolveSelectedTreeRow(SelectedContent2Node)?.FullContent);

    public async Task RefreshLuaDisplayAsync(CancellationToken cancellationToken)
    {
        await RefreshConstructDetailDisplayAsync("LUA block data", cancellationToken);
    }

    public async Task RefreshHtmlRsDisplayAsync(CancellationToken cancellationToken)
    {
        await RefreshConstructDetailDisplayAsync("HTML/RS data", cancellationToken);
    }

    public async Task RefreshDatabankDisplayAsync(CancellationToken cancellationToken)
    {
        await RefreshConstructDetailDisplayAsync("Databank data", cancellationToken);
    }

    private async Task RefreshConstructDetailDisplayAsync(string detailLabel, CancellationToken cancellationToken)
    {
        if (IsBusy)
        {
            return;
        }

        if (_lastSnapshot is not { } snapshot)
        {
            throw new InvalidOperationException("Load a DB snapshot before refreshing construct data.");
        }

        if (!IsDatabaseOnline())
        {
            throw new InvalidOperationException("DB is offline.");
        }

        try
        {
            IsBusy = true;
            StatusMessage = $"Refreshing {detailLabel} for construct {snapshot.ConstructId.ToString(CultureInfo.InvariantCulture)}...";

            int propertyLimit = ParsePropertyLimit(PropertyLimitInput);
            DataConnectionOptions options = BuildDbOptions();
            DatabaseConstructSnapshot refreshedSnapshot = await _dataService.LoadConstructSnapshotAsync(
                options,
                snapshot.ConstructId,
                null,
                propertyLimit,
                cancellationToken);

            _lastSnapshot = refreshedSnapshot;
            RefreshDamagedFilterAvailability();
            OnPropertyChanged(nameof(CanRepairDestroyedElements));
            ActiveConstructName = string.IsNullOrWhiteSpace(refreshedSnapshot.ConstructName)
                ? refreshedSnapshot.ConstructId.ToString(CultureInfo.InvariantCulture)
                : refreshedSnapshot.ConstructName;
            UpdateDatabaseSummary(refreshedSnapshot);
            await ApplyLoadedPropertyCollectionsAsync(refreshedSnapshot.Properties, cancellationToken: cancellationToken);

            StatusMessage = $"{detailLabel} refreshed for construct {refreshedSnapshot.ConstructId.ToString(CultureInfo.InvariantCulture)}.";
        }
        finally
        {
            IsBusy = false;
        }
    }

    private void RefreshSelectedLuaContent(PropertyTreeRow? row = null)
    {
        PropertyTreeRow? resolvedRow = row ?? ResolveSelectedTreeRow(SelectedDpuyaml6Node);
        string rawContent = resolvedRow?.FullContent ?? string.Empty;
        SelectedDpuyaml6Content = LuaPrettyPrintEnabled
            ? PrettyPrintDetailContent(rawContent)
            : rawContent;
    }

    private void RefreshSelectedHtmlRsContent(PropertyTreeRow? row = null)
    {
        PropertyTreeRow? resolvedRow = row ?? ResolveSelectedTreeRow(SelectedContent2Node);
        string rawContent = resolvedRow?.FullContent ?? string.Empty;
        SelectedContent2Content = HtmlRsPrettyPrintEnabled
            ? PrettyPrintDetailContent(rawContent)
            : rawContent;
    }

    partial void OnLuaPrettyPrintEnabledChanged(bool value)
    {
        RefreshSelectedLuaContent();
    }

    partial void OnHtmlRsPrettyPrintEnabledChanged(bool value)
    {
        RefreshSelectedHtmlRsContent();
    }

    private static string PrettyPrintDetailContent(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return string.Empty;
        }

        string normalized = NormalizeLineEndings(content).Trim();
        string header = string.Empty;
        string body = normalized;

        if (normalized.StartsWith("[", StringComparison.Ordinal))
        {
            int separatorIndex = normalized.IndexOf("\n\n", StringComparison.Ordinal);
            if (separatorIndex >= 0)
            {
                header = normalized[..separatorIndex].TrimEnd();
                body = normalized[(separatorIndex + 2)..].TrimStart();
            }
        }

        string formattedBody = LooksLikeStructuredData(body)
            ? PrettyPrintStructuredText(body)
            : PrettyPrintEmbeddedStructuredData(body);

        string combined = string.IsNullOrWhiteSpace(header)
            ? formattedBody
            : $"{header}\n\n{formattedBody}";

        return NormalizeLineEndings(combined).Replace("\n", Environment.NewLine);
    }

    private static bool LooksLikeStructuredData(string text)
    {
        string trimmed = text.TrimStart();
        return trimmed.StartsWith("{", StringComparison.Ordinal) ||
               trimmed.StartsWith("return {", StringComparison.Ordinal) ||
               trimmed.StartsWith("[{", StringComparison.Ordinal);
    }

    private static string PrettyPrintEmbeddedStructuredData(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return string.Empty;
        }

        string normalized = NormalizeLineEndings(text);
        var result = new StringBuilder(normalized.Length + 64);
        int index = 0;

        while (index < normalized.Length)
        {
            if (TryReadLuaLineComment(normalized, index, out int lineCommentEnd))
            {
                result.Append(normalized, index, lineCommentEnd - index);
                index = lineCommentEnd;
                continue;
            }

            if (TryReadLuaLongComment(normalized, index, out int longCommentEnd))
            {
                result.Append(normalized, index, longCommentEnd - index);
                index = longCommentEnd;
                continue;
            }

            if (TryReadLuaLongBracket(normalized, index, out int longBracketEnd))
            {
                result.Append(normalized, index, longBracketEnd - index);
                index = longBracketEnd;
                continue;
            }

            char ch = normalized[index];
            if (ch == '"' || ch == '\'')
            {
                int quotedStart = index;
                string quoted = ReadQuotedSegment(normalized, ref index);
                result.Append(quoted);
                if (index == quotedStart)
                {
                    index++;
                }

                continue;
            }

            if (ch == '{' && TryReadBalancedBraceSegment(normalized, index, out int segmentEnd))
            {
                string segment = normalized[index..segmentEnd];
                result.Append(ShouldPrettyPrintBraceSegment(segment)
                    ? PrettyPrintStructuredText(segment)
                    : segment);
                index = segmentEnd;
                continue;
            }

            result.Append(ch);
            index++;
        }

        return result.ToString();
    }

    private static string NormalizeLineEndings(string text)
    {
        return text.Replace("\r\n", "\n").Replace("\r", "\n");
    }

    private static bool ShouldPrettyPrintBraceSegment(string segment)
    {
        if (segment.Length <= 2)
        {
            return false;
        }

        string inner = segment[1..^1];
        return inner.IndexOf('=') >= 0 ||
               inner.IndexOf(',') >= 0 ||
               inner.IndexOf('{') >= 0 ||
               inner.IndexOf('[') >= 0 ||
               inner.IndexOf('\n') >= 0;
    }

    private static bool TryReadBalancedBraceSegment(string text, int startIndex, out int endExclusive)
    {
        endExclusive = startIndex;
        if (startIndex < 0 || startIndex >= text.Length || text[startIndex] != '{')
        {
            return false;
        }

        int depth = 0;
        int index = startIndex;
        while (index < text.Length)
        {
            if (TryReadLuaLineComment(text, index, out int lineCommentEnd))
            {
                index = lineCommentEnd;
                continue;
            }

            if (TryReadLuaLongComment(text, index, out int longCommentEnd))
            {
                index = longCommentEnd;
                continue;
            }

            if (TryReadLuaLongBracket(text, index, out int longBracketEnd))
            {
                index = longBracketEnd;
                continue;
            }

            char ch = text[index];
            if (ch == '"' || ch == '\'')
            {
                int quotedIndex = index;
                _ = ReadQuotedSegment(text, ref index);
                if (index == quotedIndex)
                {
                    index++;
                }

                continue;
            }

            if (ch == '{')
            {
                depth++;
                index++;
                continue;
            }

            if (ch == '}')
            {
                depth--;
                index++;
                if (depth == 0)
                {
                    endExclusive = index;
                    return true;
                }

                continue;
            }

            index++;
        }

        return false;
    }

    private static bool TryReadLuaLineComment(string text, int startIndex, out int endExclusive)
    {
        endExclusive = startIndex;
        if (startIndex + 1 >= text.Length ||
            text[startIndex] != '-' ||
            text[startIndex + 1] != '-' ||
            TryReadLuaLongComment(text, startIndex, out _))
        {
            return false;
        }

        int newlineIndex = text.IndexOf('\n', startIndex + 2);
        endExclusive = newlineIndex >= 0 ? newlineIndex : text.Length;
        return true;
    }

    private static bool TryReadLuaLongComment(string text, int startIndex, out int endExclusive)
    {
        endExclusive = startIndex;
        if (startIndex + 2 >= text.Length ||
            text[startIndex] != '-' ||
            text[startIndex + 1] != '-')
        {
            return false;
        }

        return TryReadLuaLongBracket(text, startIndex + 2, out endExclusive);
    }

    private static bool TryReadLuaLongBracket(string text, int startIndex, out int endExclusive)
    {
        endExclusive = startIndex;
        if (startIndex >= text.Length || text[startIndex] != '[')
        {
            return false;
        }

        int index = startIndex + 1;
        while (index < text.Length && text[index] == '=')
        {
            index++;
        }

        if (index >= text.Length || text[index] != '[')
        {
            return false;
        }

        string closingToken = "]" + new string('=', index - startIndex - 1) + "]";
        int closingIndex = text.IndexOf(closingToken, index + 1, StringComparison.Ordinal);
        if (closingIndex < 0)
        {
            endExclusive = text.Length;
            return true;
        }

        endExclusive = closingIndex + closingToken.Length;
        return true;
    }
}
