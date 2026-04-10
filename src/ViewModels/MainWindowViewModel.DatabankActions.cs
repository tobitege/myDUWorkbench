using CommunityToolkit.Mvvm.ComponentModel;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    [ObservableProperty]
    private bool databankPrettyPrintEnabled;

    public bool CanRefreshDatabankDisplay => !IsBusy && _lastSnapshot is not null && IsDatabaseOnline();
    public bool CanPrettyPrintSelectedDatabank => !string.IsNullOrWhiteSpace(ResolveSelectedTreeRow(SelectedDatabankNode)?.FullContent);
    public bool CanClearSelectedDatabank => !IsBusy && TryResolveSelectedDatabankBlock(out _ ) && IsDatabaseOnline();

    public async Task<LuaBackupEntry?> ClearSelectedDatabankAsync(
        Func<LuaBackupCreateRequest, CancellationToken, Task<LuaBackupEntry>>? createBackupAsync,
        CancellationToken cancellationToken)
    {
        if (!TryResolveSelectedDatabankBlock(out PropertyTreeRow? blockRow) ||
            blockRow?.ElementId is not ulong elementId ||
            elementId == 0UL)
        {
            throw new InvalidOperationException("Select a databank block first.");
        }

        if (!IsDatabaseOnline())
        {
            throw new InvalidOperationException("DB is offline.");
        }

        DataConnectionOptions options = BuildDbOptions();
        LuaBackupEntry? backupEntry = null;
        await _dataService.WriteDatabankPropertyAsync(
            options,
            elementId,
            "{}",
            createBackupAsync is null
                ? null
                : async (rawProperty, token) =>
                {
                    try
                    {
                        LuaBackupCreateRequest request = CreateDatabankBackupRequest(
                            rawProperty,
                            blockRow.ElementDisplayName ?? string.Empty,
                            blockRow.NodeLabel,
                            blockRow.PropertyName,
                            BuildDatabankSuggestedFileName(elementId, blockRow.ElementDisplayName ?? string.Empty, blockRow.PropertyName, blockRow.NodeLabel),
                            options.ServerRootPath);
                        backupEntry = await createBackupAsync(request, token);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        throw new InvalidOperationException($"Backup creation failed: {ex.Message}", ex);
                    }
                },
            cancellationToken);
        await RefreshDatabankDisplayAsync(cancellationToken);
        return backupEntry;
    }

    public async Task<LuaBackupEntry?> RestoreDatabankBackupAsync(
        BackupManagerDialogResult backup,
        Func<LuaBackupCreateRequest, CancellationToken, Task<LuaBackupEntry>>? createBackupAsync,
        CancellationToken cancellationToken)
    {
        if (backup is null)
        {
            throw new ArgumentNullException(nameof(backup));
        }

        if (backup.ContentKind != BackupContentKind.Databank)
        {
            throw new InvalidOperationException("Selected backup is not a databank backup.");
        }

        if (!backup.ElementId.HasValue || backup.ElementId.Value == 0UL)
        {
            throw new InvalidOperationException("Selected backup does not contain a target databank element id.");
        }

        if (!IsDatabaseOnline())
        {
            throw new InvalidOperationException("DB is offline.");
        }

        string propertyName = string.IsNullOrWhiteSpace(backup.PropertyName) ? "databank" : backup.PropertyName;
        string suggestedFileName = string.IsNullOrWhiteSpace(backup.SuggestedFileName)
            ? BuildDatabankSuggestedFileName(backup.ElementId.Value, backup.ElementDisplayName, propertyName, backup.NodeLabel)
            : backup.SuggestedFileName;

        DataConnectionOptions options = BuildDbOptions();
        LuaBackupEntry? backupEntry = null;
        await _dataService.WriteDatabankPropertyAsync(
            options,
            backup.ElementId.Value,
            backup.Content,
            createBackupAsync is null
                ? null
                : async (rawProperty, token) =>
                {
                    try
                    {
                        LuaBackupCreateRequest request = CreateDatabankBackupRequest(
                            rawProperty,
                            backup.ElementDisplayName,
                            backup.NodeLabel,
                            propertyName,
                            suggestedFileName,
                            options.ServerRootPath);
                        backupEntry = await createBackupAsync(request, token);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        throw new InvalidOperationException($"Backup creation failed: {ex.Message}", ex);
                    }
                },
            cancellationToken);
        await RefreshDatabankDisplayAsync(cancellationToken);
        return backupEntry;
    }

    public bool TryGetSelectedDatabankBackupRequest(out LuaBackupCreateRequest? request)
    {
        request = null;

        if (!TryResolveSelectedDatabankBlock(out PropertyTreeRow? blockRow) ||
            blockRow?.ElementId is not ulong elementId ||
            elementId == 0UL)
        {
            return false;
        }

        string propertyName = string.IsNullOrWhiteSpace(blockRow.PropertyName) ? "databank" : blockRow.PropertyName;
        string nodeLabel = string.IsNullOrWhiteSpace(blockRow.NodeLabel) ? "databank" : blockRow.NodeLabel;
        string suggestedName = string.IsNullOrWhiteSpace(BuildSuggestedFileName(blockRow, ".txt"))
            ? BuildDatabankSuggestedFileName(elementId, blockRow.ElementDisplayName, propertyName, nodeLabel)
            : BuildSuggestedFileName(blockRow, ".txt");
        request = new LuaBackupCreateRequest(
            blockRow.FullContent ?? string.Empty,
            suggestedName,
            $"db://element/{elementId}/{propertyName}",
            elementId,
            blockRow.ElementDisplayName ?? string.Empty,
            nodeLabel,
            propertyName,
            BackupContentKind.Databank);
        return true;
    }

    public bool TryGetSelectedDatabankBlobSaveRequest(out BlobSaveRequest? request)
    {
        request = null;

        PropertyTreeRow? row = ResolveSelectedTreeRow(SelectedDatabankNode);
        if (row is null)
        {
            return false;
        }

        string content = SelectedDatabankContent ?? string.Empty;
        if (string.IsNullOrWhiteSpace(content))
        {
            return false;
        }

        string suggestedName = BuildSuggestedFileName(row, ".txt");
        request = new BlobSaveRequest(suggestedName, content, ".txt");
        return true;
    }

    private void RefreshSelectedDatabankContent(PropertyTreeRow? row = null)
    {
        PropertyTreeRow? resolvedRow = row ?? ResolveSelectedTreeRow(SelectedDatabankNode);
        string rawContent = resolvedRow?.FullContent ?? string.Empty;
        SelectedDatabankContent = DatabankPrettyPrintEnabled
            ? PrettyPrintDatabankContent(rawContent)
            : rawContent;
    }

    partial void OnDatabankPrettyPrintEnabledChanged(bool value)
    {
        RefreshSelectedDatabankContent();
    }

    private bool TryResolveSelectedDatabankBlock(out PropertyTreeRow? blockRow)
    {
        blockRow = null;

        PropertyTreeRow? selectedRow = ResolveSelectedTreeRow(SelectedDatabankNode);
        if (selectedRow?.ElementId is not ulong elementId || elementId == 0UL)
        {
            return false;
        }

        if (_databankBlockNodeByElementId.TryGetValue(elementId, out PropertyTreeRow? indexedBlock))
        {
            blockRow = indexedBlock;
            return true;
        }

        if (string.Equals(selectedRow.NodeKind, "Block", StringComparison.Ordinal))
        {
            blockRow = selectedRow;
            return true;
        }

        return false;
    }

    private LuaBackupCreateRequest CreateDatabankBackupRequest(
        LuaPropertyRawRecord rawProperty,
        string elementDisplayName,
        string nodeLabel,
        string propertyName,
        string suggestedFileName,
        string serverRootPath)
    {
        ulong elementId = rawProperty.ElementId;
        if (!TryDecodeDatabankBackupContent(rawProperty.RawValue ?? Array.Empty<byte>(), serverRootPath, out string content, out string? decodeError))
        {
            throw new InvalidOperationException(
                string.IsNullOrWhiteSpace(decodeError)
                    ? "Databank backup content could not be decoded from the live DB value."
                    : $"Databank backup content could not be decoded from the live DB value. {decodeError}");
        }

        string resolvedPropertyName = string.IsNullOrWhiteSpace(propertyName) ? rawProperty.PropertyName : propertyName;
        string resolvedNodeLabel = string.IsNullOrWhiteSpace(nodeLabel) ? "databank" : nodeLabel;
        string resolvedSuggestedFileName = string.IsNullOrWhiteSpace(suggestedFileName)
            ? BuildDatabankSuggestedFileName(elementId, elementDisplayName, resolvedPropertyName, resolvedNodeLabel)
            : suggestedFileName;

        return new LuaBackupCreateRequest(
            content,
            resolvedSuggestedFileName,
            $"db://element/{elementId}/{resolvedPropertyName}",
            elementId,
            elementDisplayName ?? string.Empty,
            resolvedNodeLabel,
            resolvedPropertyName,
            BackupContentKind.Databank);
    }

    private static bool TryDecodeDatabankBackupContent(
        byte[] rawValue,
        string serverRootPath,
        out string content,
        out string? error)
    {
        if (rawValue.Length == 0)
        {
            content = string.Empty;
            error = null;
            return true;
        }

        if (ContentBlobDecoder.TryDecode(rawValue, serverRootPath ?? string.Empty, out ContentBlobDecodeResult? decoded, out string? decodeError) &&
            decoded is not null)
        {
            content = decoded.DecodedText;
            error = null;
            return true;
        }

        string utf8 = Encoding.UTF8.GetString(rawValue).Trim('\0');
        if (IsMostlyPrintableBackupText(utf8))
        {
            content = utf8;
            error = null;
            return true;
        }

        content = string.Empty;
        error = string.IsNullOrWhiteSpace(decodeError)
            ? "The payload is not plain UTF-8 text and the databank content decoder could not decode it."
            : decodeError;
        return false;
    }

    private static bool IsMostlyPrintableBackupText(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return true;
        }

        int printable = 0;
        foreach (char c in value)
        {
            if (!char.IsControl(c) || c == '\r' || c == '\n' || c == '\t')
            {
                printable++;
            }
        }

        return printable >= (value.Length * 8 / 10);
    }

    private static string BuildDatabankSuggestedFileName(
        ulong elementId,
        string elementDisplayName,
        string propertyName,
        string nodeLabel)
    {
        string resolvedElementDisplayName = string.IsNullOrWhiteSpace(elementDisplayName)
            ? "databank"
            : elementDisplayName;
        var row = new PropertyTreeRow(
            nodeLabel,
            "Block",
            elementId,
            resolvedElementDisplayName,
            string.IsNullOrWhiteSpace(propertyName) ? "databank" : propertyName,
            propertyType: null,
            byteLength: null,
            valuePreview: string.Empty,
            fullContent: string.Empty);
        return BuildSuggestedFileName(row, ".txt");
    }

    private static string PrettyPrintDatabankContent(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return string.Empty;
        }

        string normalized = content.Trim();
        string formatted = PrettyPrintStructuredText(normalized);
        return string.IsNullOrWhiteSpace(formatted) ? normalized : formatted;
    }

    private static string PrettyPrintStructuredText(string text)
    {
        var result = new StringBuilder(text.Length + 64);
        int index = 0;
        int indentLevel = 0;
        bool atLineStart = true;

        while (index < text.Length)
        {
            char ch = text[index];
            if (ch == '"' || ch == '\'')
            {
                string quoted = ReadQuotedSegment(text, ref index);
                result.Append(FormatQuotedSegment(quoted));
                atLineStart = false;
                continue;
            }

            switch (ch)
            {
                case '{':
                    AppendIndentIfNeeded(result, indentLevel, ref atLineStart);
                    result.Append('{');
                    index++;
                    if (TryPeekNonWhitespace(text, index, out char nextAfterOpen) && nextAfterOpen != '}')
                    {
                        result.AppendLine();
                        indentLevel++;
                        atLineStart = true;
                    }
                    else
                    {
                        indentLevel++;
                    }
                    break;

                case '}':
                    indentLevel = Math.Max(0, indentLevel - 1);
                    if (!atLineStart)
                    {
                        result.AppendLine();
                        atLineStart = true;
                    }

                    AppendIndentIfNeeded(result, indentLevel, ref atLineStart);
                    result.Append('}');
                    index++;
                    atLineStart = false;
                    break;

                case ',':
                    result.Append(',');
                    result.AppendLine();
                    index++;
                    atLineStart = true;
                    break;

                case ':':
                    result.Append(':');
                    index++;
                    if (!TryPeekNonWhitespace(text, index, out char nextAfterColon) ||
                        !char.IsWhiteSpace(nextAfterColon))
                    {
                        result.Append(' ');
                    }
                    atLineStart = false;
                    break;

                case '\r':
                case '\n':
                    if (!atLineStart)
                    {
                        result.AppendLine();
                        atLineStart = true;
                    }

                    index = SkipNewline(text, index);
                    break;

                default:
                    if (char.IsWhiteSpace(ch))
                    {
                        if (!atLineStart && (result.Length == 0 || !char.IsWhiteSpace(result[^1])))
                        {
                            result.Append(' ');
                        }

                        index++;
                        break;
                    }

                    AppendIndentIfNeeded(result, indentLevel, ref atLineStart);
                    result.Append(ch);
                    index++;
                    atLineStart = false;
                    break;
            }
        }

        return result.ToString().Trim();
    }

    private static string ReadQuotedSegment(string text, ref int index)
    {
        int start = index;
        char quote = text[index++];
        bool escaped = false;

        while (index < text.Length)
        {
            char ch = text[index++];
            if (escaped)
            {
                escaped = false;
                continue;
            }

            if (ch == '\\')
            {
                escaped = true;
                continue;
            }

            if (ch == quote)
            {
                break;
            }
        }

        return text[start..index];
    }

    private static string FormatQuotedSegment(string segment)
    {
        if (segment.Length < 2)
        {
            return segment;
        }

        char quote = segment[0];
        string inner = segment[1..^1];
        string trimmed = inner.TrimStart();
        if (!trimmed.StartsWith("return {", StringComparison.Ordinal) &&
            !trimmed.StartsWith("{", StringComparison.Ordinal))
        {
            return segment;
        }

        string formattedInner = PrettyPrintStructuredText(inner);
        if (string.IsNullOrWhiteSpace(formattedInner))
        {
            return segment;
        }

        return $"{quote}{formattedInner}{quote}";
    }

    private static void AppendIndentIfNeeded(StringBuilder result, int indentLevel, ref bool atLineStart)
    {
        if (!atLineStart)
        {
            return;
        }

        result.Append(' ', indentLevel * 2);
        atLineStart = false;
    }

    private static int SkipNewline(string text, int index)
    {
        if (text[index] == '\r' && index + 1 < text.Length && text[index + 1] == '\n')
        {
            return index + 2;
        }

        return index + 1;
    }

    private static bool TryPeekNonWhitespace(string text, int startIndex, out char value)
    {
        for (int i = startIndex; i < text.Length; i++)
        {
            char ch = text[i];
            if (char.IsWhiteSpace(ch))
            {
                continue;
            }

            value = ch;
            return true;
        }

        value = '\0';
        return false;
    }
}
