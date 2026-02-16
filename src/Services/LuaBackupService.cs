// Helper Index:
// - CreateBackupAsync: Writes timestamped backup snapshots with metadata header and script payload.
// - GetBackupsAsync: Lists and parses all backup files for UI browsing.
// - ReadBackupAsync: Loads backup file and separates metadata header from script content.
namespace MyDu.Services;

using MyDu.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public sealed class LuaBackupService
{
    private const string BackupHeaderMagic = "-- mydu-lua-backup-v1";
    private const string BackupHeaderEnd = "-- ---";
    private const int PreviewMaxLength = 140;

    private readonly string _backupDirectoryPath;

    public LuaBackupService(string? backupDirectoryPath = null)
    {
        _backupDirectoryPath = backupDirectoryPath ??
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "MyDu",
                "LuaBackups");
    }

    public string BackupDirectoryPath => _backupDirectoryPath;

    public async Task<LuaBackupEntry> CreateBackupAsync(LuaBackupCreateRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Directory.CreateDirectory(_backupDirectoryPath);

        DateTimeOffset backupUtc = DateTimeOffset.UtcNow;
        string fileName = BuildBackupFileName(request, backupUtc);
        string filePath = Path.Combine(_backupDirectoryPath, fileName);
        string content = request.Content ?? string.Empty;
        string header = BuildHeader(request, backupUtc, content);
        string payload = header + content;
        await File.WriteAllTextAsync(filePath, payload, new UTF8Encoding(false), cancellationToken).ConfigureAwait(false);

        return BuildEntry(
            filePath,
            fileName,
            backupUtc,
            request.ElementId,
            request.ElementDisplayName,
            request.NodeLabel,
            request.PropertyName,
            request.SourceFilePath,
            request.SuggestedFileName,
            BuildPreview(content));
    }

    public async Task<IReadOnlyList<LuaBackupEntry>> GetBackupsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(_backupDirectoryPath))
        {
            return Array.Empty<LuaBackupEntry>();
        }

        string[] files = Directory.GetFiles(_backupDirectoryPath, "*.lua.bak", SearchOption.TopDirectoryOnly);
        var entries = new List<LuaBackupEntry>(files.Length);
        foreach (string path in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LuaBackupDocument? document = await ReadBackupAsync(path, cancellationToken).ConfigureAwait(false);
            if (document is not null)
            {
                entries.Add(document.Entry);
            }
        }

        return entries
            .OrderByDescending(entry => entry.BackupUtc)
            .ThenByDescending(entry => entry.FileName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<LuaBackupDocument?> ReadBackupAsync(string filePath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(filePath) || !File.Exists(filePath))
        {
            return null;
        }

        string raw = await File.ReadAllTextAsync(filePath, cancellationToken).ConfigureAwait(false);
        ParsedBackup parsed = ParseBackup(filePath, raw);
        var entry = BuildEntry(
            filePath,
            Path.GetFileName(filePath),
            parsed.BackupUtc,
            parsed.ElementId,
            parsed.ElementDisplayName,
            parsed.NodeLabel,
            parsed.PropertyName,
            parsed.SourceFilePath,
            parsed.SuggestedFileName,
            BuildPreview(parsed.ScriptContent));
        return new LuaBackupDocument(entry, raw, parsed.ScriptContent);
    }

    public Task DeleteBackupAsync(string filePath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.IsNullOrWhiteSpace(filePath) && File.Exists(filePath))
        {
            File.Delete(filePath);
        }

        return Task.CompletedTask;
    }

    public Task<int> DeleteAllBackupsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(_backupDirectoryPath))
        {
            return Task.FromResult(0);
        }

        int deleted = 0;
        foreach (string filePath in Directory.GetFiles(_backupDirectoryPath, "*.lua.bak", SearchOption.TopDirectoryOnly))
        {
            cancellationToken.ThrowIfCancellationRequested();
            File.Delete(filePath);
            deleted++;
        }

        return Task.FromResult(deleted);
    }

    private static LuaBackupEntry BuildEntry(
        string filePath,
        string fileName,
        DateTimeOffset backupUtc,
        ulong? elementId,
        string elementDisplayName,
        string nodeLabel,
        string propertyName,
        string sourceFilePath,
        string suggestedFileName,
        string preview)
    {
        return new LuaBackupEntry(
            filePath,
            fileName,
            backupUtc,
            elementId,
            elementDisplayName ?? string.Empty,
            nodeLabel ?? string.Empty,
            propertyName ?? string.Empty,
            sourceFilePath ?? string.Empty,
            suggestedFileName ?? string.Empty,
            preview ?? string.Empty);
    }

    private static string BuildBackupFileName(LuaBackupCreateRequest request, DateTimeOffset backupUtc)
    {
        string timestamp = backupUtc.ToString("yyyyMMdd-HHmmss-fff", CultureInfo.InvariantCulture);
        string elementIdPart = request.ElementId.HasValue
            ? $"elm{request.ElementId.Value.ToString(CultureInfo.InvariantCulture)}"
            : "elmUnknown";
        string elementPart = SanitizeFileNameSegment(request.ElementDisplayName);
        string nodePart = SanitizeFileNameSegment(request.NodeLabel);
        string propertyPart = SanitizeFileNameSegment(request.PropertyName);

        var parts = new List<string>
        {
            timestamp,
            elementIdPart
        };

        if (!string.IsNullOrWhiteSpace(elementPart))
        {
            parts.Add(elementPart);
        }

        if (!string.IsNullOrWhiteSpace(nodePart))
        {
            parts.Add(nodePart);
        }

        if (!string.IsNullOrWhiteSpace(propertyPart))
        {
            parts.Add(propertyPart);
        }

        if (parts.Count < 3)
        {
            string suggested = SanitizeFileNameSegment(Path.GetFileNameWithoutExtension(request.SuggestedFileName));
            if (!string.IsNullOrWhiteSpace(suggested))
            {
                parts.Add(suggested);
            }
        }

        return string.Join("_", parts) + ".lua.bak";
    }

    private static string BuildHeader(LuaBackupCreateRequest request, DateTimeOffset backupUtc, string content)
    {
        string hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
        var header = new StringBuilder();
        header.AppendLine(BackupHeaderMagic);
        header.AppendLine($"-- backupUtc: {backupUtc:O}");
        header.AppendLine($"-- elementId: {(request.ElementId.HasValue ? request.ElementId.Value.ToString(CultureInfo.InvariantCulture) : string.Empty)}");
        header.AppendLine($"-- elementName: {NormalizeHeaderValue(request.ElementDisplayName)}");
        header.AppendLine($"-- nodeLabel: {NormalizeHeaderValue(request.NodeLabel)}");
        header.AppendLine($"-- propertyName: {NormalizeHeaderValue(request.PropertyName)}");
        header.AppendLine($"-- sourceFilePath: {NormalizeHeaderValue(request.SourceFilePath)}");
        header.AppendLine($"-- suggestedFileName: {NormalizeHeaderValue(request.SuggestedFileName)}");
        header.AppendLine($"-- sha256: {hash}");
        header.AppendLine(BackupHeaderEnd);
        header.AppendLine();
        return header.ToString();
    }

    private static string NormalizeHeaderValue(string? value)
    {
        return (value ?? string.Empty).Replace("\r", " ").Replace("\n", " ").Trim();
    }

    private static string BuildPreview(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return string.Empty;
        }

        string normalized = content.Replace("\r\n", "\n").Replace('\r', '\n');
        int end = normalized.IndexOf('\n');
        string firstLine = end >= 0 ? normalized[..end] : normalized;
        string collapsed = string.Join(" ", firstLine.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries));
        if (collapsed.Length <= PreviewMaxLength)
        {
            return collapsed;
        }

        return collapsed[..PreviewMaxLength] + "...";
    }

    private static string SanitizeFileNameSegment(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        string invalidChars = new(Path.GetInvalidFileNameChars());
        var sanitized = new StringBuilder(value.Length);
        foreach (char c in value)
        {
            if (invalidChars.IndexOf(c) >= 0)
            {
                sanitized.Append('_');
            }
            else if (char.IsWhiteSpace(c))
            {
                sanitized.Append('_');
            }
            else
            {
                sanitized.Append(c);
            }
        }

        string compact = sanitized.ToString().Trim('_');
        if (compact.Length > 80)
        {
            compact = compact[..80];
        }

        return compact;
    }

    private static ParsedBackup ParseBackup(string filePath, string raw)
    {
        DateTimeOffset fallbackUtc = File.GetLastWriteTimeUtc(filePath);
        string fileName = Path.GetFileName(filePath);

        if (string.IsNullOrWhiteSpace(raw) || !raw.StartsWith(BackupHeaderMagic, StringComparison.Ordinal))
        {
            return new ParsedBackup(
                fallbackUtc,
                null,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                Path.GetFileNameWithoutExtension(fileName),
                raw ?? string.Empty);
        }

        string normalized = raw.Replace("\r\n", "\n");
        int headerEndIndex = normalized.IndexOf(BackupHeaderEnd, StringComparison.Ordinal);
        if (headerEndIndex < 0)
        {
            return new ParsedBackup(
                fallbackUtc,
                null,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                Path.GetFileNameWithoutExtension(fileName),
                raw);
        }

        int scriptStart = headerEndIndex + BackupHeaderEnd.Length;
        while (scriptStart < normalized.Length && (normalized[scriptStart] == '\n' || normalized[scriptStart] == '\r'))
        {
            scriptStart++;
        }

        string header = normalized[..headerEndIndex];
        string script = scriptStart < normalized.Length ? normalized[scriptStart..] : string.Empty;
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (string line in header.Split('\n'))
        {
            string trimmed = line.Trim();
            if (!trimmed.StartsWith("-- ", StringComparison.Ordinal) || trimmed.Length <= 3)
            {
                continue;
            }

            string payload = trimmed[3..];
            int sep = payload.IndexOf(':');
            if (sep <= 0)
            {
                continue;
            }

            string key = payload[..sep].Trim();
            string value = payload[(sep + 1)..].Trim();
            values[key] = value;
        }

        DateTimeOffset backupUtc = fallbackUtc;
        if (values.TryGetValue("backupUtc", out string? backupUtcText) &&
            DateTimeOffset.TryParse(backupUtcText, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset parsedUtc))
        {
            backupUtc = parsedUtc;
        }

        ulong? elementId = null;
        if (values.TryGetValue("elementId", out string? elementIdText) &&
            ulong.TryParse(elementIdText, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsedElementId))
        {
            elementId = parsedElementId;
        }

        values.TryGetValue("elementName", out string? elementName);
        values.TryGetValue("nodeLabel", out string? nodeLabel);
        values.TryGetValue("propertyName", out string? propertyName);
        values.TryGetValue("sourceFilePath", out string? sourceFilePath);
        values.TryGetValue("suggestedFileName", out string? suggestedFileName);

        return new ParsedBackup(
            backupUtc,
            elementId,
            elementName ?? string.Empty,
            nodeLabel ?? string.Empty,
            propertyName ?? string.Empty,
            sourceFilePath ?? string.Empty,
            string.IsNullOrWhiteSpace(suggestedFileName) ? Path.GetFileNameWithoutExtension(fileName) : suggestedFileName,
            script);
    }

    private sealed record ParsedBackup(
        DateTimeOffset BackupUtc,
        ulong? ElementId,
        string ElementDisplayName,
        string NodeLabel,
        string PropertyName,
        string SourceFilePath,
        string SuggestedFileName,
        string ScriptContent);
}
