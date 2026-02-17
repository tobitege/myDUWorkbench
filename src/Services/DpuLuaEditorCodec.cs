// Helper Index:
// - TryReencodeCombinedLua: Rewrites handler/method/event code sections into original JSON and returns DB-storable bytes.
// - BuildCombinedLuaFromDbValue: Builds editable combined Lua text from stored DB value.
//
// DB Save Overview (dpuyaml_6):
// 1) Caller reads and locks target row in a DB transaction (SELECT ... FOR UPDATE).
// 2) Caller verifies optimistic concurrency by comparing current DB bytes with originally loaded bytes.
// 3) This codec decodes dpuyaml payload (LZ4 + JSON), maps edited combined-Lua sections back to JSON, and re-encodes payload.
// 4) For property_type=7/hash-backed values, payload is written to data/user_content/<sha256>; DB value stores the hash text.
// 5) For inline values, DB value stores the encoded payload bytes directly.
// 6) Caller verifies decodability of the newly encoded value before UPDATE+COMMIT.
// 7) Note: DB transaction and file write cannot be one physical atomic transaction across systems; orphan blobs are possible on failure.
//
// Critical Safety Findings Captured Here:
// - Section header parsing must be strict. We only accept generated headers:
//   "-- ===== 001 <title> =====" (regex-based) to avoid false positives from normal Lua comments.
// - Duplicate section titles are dangerous. Dictionary-based title matching can silently overwrite mappings.
//   We detect duplicates and require strict positional matching when duplicates exist.
// - Section cardinality must match exactly. Partial matches are rejected to prevent silent partial DB rewrites.
// - Hash-backed payload absence is explicit. If property_type=7 and referenced blob is missing, fail fast with clear error.
// - Single-section payloads are intentionally flexible: if exactly one DB section and one edited section exist, body replacement is allowed.
namespace myDUWorker.Services;

using K4os.Compression.LZ4;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

public sealed record DpuLuaReencodeResult(
    byte[] DbValue,
    bool UsesHashReference,
    string? HashReference,
    int SectionCount);

public static class DpuLuaEditorCodec
{
    private static readonly Regex HashRegex = new("^[0-9a-f]{64}$", RegexOptions.Compiled);
    private static readonly Regex SectionHeaderRegex = new(
        "^\\s*-- ===== (?<index>\\d{3}) (?<title>.+?) =====\\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static bool TryBuildCombinedLuaFromDbValue(byte[] dbValue, string serverRootPath, out string combinedLua, out int sectionCount, out string? error)
    {
        combinedLua = string.Empty;
        sectionCount = 0;
        error = null;
        if (!DpuLuaDecoder.TryDecode(dbValue, serverRootPath, out DpuLuaDecodeResult? decoded, out string? decodeError) || decoded is null)
        {
            error = string.IsNullOrWhiteSpace(decodeError) ? "Could not decode dpuyaml payload." : decodeError;
            return false;
        }

        combinedLua = decoded.DecodedText ?? string.Empty;
        sectionCount = decoded.SectionCount;
        return true;
    }

    public static bool TryReencodeCombinedLua(
        byte[] currentDbValue,
        int propertyType,
        string serverRootPath,
        string editedCombinedLua,
        out DpuLuaReencodeResult? result,
        out string? error)
    {
        result = null;
        error = null;

        try
        {
            byte[] payload = currentDbValue;
            bool isHashBacked = false;
            if (TryResolveHashBlob(currentDbValue, serverRootPath, out byte[] resolvedPayload, out string? _))
            {
                payload = resolvedPayload;
                isHashBacked = true;
            }
            else if (propertyType == 7)
            {
                throw new InvalidOperationException("Hash-backed dpuyaml blob is missing in server data\\user_content.");
            }

            JsonObject root = ParsePayloadObject(payload);
            List<SectionTarget> targets = ExtractSectionTargets(root);
            ApplyEditedCode(targets, editedCombinedLua ?? string.Empty);

            byte[] encodedPayload = EncodePayload(root);
            int sectionCount = targets.Count;

            if (isHashBacked || propertyType == 7)
            {
                string hash = Convert.ToHexStringLower(SHA256.HashData(encodedPayload));
                WriteHashBlobAtomically(serverRootPath, hash, encodedPayload);
                byte[] dbValue = Encoding.UTF8.GetBytes(hash);
                result = new DpuLuaReencodeResult(dbValue, true, hash, sectionCount);
                return true;
            }

            result = new DpuLuaReencodeResult(encodedPayload, false, null, sectionCount);
            return true;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }
    }

    private static JsonObject ParsePayloadObject(byte[] payload)
    {
        byte[] decodedBytes = DecodeLz4Payload(payload);
        JsonNode? node = JsonNode.Parse(decodedBytes);
        if (node is not JsonObject root)
        {
            throw new InvalidOperationException("dpuyaml payload root is not a JSON object.");
        }

        return root;
    }

    private static byte[] EncodePayload(JsonObject root)
    {
        string json = root.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = false
        });
        byte[] uncompressed = Encoding.UTF8.GetBytes(json);
        if (uncompressed.Length == 0)
        {
            throw new InvalidOperationException("Encoded dpuyaml JSON is empty.");
        }

        int maxEncoded = LZ4Codec.MaximumOutputSize(uncompressed.Length);
        var encoded = new byte[maxEncoded];
        int encodedLength = LZ4Codec.Encode(
            uncompressed,
            0,
            uncompressed.Length,
            encoded,
            0,
            maxEncoded);
        if (encodedLength <= 0)
        {
            throw new InvalidOperationException("LZ4 encode failed.");
        }

        var payload = new byte[4 + encodedLength];
        BitConverter.GetBytes(uncompressed.Length).CopyTo(payload, 0);
        Buffer.BlockCopy(encoded, 0, payload, 4, encodedLength);
        return payload;
    }

    private static void ApplyEditedCode(IReadOnlyList<SectionTarget> targets, string editedCombinedLua)
    {
        List<ParsedSection> editedSections = ParseCombinedLuaSections(editedCombinedLua);
        if (editedSections.Count == 0)
        {
            if (targets.Count != 1)
            {
                throw new InvalidOperationException("Multiple Lua sections exist. Keep section headers when saving to DB.");
            }

            targets[0].TargetObject["code"] = editedCombinedLua.TrimEnd();
            return;
        }

        if (targets.Count == 1 && editedSections.Count == 1)
        {
            targets[0].TargetObject["code"] = editedSections[0].Code;
            return;
        }

        if (editedSections.Count != targets.Count)
        {
            throw new InvalidOperationException(
                $"Edited section count ({editedSections.Count}) does not match original section count ({targets.Count}).");
        }

        bool hasTargetDuplicates = HasDuplicateTitles(targets.Select(target => target.Title));
        bool hasEditedDuplicates = HasDuplicateTitles(editedSections.Select(section => section.Title));
        if (hasTargetDuplicates || hasEditedDuplicates)
        {
            for (int i = 0; i < targets.Count; i++)
            {
                SectionTarget target = targets[i];
                ParsedSection edited = editedSections[i];
                if (!string.Equals(target.Title, edited.Title, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        "Duplicate section titles detected. Keep original section order and headers unchanged.");
                }

                target.TargetObject["code"] = edited.Code;
            }

            return;
        }

        var editedByTitle = editedSections.ToDictionary(section => section.Title, section => section.Code, StringComparer.Ordinal);
        foreach (SectionTarget target in targets)
        {
            if (!editedByTitle.TryGetValue(target.Title, out string? updatedCode))
            {
                throw new InvalidOperationException($"Edited Lua is missing section: {target.Title}");
            }

            target.TargetObject["code"] = updatedCode;
        }
    }

    private static bool HasDuplicateTitles(IEnumerable<string> titles)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (string title in titles)
        {
            if (!seen.Add(title))
            {
                return true;
            }
        }

        return false;
    }

    private static List<ParsedSection> ParseCombinedLuaSections(string text)
    {
        string normalized = (text ?? string.Empty).Replace("\r\n", "\n");
        string[] lines = normalized.Split('\n');
        var sections = new List<ParsedSection>();
        int currentHeaderLine = -1;
        string currentTitle = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            if (!TryParseHeaderLine(lines[i], out string title))
            {
                continue;
            }

            if (currentHeaderLine >= 0)
            {
                sections.Add(new ParsedSection(currentTitle, JoinSectionBody(lines, currentHeaderLine + 1, i - 1)));
            }

            currentHeaderLine = i;
            currentTitle = title;
        }

        if (currentHeaderLine >= 0)
        {
            sections.Add(new ParsedSection(currentTitle, JoinSectionBody(lines, currentHeaderLine + 1, lines.Length - 1)));
        }

        return sections;
    }

    private static bool TryParseHeaderLine(string line, out string title)
    {
        title = string.Empty;
        if (line is null)
        {
            return false;
        }

        Match match = SectionHeaderRegex.Match(line);
        if (!match.Success)
        {
            return false;
        }

        string inner = match.Groups["title"].Value.Trim();
        if (inner.Length == 0)
        {
            return false;
        }

        title = inner;
        return true;
    }

    private static string JoinSectionBody(string[] lines, int start, int end)
    {
        if (start > end || start < 0 || end >= lines.Length)
        {
            return string.Empty;
        }

        return string.Join(Environment.NewLine, lines[start..(end + 1)]).TrimEnd();
    }

    private static List<SectionTarget> ExtractSectionTargets(JsonObject root)
    {
        var sections = new List<SectionTarget>();
        Dictionary<string, string> slotNameByKey = ExtractSlotNameMap(root);

        if (root["handlers"] is JsonArray handlers)
        {
            int idx = 0;
            foreach (JsonNode? node in handlers)
            {
                idx++;
                if (node is not JsonObject item)
                {
                    continue;
                }

                string key = item["key"]?.ToString() ?? idx.ToString(CultureInfo.InvariantCulture);
                string signature = string.Empty;
                string slotKey = string.Empty;
                List<string> filterArgs = ReadFilterArgs(item);
                if (item["filter"] is JsonObject filter)
                {
                    signature = ReadNodeText(filter["signature"]);
                    slotKey = ReadNodeText(filter["slotKey"]);
                }

                if (string.IsNullOrWhiteSpace(signature))
                {
                    signature = ReadNodeText(item["signature"]);
                }

                if (string.IsNullOrWhiteSpace(slotKey))
                {
                    slotKey = ReadNodeText(item["slotKey"]);
                }

                string title = DpuLuaSectionTitleBuilder.BuildHandlerTitle(idx, key, slotKey, signature, filterArgs, slotNameByKey);
                sections.Add(new SectionTarget(title, item));
            }
        }

        if (root["methods"] is JsonArray methods)
        {
            int idx = 0;
            foreach (JsonNode? node in methods)
            {
                idx++;
                if (node is not JsonObject item)
                {
                    continue;
                }

                string methodName = ReadNodeText(item["name"]);
                string title = DpuLuaSectionTitleBuilder.BuildMethodTitle(idx, methodName);
                sections.Add(new SectionTarget(title, item));
            }
        }

        if (root["events"] is JsonArray eventsArray)
        {
            int idx = 0;
            foreach (JsonNode? node in eventsArray)
            {
                idx++;
                if (node is not JsonObject item)
                {
                    continue;
                }

                string eventName = ReadNodeText(item["name"]);
                string title = DpuLuaSectionTitleBuilder.BuildEventTitle(idx, eventName);
                sections.Add(new SectionTarget(title, item));
            }
        }

        return sections;
    }

    private static Dictionary<string, string> ExtractSlotNameMap(JsonObject root)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (root["slots"] is not JsonObject slots)
        {
            return map;
        }

        foreach ((string? slotKey, JsonNode? slotNode) in slots)
        {
            if (string.IsNullOrWhiteSpace(slotKey) || slotNode is not JsonObject slotObject)
            {
                continue;
            }

            string slotName = ReadNodeText(slotObject["name"]);
            if (!string.IsNullOrWhiteSpace(slotName))
            {
                map[slotKey] = slotName.Trim();
            }
        }

        return map;
    }

    private static List<string> ReadFilterArgs(JsonObject item)
    {
        var args = new List<string>();
        if (item["filter"] is not JsonObject filter ||
            filter["args"] is not JsonArray argsArray)
        {
            return args;
        }

        foreach (JsonNode? argNode in argsArray)
        {
            if (argNode is JsonObject argObject)
            {
                string value = ReadNodeText(argObject["value"]);
                if (string.IsNullOrWhiteSpace(value))
                {
                    value = ReadNodeText(argObject["variable"]);
                }

                args.Add(value);
                continue;
            }

            args.Add(ReadNodeText(argNode));
        }

        return args;
    }

    private static string ReadNodeText(JsonNode? node)
    {
        return node switch
        {
            JsonValue valueNode => valueNode.TryGetValue<string>(out string? textValue) ? textValue ?? string.Empty : valueNode.ToJsonString(),
            _ => string.Empty
        };
    }

    private static bool TryResolveHashBlob(byte[] value, string serverRootPath, out byte[] payload, out string? blobPath)
    {
        payload = value;
        blobPath = null;
        string text;
        try
        {
            text = Encoding.UTF8.GetString(value).Trim();
        }
        catch
        {
            return false;
        }

        if (!HashRegex.IsMatch(text))
        {
            return false;
        }

        string path = Path.Combine(serverRootPath, "data", "user_content", text);
        if (!File.Exists(path))
        {
            return false;
        }

        payload = File.ReadAllBytes(path);
        blobPath = path;
        return true;
    }

    private static byte[] DecodeLz4Payload(byte[] blob)
    {
        if (blob.Length < 8)
        {
            throw new InvalidOperationException("Blob too short for LZ4 header.");
        }

        int uncompressedSize = BitConverter.ToInt32(blob, 0);
        if (uncompressedSize <= 0)
        {
            throw new InvalidOperationException($"Invalid uncompressed size: {uncompressedSize}.");
        }

        int compressedLength = blob.Length - 4;
        var decoded = new byte[uncompressedSize];
        int decodedLength = LZ4Codec.Decode(
            blob,
            4,
            compressedLength,
            decoded,
            0,
            uncompressedSize);
        if (decodedLength < 0)
        {
            throw new InvalidOperationException("LZ4 decode failed.");
        }

        if (decodedLength != uncompressedSize)
        {
            throw new InvalidOperationException($"Decoded size mismatch: got {decodedLength}, expected {uncompressedSize}.");
        }

        return decoded;
    }

    private static void WriteHashBlobAtomically(string serverRootPath, string hash, byte[] payload)
    {
        if (!HashRegex.IsMatch(hash))
        {
            throw new InvalidOperationException("Generated invalid content hash.");
        }

        string userContentDirectory = Path.Combine(serverRootPath, "data", "user_content");
        Directory.CreateDirectory(userContentDirectory);
        string targetPath = Path.Combine(userContentDirectory, hash);

        if (File.Exists(targetPath))
        {
            return;
        }

        string tempPath = targetPath + ".tmp." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        File.WriteAllBytes(tempPath, payload);
        try
        {
            File.Move(tempPath, targetPath);
        }
        catch
        {
            if (!File.Exists(targetPath))
            {
                throw;
            }
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    private sealed record SectionTarget(string Title, JsonObject TargetObject);
    private sealed record ParsedSection(string Title, string Code);
}
