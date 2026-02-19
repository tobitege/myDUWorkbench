// Helper Index:
// - TryDecode: Resolves dpuyaml hash blobs, decodes LZ4 JSON, and emits stitched Lua sections.
// - DecodeLz4Payload: Performs strict LZ4 decode with size validation and actionable errors.
// - ExtractCodeSections: Pulls handlers/methods/events code blocks from decoded JSON payloads.
// - BuildCombinedLua: Concatenates extracted code sections into readable export text.
using K4os.Compression.LZ4;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace myDUWorkbench.Services;

public sealed record DpuLuaDecodeResult(
    string DecodedText,
    int DbValueBytes,
    int PayloadBytes,
    int DecodedBytes,
    int SectionCount);

public static class DpuLuaDecoder
{
    private static readonly Regex HashRegex = new("^[0-9a-f]{64}$", RegexOptions.Compiled);

    public static bool TryDecode(byte[] dbValue, string serverRootPath, out DpuLuaDecodeResult? result, out string? error)
    {
        result = null;
        error = null;

        if (dbValue.Length == 0)
        {
            error = "dpuyaml payload is empty.";
            return false;
        }

        byte[] payload = dbValue;
        if (TryResolveHashBlob(dbValue, serverRootPath, out byte[] resolvedPayload))
        {
            payload = resolvedPayload;
        }

        try
        {
            byte[] decodedBytes = DecodeLz4Payload(payload);
            string decodedJsonText = Encoding.UTF8.GetString(decodedBytes);
            using JsonDocument document = JsonDocument.Parse(decodedJsonText);

            List<(string Name, string Code)> sections = ExtractCodeSections(document.RootElement);
            string decodedText;
            if (sections.Count > 0)
            {
                decodedText = BuildCombinedLua(sections);
            }
            else
            {
                decodedText = JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions
                {
                    WriteIndented = true
                });
            }

            result = new DpuLuaDecodeResult(
                decodedText,
                dbValue.Length,
                payload.Length,
                decodedBytes.Length,
                sections.Count);

            return true;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }
    }

    public static bool TryBuildCombinedLuaFromJsonText(
        string jsonText,
        out string combinedLua,
        out int sectionCount,
        out string? error)
    {
        combinedLua = string.Empty;
        sectionCount = 0;
        error = null;

        if (string.IsNullOrWhiteSpace(jsonText))
        {
            error = "JSON payload is empty.";
            return false;
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(jsonText);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                error = "JSON root must be an object.";
                return false;
            }

            List<(string Name, string Code)> sections = ExtractCodeSections(document.RootElement);
            if (sections.Count == 0)
            {
                error = "No handlers/methods/events code sections found in JSON.";
                return false;
            }

            combinedLua = BuildCombinedLua(sections);
            sectionCount = sections.Count;
            return true;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }
    }

    private static bool TryResolveHashBlob(byte[] value, string serverRootPath, out byte[] payload)
    {
        payload = value;

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
            throw new InvalidOperationException(
                $"Decoded size mismatch: got {decodedLength}, expected {uncompressedSize}.");
        }

        return decoded;
    }

    private static List<(string Name, string Code)> ExtractCodeSections(JsonElement root)
    {
        var sections = new List<(string Name, string Code)>();
        Dictionary<string, string> slotNameByKey = ExtractSlotNameMap(root);

        if (root.ValueKind != JsonValueKind.Object)
        {
            return sections;
        }

        if (root.TryGetProperty("handlers", out JsonElement handlers) && handlers.ValueKind == JsonValueKind.Array)
        {
            int idx = 0;
            foreach (JsonElement item in handlers.EnumerateArray())
            {
                idx++;
                if (item.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryGetString(item, "code", out string? code) || string.IsNullOrEmpty(code))
                {
                    continue;
                }

                string key = item.TryGetProperty("key", out JsonElement keyElement)
                    ? keyElement.ToString()
                    : idx.ToString(CultureInfo.InvariantCulture);

                string signature = GetHandlerSignature(item);
                string slotKey = GetHandlerSlotKey(item);
                IReadOnlyList<string> filterArgs = GetHandlerFilterArgs(item);
                string title = DpuLuaSectionTitleBuilder.BuildHandlerTitle(idx, key, slotKey, signature, filterArgs, slotNameByKey);
                sections.Add((title, code));
            }
        }

        if (root.TryGetProperty("methods", out JsonElement methods) && methods.ValueKind == JsonValueKind.Array)
        {
            int idx = 0;
            foreach (JsonElement item in methods.EnumerateArray())
            {
                idx++;
                if (item.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryGetString(item, "code", out string? code) || string.IsNullOrEmpty(code))
                {
                    continue;
                }

                string name = TryGetString(item, "name", out string? methodName) && !string.IsNullOrWhiteSpace(methodName)
                    ? methodName
                    : string.Empty;

                string title = DpuLuaSectionTitleBuilder.BuildMethodTitle(idx, name);
                sections.Add((title, code));
            }
        }

        if (root.TryGetProperty("events", out JsonElement eventsElement) && eventsElement.ValueKind == JsonValueKind.Array)
        {
            int idx = 0;
            foreach (JsonElement item in eventsElement.EnumerateArray())
            {
                idx++;
                if (item.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryGetString(item, "code", out string? code) || string.IsNullOrEmpty(code))
                {
                    continue;
                }

                string name = TryGetString(item, "name", out string? eventName) && !string.IsNullOrWhiteSpace(eventName)
                    ? eventName
                    : string.Empty;

                string title = DpuLuaSectionTitleBuilder.BuildEventTitle(idx, name);
                sections.Add((title, code));
            }
        }

        return sections;
    }

    private static Dictionary<string, string> ExtractSlotNameMap(JsonElement root)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("slots", out JsonElement slotsElement) ||
            slotsElement.ValueKind != JsonValueKind.Object)
        {
            return map;
        }

        foreach (JsonProperty slotProperty in slotsElement.EnumerateObject())
        {
            if (slotProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (TryGetString(slotProperty.Value, "name", out string? slotName) &&
                !string.IsNullOrWhiteSpace(slotName))
            {
                map[slotProperty.Name] = slotName.Trim();
            }
        }

        return map;
    }

    private static string BuildCombinedLua(IReadOnlyList<(string Name, string Code)> sections)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < sections.Count; i++)
        {
            (string name, string code) = sections[i];
            sb.Append("-- ===== ");
            sb.Append((i + 1).ToString("000", CultureInfo.InvariantCulture));
            sb.Append(' ');
            sb.Append(name);
            sb.AppendLine(" =====");
            sb.AppendLine(code.TrimEnd());
            sb.AppendLine();
        }

        return sb.ToString().TrimEnd();
    }

    private static string GetHandlerSignature(JsonElement item)
    {
        if (item.TryGetProperty("filter", out JsonElement filterElement) &&
            filterElement.ValueKind == JsonValueKind.Object)
        {
            string signature = GetStringValue(filterElement, "signature");
            if (!string.IsNullOrWhiteSpace(signature))
            {
                return signature;
            }
        }

        return GetStringValue(item, "signature");
    }

    private static string GetHandlerSlotKey(JsonElement item)
    {
        if (item.TryGetProperty("filter", out JsonElement filterElement) &&
            filterElement.ValueKind == JsonValueKind.Object)
        {
            string slotKey = GetStringValue(filterElement, "slotKey");
            if (!string.IsNullOrWhiteSpace(slotKey))
            {
                return slotKey;
            }
        }

        return GetStringValue(item, "slotKey");
    }

    private static IReadOnlyList<string> GetHandlerFilterArgs(JsonElement item)
    {
        var args = new List<string>();
        if (!item.TryGetProperty("filter", out JsonElement filterElement) ||
            filterElement.ValueKind != JsonValueKind.Object ||
            !filterElement.TryGetProperty("args", out JsonElement argsElement) ||
            argsElement.ValueKind != JsonValueKind.Array)
        {
            return args;
        }

        foreach (JsonElement argElement in argsElement.EnumerateArray())
        {
            if (argElement.ValueKind == JsonValueKind.Object)
            {
                string value = GetStringValue(argElement, "value");
                if (string.IsNullOrWhiteSpace(value))
                {
                    value = GetStringValue(argElement, "variable");
                }

                args.Add(value);
                continue;
            }

            args.Add(argElement.ToString());
        }

        return args;
    }

    private static string GetStringValue(JsonElement obj, string name)
    {
        if (!obj.TryGetProperty(name, out JsonElement element))
        {
            return string.Empty;
        }

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.ToString(),
            JsonValueKind.True => bool.TrueString,
            JsonValueKind.False => bool.FalseString,
            _ => string.Empty
        };
    }

    private static bool TryGetString(JsonElement obj, string name, out string? value)
    {
        value = null;
        if (!obj.TryGetProperty(name, out JsonElement element))
        {
            return false;
        }

        if (element.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        value = element.GetString();
        return true;
    }
}
