using myDUWorkbench.Models;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.Services;

public sealed partial class MyDuDataService
{
    private static string BuildBlueprintElementDisplayName(JsonElement element, ulong elementId)
    {
        ulong? localId = TryReadUInt64(element, "localId", "local_id");
        ulong displayId = localId ?? elementId;

        string typeLabel = "BlueprintElement";
        if (TryGetPropertyIgnoreCase(element, "elementType", out JsonElement elementType) ||
            TryGetPropertyIgnoreCase(element, "type", out elementType))
        {
            string token = BuildScalarToken(elementType);
            if (!string.IsNullOrWhiteSpace(token))
            {
                typeLabel = $"type_{token}";
            }
        }

        return $"{typeLabel} [{displayId.ToString(CultureInfo.InvariantCulture)}]";
    }

    private static void AddBlueprintPropertyRecord(
        ICollection<ElementPropertyRecord> records,
        ulong elementId,
        string elementDisplayName,
        string propertyName,
        JsonElement value,
        int? propertyTypeOverride = null,
        string? serverRootPath = null)
    {
        int propertyType = propertyTypeOverride ?? InferBlueprintPropertyType(value);
        string decodedValue = RenderBlueprintJsonValue(value);
        int byteLength = Encoding.UTF8.GetByteCount(decodedValue);

        if (TryDecodeBlueprintSpecialProperty(
                propertyName,
                value,
                propertyType,
                serverRootPath,
                out string? decodedSpecialValue,
                out int rawByteLength) &&
            !string.IsNullOrWhiteSpace(decodedSpecialValue))
        {
            decodedValue = decodedSpecialValue;
            byteLength = rawByteLength > 0 ? rawByteLength : Encoding.UTF8.GetByteCount(decodedValue);
        }

        records.Add(new ElementPropertyRecord(
            elementId,
            elementDisplayName,
            propertyName,
            propertyType,
            decodedValue,
            byteLength));
    }

    private static bool TryExpandBlueprintElementProperties(
        ICollection<ElementPropertyRecord> records,
        ulong elementId,
        string elementDisplayName,
        JsonElement propertiesArray,
        string? serverRootPath)
    {
        int added = 0;
        foreach (JsonElement entry in propertiesArray.EnumerateArray())
        {
            if (!TryParseBlueprintPropertyEntry(entry, out string propertyName, out JsonElement propertyValue, out int? propertyType))
            {
                continue;
            }

            AddBlueprintPropertyRecord(
                records,
                elementId,
                elementDisplayName,
                propertyName,
                propertyValue,
                propertyType,
                serverRootPath);
            added++;
        }

        return added > 0;
    }

    private static bool TryParseBlueprintPropertyEntry(
        JsonElement entry,
        out string propertyName,
        out JsonElement propertyValue,
        out int? propertyType)
    {
        propertyName = string.Empty;
        propertyValue = default;
        propertyType = null;

        if (entry.ValueKind == JsonValueKind.Array)
        {
            JsonElement[] parts = entry.EnumerateArray().ToArray();
            if (parts.Length < 2)
            {
                return false;
            }

            propertyName = BuildScalarToken(parts[0]).Trim();
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                return false;
            }

            JsonElement payload = parts[1];
            if (payload.ValueKind == JsonValueKind.Object &&
                TryGetPropertyIgnoreCase(payload, "value", out JsonElement nestedValue))
            {
                propertyValue = nestedValue;
                propertyType = TryReadInt32(payload, "type");
            }
            else
            {
                propertyValue = payload;
            }

            return true;
        }

        if (entry.ValueKind == JsonValueKind.Object)
        {
            string? name = TryReadString(entry, "name", "key", "property");
            if (string.IsNullOrWhiteSpace(name))
            {
                return false;
            }

            propertyName = name.Trim();
            if (TryGetPropertyIgnoreCase(entry, "value", out JsonElement value))
            {
                propertyValue = value;
                propertyType = TryReadInt32(entry, "type");
                return true;
            }
        }

        return false;
    }

    private static int? TryReadInt32(JsonElement jsonObject, params string[] propertyNames)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object || propertyNames is null || propertyNames.Length == 0)
        {
            return null;
        }

        foreach (string propertyName in propertyNames)
        {
            if (string.IsNullOrWhiteSpace(propertyName) ||
                !TryGetPropertyIgnoreCase(jsonObject, propertyName, out JsonElement value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out int numeric))
            {
                return numeric;
            }

            if (value.ValueKind == JsonValueKind.String &&
                int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    private static bool TryDecodeBlueprintSpecialProperty(
        string propertyName,
        JsonElement value,
        int propertyType,
        string? serverRootPath,
        out string decodedValue,
        out int rawByteLength)
    {
        decodedValue = string.Empty;
        rawByteLength = 0;

        if (value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(propertyName))
        {
            return false;
        }

        if (!string.Equals(propertyName, "dpuyaml_6", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(propertyName, "content_2", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(propertyName, "databank", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        string rawText = value.GetString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(rawText))
        {
            return false;
        }

        if (!TryDecodeBase64(rawText.Trim(), out byte[] rawBytes))
        {
            return false;
        }

        rawByteLength = rawBytes.Length;
        string rootPath = serverRootPath ?? string.Empty;

        if (string.Equals(propertyName, "dpuyaml_6", StringComparison.OrdinalIgnoreCase))
        {
            if (DpuLuaDecoder.TryDecode(rawBytes, rootPath, out DpuLuaDecodeResult? lua, out _) && lua is not null)
            {
                decodedValue = lua.DecodedText;
                return true;
            }

            return false;
        }

        if (ContentBlobDecoder.TryDecode(rawBytes, rootPath, out ContentBlobDecodeResult? content, out _) &&
            content is not null)
        {
            decodedValue = content.DecodedText;
            return true;
        }

        return false;
    }

    private static bool TryDecodeBase64(string text, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        string trimmed = text.Trim();
        int remainder = trimmed.Length % 4;
        string padded = remainder == 0
            ? trimmed
            : trimmed + new string('=', 4 - remainder);

        try
        {
            bytes = Convert.FromBase64String(padded);
            return bytes.Length > 0;
        }
        catch
        {
            return false;
        }
    }

    private static int InferBlueprintPropertyType(JsonElement value)
    {
        return value.ValueKind switch
        {
            JsonValueKind.True or JsonValueKind.False => 1,
            JsonValueKind.Number when value.TryGetInt64(out _) => 2,
            JsonValueKind.Number => 3,
            _ => 4
        };
    }

    private static string RenderBlueprintJsonValue(JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return string.Empty;
            case JsonValueKind.True:
                return "true";
            case JsonValueKind.False:
                return "false";
            case JsonValueKind.Number:
                if (value.TryGetInt64(out long longValue))
                {
                    return longValue.ToString(CultureInfo.InvariantCulture);
                }

                if (value.TryGetDouble(out double doubleValue))
                {
                    return doubleValue.ToString("R", CultureInfo.InvariantCulture);
                }

                return value.GetRawText();
            case JsonValueKind.String:
                return value.GetString() ?? string.Empty;
            case JsonValueKind.Array:
                return RenderBlueprintArrayValue(value);
            case JsonValueKind.Object:
                return RenderBlueprintObjectValue(value);
            default:
                return value.GetRawText();
        }
    }

    private static string RenderBlueprintArrayValue(JsonElement value)
    {
        int length = value.GetArrayLength();
        if (length == 0)
        {
            return "[]";
        }

        if (length <= 8)
        {
            string serialized = System.Text.Json.JsonSerializer.Serialize(value);
            if (serialized.Length <= 1024)
            {
                return serialized;
            }
        }

        return $"array(length={length.ToString(CultureInfo.InvariantCulture)})";
    }

    private static string RenderBlueprintObjectValue(JsonElement value)
    {
        string serialized = System.Text.Json.JsonSerializer.Serialize(value);
        if (serialized.Length <= 2048)
        {
            return serialized;
        }

        string[] keys = value.EnumerateObject()
            .Select(property => property.Name)
            .Take(8)
            .ToArray();
        int keyCount = value.EnumerateObject().Count();

        string keyPreview = string.Join(", ", keys);
        string suffix = keyCount > keys.Length ? ", ..." : string.Empty;
        return $"object(keys={keyPreview}{suffix}; count={keyCount.ToString(CultureInfo.InvariantCulture)})";
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonElement jsonObject,
        string propertyName,
        out JsonElement value)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object)
        {
            value = default;
            return false;
        }

        if (jsonObject.TryGetProperty(propertyName, out value))
        {
            return true;
        }

        foreach (JsonProperty property in jsonObject.EnumerateObject())
        {
            if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static ulong? TryReadUInt64(JsonElement jsonObject, params string[] propertyNames)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object || propertyNames is null || propertyNames.Length == 0)
        {
            return null;
        }

        foreach (string propertyName in propertyNames)
        {
            if (string.IsNullOrWhiteSpace(propertyName) ||
                !TryGetPropertyIgnoreCase(jsonObject, propertyName, out JsonElement value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out ulong numeric))
            {
                return numeric;
            }

            if (value.ValueKind == JsonValueKind.String &&
                ulong.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    private static string? TryReadString(JsonElement jsonObject, params string[] propertyNames)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object || propertyNames is null || propertyNames.Length == 0)
        {
            return null;
        }

        foreach (string propertyName in propertyNames)
        {
            if (string.IsNullOrWhiteSpace(propertyName) ||
                !TryGetPropertyIgnoreCase(jsonObject, propertyName, out JsonElement value))
            {
                continue;
            }

            return value.ValueKind switch
            {
                JsonValueKind.String => value.GetString(),
                JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => value.GetRawText(),
                _ => null
            };
        }

        return null;
    }

    private static string BuildScalarToken(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Number)
        {
            return value.GetRawText();
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            return value.GetString() ?? string.Empty;
        }

        if (value.ValueKind == JsonValueKind.True || value.ValueKind == JsonValueKind.False)
        {
            return value.GetRawText();
        }

        return string.Empty;
    }

}
