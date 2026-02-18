using myDUWorker.Models;
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

namespace myDUWorker.Services;

public sealed partial class MyDuDataService
{
    public async Task<EndpointProbeResult> ProbeEndpointAsync(
        Uri endpointUri,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, endpointUri);
        using HttpResponseMessage response = await _httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseContentRead,
            cancellationToken);

        byte[] payload = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        string contentType = response.Content.Headers.ContentType?.MediaType ?? string.Empty;
        string notes = string.Empty;
        ConstructUpdate? constructUpdate = null;
        ConstructInfoPreamble? constructInfoPreamble = null;
        NqStructBlobHeader? blobHeader = null;

        if (payload.Length > 0)
        {
            if (LooksLikeJson(payload))
            {
                notes = "Payload looks like JSON/text.";
            }
            else
            {
                try
                {
                    var constructDeserializer = new NqBinaryDeserializer(payload);
                    constructUpdate = ConstructUpdate.Deserialize(constructDeserializer);
                    constructDeserializer.EnsureAtEnd("ConstructUpdate");
                    notes = "Binary payload decoded as ConstructUpdate.";
                }
                catch (Exception exConstruct)
                {
                    try
                    {
                        var preambleDeserializer = new NqBinaryDeserializer(payload);
                        constructInfoPreamble = ConstructInfoPreamble.Deserialize(preambleDeserializer);
                        notes =
                            "Payload matched /constructs/{id}/info preamble " +
                            "(constructId/parentId/position/rotation). " +
                            "ConstructUpdate was not used for this endpoint shape.";
                    }
                    catch (Exception exInfo)
                    {
                        try
                        {
                            var blobDeserializer = new NqBinaryDeserializer(payload);
                            blobHeader = NqStructBlobHeader.DeserializeHeader(blobDeserializer);
                            blobDeserializer.EnsureAtEnd("NQ struct blob header");
                            notes =
                                "ConstructUpdate decode failed; blob header decode succeeded. " +
                                $"Construct error: {exConstruct.Message}";
                        }
                        catch (Exception exBlob)
                        {
                            notes =
                                "Binary decode failed for ConstructUpdate, /constructs/{id}/info preamble, and NQ struct blob header. " +
                                $"Construct error: {exConstruct.Message} | Info error: {exInfo.Message} | Blob error: {exBlob.Message}";
                        }
                    }
                }
            }
        }
        else
        {
            notes = "Endpoint returned an empty payload.";
        }

        string rawPreview = BuildRawPreview(payload);

        return new EndpointProbeResult(
            endpointUri,
            (int)response.StatusCode,
            contentType,
            payload.Length,
            constructUpdate,
            constructInfoPreamble,
            blobHeader,
            rawPreview,
            notes);
    }

    private static async Task<(ulong? PlayerId, string? DisplayName, ulong? ConstructId)> QueryPlayerAsync(
        NpgsqlConnection connection,
        ulong playerId,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT id, display_name, construct_id
            FROM player
            WHERE id = @playerId
            LIMIT 1;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("playerId", (long)playerId);

        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null, null);
        }

        ulong? resolvedPlayerId = TryGetUInt64(reader, 0);
        string? displayName = reader.IsDBNull(1) ? null : reader.GetString(1);
        ulong? constructId = TryGetUInt64(reader, 2);
        return (resolvedPlayerId, displayName, constructId);
    }

    private static async Task<(string Name, Vec3 Position, Quat Rotation)> QueryConstructAsync(
        NpgsqlConnection connection,
        ulong constructId,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT name,
                   position_x, position_y, position_z,
                   rotation_x, rotation_y, rotation_z, rotation_w
            FROM construct
            WHERE id = @constructId
            LIMIT 1;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("constructId", (long)constructId);

        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Construct {constructId} was not found in table construct.");
        }

        string name = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
        Vec3 position = new Vec3(
            ReadDouble(reader, 1),
            ReadDouble(reader, 2),
            ReadDouble(reader, 3));
        Quat rotation = new Quat(
            ReadSingle(reader, 4),
            ReadSingle(reader, 5),
            ReadSingle(reader, 6),
            ReadSingle(reader, 7));

        return (name, position, rotation);
    }

    private static async Task<List<ElementPropertyRecord>> QueryElementPropertiesAsync(
        NpgsqlConnection connection,
        ulong constructId,
        string serverRootPath,
        int propertyLimit,
        CancellationToken cancellationToken)
    {
        const string sql = """
            WITH ranked AS (
                SELECT ep.element_id,
                       e.local_id,
                       COALESCE(
                           NULLIF(substring(i.yaml from E'displayName:\\s*([^\\n\\r]+)'), ''),
                           NULLIF(i.name, ''),
                           'type_' || e.element_type_id::text
                       ) AS element_display_name,
                       ep.name,
                       ep.property_type,
                       ep.value,
                       ROW_NUMBER() OVER (PARTITION BY ep.element_id ORDER BY ep.name) AS row_num
                FROM element_property ep
                JOIN element e ON e.id = ep.element_id
                LEFT JOIN item_definition i ON i.id = e.element_type_id
                WHERE e.construct_id = @constructId
            )
            SELECT element_id,
                   local_id,
                   element_display_name,
                   name,
                   property_type,
                   value
            FROM ranked
            WHERE row_num <= @propertyLimit
            ORDER BY element_id, name;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("constructId", (long)constructId);
        cmd.Parameters.AddWithValue("propertyLimit", propertyLimit);

        var records = new List<ElementPropertyRecord>();
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong elementId = TryGetUInt64(reader, 0) ?? 0UL;
            ulong? localId = TryGetUInt64(reader, 1);
            string elementDisplayName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            if (localId.HasValue)
            {
                elementDisplayName = string.IsNullOrWhiteSpace(elementDisplayName)
                    ? $"[{localId.Value}]"
                    : $"{elementDisplayName} [{localId.Value}]";
            }

            string name = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
            int propertyType = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture);
            byte[] raw = reader.IsDBNull(5) ? Array.Empty<byte>() : (byte[])reader.GetValue(5);

            string decoded = DecodePropertyValue(name, propertyType, raw, serverRootPath);
            records.Add(new ElementPropertyRecord(elementId, elementDisplayName, name, propertyType, decoded, raw.Length));
        }

        return records;
    }

    private static string DecodePropertyValue(string propertyName, int propertyType, byte[] raw, string serverRootPath)
    {
        if (raw.Length == 0)
        {
            return string.Empty;
        }

        if (propertyName.StartsWith("dpuyaml_", StringComparison.OrdinalIgnoreCase))
        {
            if (DpuLuaDecoder.TryDecode(raw, serverRootPath, out DpuLuaDecodeResult? decodeResult, out string? decodeError) &&
                decodeResult is not null)
            {
                string source = string.IsNullOrWhiteSpace(decodeResult.SourceBlobPath)
                    ? "db-value"
                    : decodeResult.SourceBlobPath;
                return
                    $"[dpuyaml decoded | sections={decodeResult.SectionCount} | payloadBytes={decodeResult.PayloadBytes} | decodedBytes={decodeResult.DecodedBytes} | source={source}]{Environment.NewLine}{Environment.NewLine}{decodeResult.DecodedText}";
            }

            if (!string.IsNullOrWhiteSpace(decodeError))
            {
                return $"[dpuyaml decode failed] {decodeError}";
            }
        }

        if (string.Equals(propertyName, "content", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(propertyName, "content_2", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(propertyName, "databank", StringComparison.OrdinalIgnoreCase))
        {
            if (ContentBlobDecoder.TryDecode(raw, serverRootPath, out ContentBlobDecodeResult? contentResult, out _) &&
                contentResult is not null)
            {
                return contentResult.DecodedText;
            }
        }

        string text = Encoding.UTF8.GetString(raw);
        string trimmed = text.Trim();

        switch (propertyType)
        {
            case 1:
                if (trimmed == "1")
                {
                    return "true";
                }

                if (trimmed == "0")
                {
                    return "false";
                }

                if (bool.TryParse(trimmed, out bool boolValue))
                {
                    return boolValue ? "true" : "false";
                }

                break;
            case 2:
                if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
                {
                    return longValue.ToString(CultureInfo.InvariantCulture);
                }

                break;
            case 3:
                if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue))
                {
                    return doubleValue.ToString("R", CultureInfo.InvariantCulture);
                }

                break;
            case 4:
                return text;
            case 5:
                if (Quat.TryParseCsv(trimmed, out Quat quat))
                {
                    return quat.ToString();
                }

                break;
            case 6:
                if (Vec3.TryParseCsv(trimmed, out Vec3 vec))
                {
                    return vec.ToString();
                }

                break;
            case 7:
                return trimmed;
        }

        if (IsMostlyPrintable(text))
        {
            return text;
        }

        return $"base64:{Convert.ToBase64String(raw)}";
    }

    private static bool IsMostlyPrintable(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return true;
        }

        int printable = value.Count(c => !char.IsControl(c) || c == '\r' || c == '\n' || c == '\t');
        return printable >= (value.Length * 8 / 10);
    }

    private static double? TryReadDoubleProperty(IReadOnlyList<ElementPropertyRecord> records, string propertyName)
    {
        ElementPropertyRecord? match = records.FirstOrDefault(p =>
            string.Equals(p.Name, propertyName, StringComparison.Ordinal));

        if (match is null)
        {
            return null;
        }

        return double.TryParse(match.DecodedValue, NumberStyles.Float, CultureInfo.InvariantCulture, out double value)
            ? value
            : null;
    }

    private static Vec3? TryReadVec3Property(IReadOnlyList<ElementPropertyRecord> records, string propertyName)
    {
        ElementPropertyRecord? match = records.FirstOrDefault(p =>
            string.Equals(p.Name, propertyName, StringComparison.Ordinal));

        if (match is null)
        {
            return null;
        }

        return Vec3.TryParseCsv(match.DecodedValue, out Vec3 vec) ? vec : null;
    }

    private static bool LooksLikeJson(IReadOnlyList<byte> payload)
    {
        if (payload.Count == 0)
        {
            return false;
        }

        byte first = payload[0];
        return first == (byte)'{' || first == (byte)'[';
    }

    private static string BuildRawPreview(byte[] payload)
    {
        if (payload.Length == 0)
        {
            return "<empty>";
        }

        if (LooksLikeJson(payload))
        {
            string text = Encoding.UTF8.GetString(payload);
            return text.Length <= 4000 ? text : text[..4000];
        }

        string utf8 = Encoding.UTF8.GetString(payload);
        if (IsMostlyPrintable(utf8))
        {
            return utf8.Length <= 4000 ? utf8 : utf8[..4000];
        }

        int count = Math.Min(128, payload.Length);
        byte[] prefix = payload.Take(count).ToArray();
        return $"hex:{Convert.ToHexString(prefix)}";
    }

    private static string BuildConnectionString(DataConnectionOptions options)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = options.Port,
            Database = options.Database,
            Username = options.Username,
            Password = options.Password,
            Timeout = 5,
            CommandTimeout = 15
        };

        return builder.ConnectionString;
    }

    private static string BuildSqlLikePattern(string searchInput)
    {
        string trimmed = searchInput.Trim();
        string normalized = trimmed.Replace('*', '%').Replace('?', '_');

        if (trimmed.IndexOf('%') >= 0 || trimmed.IndexOf('*') >= 0)
        {
            return normalized;
        }

        if (normalized.IndexOf('%') < 0 && normalized.IndexOf('_') < 0)
        {
            normalized = "%" + normalized + "%";
        }

        return normalized;
    }

    private static string[] BuildCoreKindFilter(IReadOnlyCollection<ConstructCoreKind>? coreKinds)
    {
        if (coreKinds is null || coreKinds.Count == 0)
        {
            return DefaultCoreKindFilter;
        }

        var filter = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ConstructCoreKind coreKind in coreKinds)
        {
            switch (coreKind)
            {
                case ConstructCoreKind.Dynamic:
                    filter.Add("dynamic");
                    break;
                case ConstructCoreKind.Static:
                    filter.Add("static");
                    break;
                case ConstructCoreKind.Space:
                    filter.Add("space");
                    break;
                case ConstructCoreKind.Unknown:
                    filter.Add("unknown");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(coreKinds), coreKind, "Unsupported construct core kind filter.");
            }
        }

        if (filter.Count == 0)
        {
            return DefaultCoreKindFilter;
        }

        return filter.ToArray();
    }

    private static string BuildConstructOrderClause(ConstructListSort sortBy, bool descending)
    {
        string direction = descending ? "DESC" : "ASC";
        return sortBy switch
        {
            ConstructListSort.Name => $"c.name {direction} NULLS LAST, c.id {direction}",
            ConstructListSort.Id => $"c.id {direction}, c.name {direction} NULLS LAST",
            _ => throw new ArgumentOutOfRangeException(nameof(sortBy), sortBy, "Unsupported construct sort field.")
        };
    }

    private static ConstructCoreKind ParseCoreKind(string rawCoreKind)
    {
        if (string.Equals(rawCoreKind, "dynamic", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Dynamic;
        }

        if (string.Equals(rawCoreKind, "static", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Static;
        }

        if (string.Equals(rawCoreKind, "space", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Space;
        }

        return ConstructCoreKind.Unknown;
    }

    private static async Task<bool> TableExistsAsync(
        NpgsqlConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT to_regclass(@tableName) IS NOT NULL;";
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("tableName", tableName);

        object? result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    private static ulong? TryGetUInt64(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        object value = reader.GetValue(ordinal);
        return value switch
        {
            ulong u => u,
            long l when l >= 0L => (ulong)l,
            int i when i >= 0 => (ulong)i,
            decimal d when d >= 0 => (ulong)d,
            _ => ulong.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed)
                ? parsed
                : null
        };
    }

    private static double ReadDouble(NpgsqlDataReader reader, int ordinal)
    {
        object value = reader.GetValue(ordinal);
        return Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }

    private static float ReadSingle(NpgsqlDataReader reader, int ordinal)
    {
        object value = reader.GetValue(ordinal);
        return Convert.ToSingle(value, CultureInfo.InvariantCulture);
    }

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
