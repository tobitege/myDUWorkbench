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
        return await ReadElementPropertyRecordsAsync(cmd, serverRootPath, cancellationToken);
    }

    private static async Task<List<ElementPropertyRecord>> QueryBlueprintElementPropertiesAsync(
        NpgsqlConnection connection,
        ulong blueprintId,
        string serverRootPath,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT ep.element_id,
                   e.local_id,
                   COALESCE(
                       NULLIF(substring(i.yaml from E'displayName:\\s*([^\\n\\r]+)'), ''),
                       NULLIF(i.name, ''),
                       'type_' || e.element_type_id::text
                   ) AS element_display_name,
                   ep.name,
                   ep.property_type,
                   ep.value
            FROM element_property ep
            JOIN element e ON e.id = ep.element_id
            LEFT JOIN item_definition i ON i.id = e.element_type_id
            WHERE e.blueprint_id = @blueprintId
            ORDER BY ep.element_id, ep.name;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("blueprintId", (long)blueprintId);
        return await ReadElementPropertyRecordsAsync(cmd, serverRootPath, cancellationToken);
    }

    private static async Task<List<ElementPropertyRecord>> ReadElementPropertyRecordsAsync(
        NpgsqlCommand cmd,
        string serverRootPath,
        CancellationToken cancellationToken)
    {
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
                return
                    $"[dpuyaml decoded | sections={decodeResult.SectionCount} | payloadBytes={decodeResult.PayloadBytes} | decodedBytes={decodeResult.DecodedBytes}]{Environment.NewLine}{Environment.NewLine}{decodeResult.DecodedText}";
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
}
