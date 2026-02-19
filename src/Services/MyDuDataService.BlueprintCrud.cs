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
    public async Task<IReadOnlyList<BlueprintDbRecord>> GetBlueprintsAsync(
        DataConnectionOptions options,
        string? nameFilter,
        ulong? creatorPlayerId,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT b.id, b.name, b.creator_id, b.created_at, b.free_deploy, b.max_use, b.has_materials,
                   COUNT(e.id)::int AS element_count
            FROM blueprint b
            LEFT JOIN element e ON e.blueprint_id = b.id
            WHERE (@namePattern IS NULL OR b.name ILIKE @namePattern)
              AND (@creatorId IS NULL OR b.creator_id = @creatorId)
            GROUP BY b.id, b.name, b.creator_id, b.created_at, b.free_deploy, b.max_use, b.has_materials
            ORDER BY b.id DESC;
            """;

        NpgsqlCommand cmd = new(sql, connection);
        string? namePattern = string.IsNullOrWhiteSpace(nameFilter)
            ? null
            : BuildSqlLikePattern(nameFilter);
        cmd.Parameters.Add(new NpgsqlParameter("namePattern", NpgsqlTypes.NpgsqlDbType.Text)
        {
            Value = namePattern is null ? DBNull.Value : namePattern
        });
        cmd.Parameters.Add(new NpgsqlParameter("creatorId", NpgsqlTypes.NpgsqlDbType.Bigint)
        {
            Value = creatorPlayerId.HasValue ? (object)(long)creatorPlayerId.Value : DBNull.Value
        });

        var records = new List<BlueprintDbRecord>();
        await using (cmd)
        {
            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                ulong id = TryGetUInt64(reader, 0) ?? 0UL;
                string name = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                ulong? creatorId = TryGetUInt64(reader, 2);
                DateTime? createdAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
                bool freeDeploy = !reader.IsDBNull(4) && reader.GetBoolean(4);
                long? maxUse = reader.IsDBNull(5) ? null : reader.GetInt64(5);
                bool hasMaterials = !reader.IsDBNull(6) && reader.GetBoolean(6);
                int elementCount = reader.IsDBNull(7) ? 0 : reader.GetInt32(7);
                records.Add(new BlueprintDbRecord(id, name, creatorId, createdAt, freeDeploy, maxUse, hasMaterials, elementCount));
            }
        }

        return records;
    }

    public async Task<BlueprintDeleteResult> DeleteBlueprintAsync(
        DataConnectionOptions options,
        ulong blueprintId,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        int deletedElementPropertyRows = 0;
        int deletedElementRows = 0;
        int deletedBlueprintRows;

        bool hasElementTable = await TableExistsAsync(connection, "element", cancellationToken);
        bool hasElementPropertyTable = await TableExistsAsync(connection, "element_property", cancellationToken);
        if (hasElementTable && hasElementPropertyTable)
        {
            const string deleteElementPropertySql = """
                DELETE FROM element_property ep
                USING element e
                WHERE ep.element_id = e.id
                  AND e.blueprint_id = @id;
                """;
            await using var deleteElementPropertyCmd = new NpgsqlCommand(deleteElementPropertySql, connection, transaction);
            deleteElementPropertyCmd.Parameters.AddWithValue("id", (long)blueprintId);
            deletedElementPropertyRows = await deleteElementPropertyCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        if (hasElementTable)
        {
            const string deleteElementSql = "DELETE FROM element WHERE blueprint_id = @id;";
            await using var deleteElementCmd = new NpgsqlCommand(deleteElementSql, connection, transaction);
            deleteElementCmd.Parameters.AddWithValue("id", (long)blueprintId);
            deletedElementRows = await deleteElementCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        const string deleteBlueprintSql = "DELETE FROM blueprint WHERE id = @id;";
        await using (var deleteBlueprintCmd = new NpgsqlCommand(deleteBlueprintSql, connection, transaction))
        {
            deleteBlueprintCmd.Parameters.AddWithValue("id", (long)blueprintId);
            deletedBlueprintRows = await deleteBlueprintCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);

        bool voxelCleanupAttempted = false;
        bool voxelCleanupSucceeded = false;
        string voxelCleanupNote = string.Empty;
        if (deletedBlueprintRows > 0)
        {
            (voxelCleanupAttempted, voxelCleanupSucceeded, voxelCleanupNote) = await TryClearBlueprintVoxelDataAsync(
                endpointTemplate,
                blueprintImportEndpoint,
                blueprintId,
                cancellationToken);
        }

        return new BlueprintDeleteResult(
            blueprintId,
            deletedBlueprintRows,
            deletedElementRows,
            deletedElementPropertyRows,
            voxelCleanupAttempted,
            voxelCleanupSucceeded,
            voxelCleanupNote);
    }

    public async Task<BlueprintCopyResult> CopyBlueprintAsync(
        DataConnectionOptions options,
        ulong sourceBlueprintId,
        string newName,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        IReadOnlyList<string> blueprintColumns = await GetTableColumnNamesAsync(
            connection,
            transaction,
            "blueprint",
            cancellationToken);
        List<string> copyColumns = blueprintColumns
            .Where(static column => !IsIdColumn(column))
            .ToList();

        if (copyColumns.Count == 0)
        {
            throw new InvalidOperationException("Cannot copy blueprint row because no writable columns were discovered.");
        }

        bool hasNameColumn = copyColumns.Any(column => string.Equals(column, "name", StringComparison.OrdinalIgnoreCase));
        if (!hasNameColumn)
        {
            throw new InvalidOperationException("Cannot copy blueprint row because column 'name' is missing.");
        }

        string insertColumnsSql = string.Join(", ", copyColumns.Select(QuoteIdentifier));
        string selectExpressionsSql = string.Join(", ", copyColumns.Select(column =>
            string.Equals(column, "name", StringComparison.OrdinalIgnoreCase)
                ? "@newName"
                : $"b.{QuoteIdentifier(column)}"));

        string copyBlueprintSql = $"""
            INSERT INTO blueprint ({insertColumnsSql})
            SELECT {selectExpressionsSql}
            FROM blueprint b
            WHERE b.id = @sourceId
            RETURNING id;
            """;

        ulong? copiedBlueprintId = null;
        await using (var copyBlueprintCmd = new NpgsqlCommand(copyBlueprintSql, connection, transaction))
        {
            copyBlueprintCmd.Parameters.AddWithValue("sourceId", (long)sourceBlueprintId);
            copyBlueprintCmd.Parameters.AddWithValue("newName", newName);
            object? scalar = await copyBlueprintCmd.ExecuteScalarAsync(cancellationToken);
            if (scalar is not null && scalar != DBNull.Value)
            {
                copiedBlueprintId = Convert.ToUInt64(scalar, CultureInfo.InvariantCulture);
            }
        }

        if (!copiedBlueprintId.HasValue || copiedBlueprintId.Value == 0UL)
        {
            await transaction.CommitAsync(cancellationToken);
            return new BlueprintCopyResult(
                sourceBlueprintId,
                null,
                0,
                0,
                0,
                "Source blueprint not found.");
        }

        (int copiedElementRows, int copiedElementPropertyRows, string copyNote) =
            await CopyBlueprintElementsAndPropertiesAsync(
                connection,
                transaction,
                sourceBlueprintId,
                copiedBlueprintId.Value,
                cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return new BlueprintCopyResult(
            sourceBlueprintId,
            copiedBlueprintId.Value,
            1,
            copiedElementRows,
            copiedElementPropertyRows,
            copyNote);
    }

    public async Task<BlueprintUpdateResult> UpdateBlueprintFieldsAsync(
        DataConnectionOptions options,
        ulong blueprintId,
        string name,
        bool freeDeploy,
        bool hasMaterials,
        bool updateMaxUse,
        long? maxUse,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        string sql = updateMaxUse
            ? """
            UPDATE blueprint
            SET name = @name, free_deploy = @freeDeploy, has_materials = @hasMaterials, max_use = @maxUse
            WHERE id = @id;
            """
            : """
            UPDATE blueprint
            SET name = @name, free_deploy = @freeDeploy, has_materials = @hasMaterials
            WHERE id = @id;
            """;
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("freeDeploy", freeDeploy);
        cmd.Parameters.AddWithValue("hasMaterials", hasMaterials);
        if (updateMaxUse)
        {
            cmd.Parameters.Add(new NpgsqlParameter("maxUse", NpgsqlTypes.NpgsqlDbType.Bigint) { Value = maxUse.HasValue ? (object)maxUse.Value : DBNull.Value });
        }

        cmd.Parameters.AddWithValue("id", (long)blueprintId);
        int rowsUpdated = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return new BlueprintUpdateResult(blueprintId, rowsUpdated);
    }

    public async Task<string> ExportBlueprintJsonAsync(
        DataConnectionOptions options,
        ulong blueprintId,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        bool excludeVoxels,
        bool excludeElementsAndLinks,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        JsonObject blueprintObject = await QuerySingleBlueprintRowAsync(connection, blueprintId, cancellationToken);
        var result = new JsonObject
        {
            ["Blueprint"] = blueprintObject
        };

        var notes = new List<string>();
        var optionsObject = new JsonObject
        {
            ["excludeVoxels"] = excludeVoxels,
            ["excludeElementsLinks"] = excludeElementsAndLinks
        };
        result["ExportOptions"] = optionsObject;
        result["ExportedAtUtc"] = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        if (!excludeElementsAndLinks)
        {
            if (await TableExistsAsync(connection, "element", cancellationToken) &&
                await TableHasColumnAsync(connection, "element", "blueprint_id", cancellationToken))
            {
                result["Elements"] = await QueryTableRowsByBlueprintIdAsync(connection, "element", blueprintId, cancellationToken);
            }
            else
            {
                notes.Add("Elements not exported: table/column missing.");
            }

            if (await TableExistsAsync(connection, "element_link", cancellationToken) &&
                await TableHasColumnAsync(connection, "element_link", "blueprint_id", cancellationToken))
            {
                result["Links"] = await QueryTableRowsByBlueprintIdAsync(connection, "element_link", blueprintId, cancellationToken);
            }
            else
            {
                notes.Add("Links not exported: table/column missing.");
            }
        }

        if (!excludeVoxels)
        {
            (JsonNode? voxelData, string note) = await TryExportBlueprintVoxelDataAsync(
                endpointTemplate,
                blueprintImportEndpoint,
                blueprintId,
                cancellationToken);
            if (voxelData is not null)
            {
                result["VoxelData"] = voxelData;
            }

            if (!string.IsNullOrWhiteSpace(note))
            {
                notes.Add(note);
            }
        }

        if (notes.Count > 0)
        {
            var notesArray = new JsonArray();
            foreach (string note in notes)
            {
                notesArray.Add(note);
            }

            result["ExportNotes"] = notesArray;
        }

        return result.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    public async Task<IReadOnlyList<ElementPropertyRecord>> GetBlueprintElementPropertiesAsync(
        DataConnectionOptions options,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        if (blueprintId == 0UL)
        {
            return Array.Empty<ElementPropertyRecord>();
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        return await QueryBlueprintElementPropertiesAsync(
            connection,
            blueprintId,
            options.ServerRootPath,
            cancellationToken);
    }

    private static async Task<JsonObject> QuerySingleBlueprintRowAsync(
        NpgsqlConnection connection,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT * FROM blueprint WHERE id = @id;";
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("id", (long)blueprintId);
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Blueprint {blueprintId} was not found.");
        }

        return ReadCurrentRowAsJsonObject(reader);
    }

    private static async Task<JsonArray> QueryTableRowsByBlueprintIdAsync(
        NpgsqlConnection connection,
        string tableName,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        string sql = $"SELECT * FROM {QuoteIdentifier(tableName)} WHERE blueprint_id = @id;";
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("id", (long)blueprintId);
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var result = new JsonArray();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadCurrentRowAsJsonObject(reader));
        }

        return result;
    }

    private async Task<(JsonNode? VoxelData, string Note)> TryExportBlueprintVoxelDataAsync(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        var gameplayCandidates = new List<Uri>();
        try
        {
            gameplayCandidates.AddRange(BuildBlueprintImportEndpointCandidates(
                endpointTemplate,
                blueprintImportEndpoint,
                2UL,
                0UL));
        }
        catch
        {
            gameplayCandidates.Add(new Uri("http://127.0.0.1:10111/blueprint/import", UriKind.Absolute));
            gameplayCandidates.Add(new Uri("http://localhost:10111/blueprint/import", UriKind.Absolute));
            gameplayCandidates.Add(new Uri("http://[::1]:10111/blueprint/import", UriKind.Absolute));
        }

        var dumpCandidates = new List<Uri>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri gameplayCandidate in gameplayCandidates)
        {
            IReadOnlyList<Uri> voxelJsonImportEndpoints = BuildVoxelServiceJsonImportEndpointCandidates(
                gameplayCandidate,
                blueprintId,
                clearExistingCells: false);
            foreach (Uri voxelJsonImportEndpoint in voxelJsonImportEndpoints)
            {
                Uri dumpEndpoint = BuildVoxelServiceDumpEndpoint(voxelJsonImportEndpoint, blueprintId);
                if (seen.Add(dumpEndpoint.AbsoluteUri))
                {
                    dumpCandidates.Add(dumpEndpoint);
                }
            }
        }

        var failures = new List<string>();
        foreach (Uri dumpEndpoint in dumpCandidates)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, dumpEndpoint)
                {
                    Version = HttpVersion.Version11,
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
                };
                using HttpResponseMessage response = await _httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseContentRead,
                    cancellationToken);
                byte[] dumpBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                string dumpText = Encoding.UTF8.GetString(dumpBytes);
                if (!response.IsSuccessStatusCode)
                {
                    failures.Add(
                        $"'{dumpEndpoint}' => HTTP {(int)response.StatusCode} {response.StatusCode}, body={BuildHttpBodyPreview(dumpText)}");
                    continue;
                }

                JsonNode? parsed = JsonNode.Parse(dumpText);
                if (parsed is JsonObject dumpObject &&
                    TryGetJsonPropertyIgnoreCase(dumpObject, "cells", out _, out JsonNode? cellsNode))
                {
                    return (cellsNode?.DeepClone(), $"Voxel dump exported from '{dumpEndpoint}'.");
                }

                return (parsed?.DeepClone(), $"Voxel dump exported from '{dumpEndpoint}' (no 'cells' key found).");
            }
            catch (Exception ex)
            {
                failures.Add($"'{dumpEndpoint}' => {BuildSingleLineExceptionPreview(ex)}");
            }
        }

        string note = failures.Count == 0
            ? "Voxel export skipped: no dump endpoint candidate available."
            : $"Voxel export unavailable: {string.Join(" | ", failures)}";
        return (null, note);
    }

    private static bool TryConvertToJsonValueNode(object value, out JsonNode? node)
    {
        switch (value)
        {
            case bool boolValue:
                node = JsonValue.Create(boolValue);
                return true;
            case byte byteValue:
                node = JsonValue.Create((int)byteValue);
                return true;
            case sbyte sbyteValue:
                node = JsonValue.Create((int)sbyteValue);
                return true;
            case short shortValue:
                node = JsonValue.Create((int)shortValue);
                return true;
            case ushort ushortValue:
                node = JsonValue.Create((int)ushortValue);
                return true;
            case int intValue:
                node = JsonValue.Create(intValue);
                return true;
            case uint uintValue:
                node = JsonValue.Create((long)uintValue);
                return true;
            case long longValue:
                node = JsonValue.Create(longValue);
                return true;
            case ulong ulongValue:
                node = JsonValue.Create(ulongValue.ToString(CultureInfo.InvariantCulture));
                return true;
            case float floatValue:
                node = JsonValue.Create(floatValue);
                return true;
            case double doubleValue:
                node = JsonValue.Create(doubleValue);
                return true;
            case decimal decimalValue:
                node = JsonValue.Create(decimalValue);
                return true;
            case string textValue:
                node = JsonValue.Create(textValue);
                return true;
            case DateTime dt:
                node = JsonValue.Create(dt.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case DateTimeOffset dto:
                node = JsonValue.Create(dto.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case Guid guid:
                node = JsonValue.Create(guid.ToString("D", CultureInfo.InvariantCulture));
                return true;
            case byte[] bytes:
                node = JsonValue.Create(Convert.ToBase64String(bytes));
                return true;
            default:
                node = null;
                return false;
        }
    }

    private static JsonObject ReadCurrentRowAsJsonObject(NpgsqlDataReader reader)
    {
        var row = new JsonObject();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            string columnName = reader.GetName(i);
            if (reader.IsDBNull(i))
            {
                row[columnName] = null;
                continue;
            }

            object value = reader.GetValue(i);
            if (TryConvertToJsonValueNode(value, out JsonNode? node))
            {
                row[columnName] = node;
                continue;
            }

            try
            {
                row[columnName] = System.Text.Json.JsonSerializer.SerializeToNode(value);
            }
            catch
            {
                row[columnName] = JsonValue.Create(Convert.ToString(value, CultureInfo.InvariantCulture));
            }
        }

        return row;
    }

    private static async Task<bool> TableHasColumnAsync(
        NpgsqlConnection connection,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = @tableName
                  AND column_name = @columnName
            );
            """;
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("tableName", tableName);
        cmd.Parameters.AddWithValue("columnName", columnName);
        object? scalar = await cmd.ExecuteScalarAsync(cancellationToken);
        return scalar is bool exists && exists;
    }

    private async Task<(bool Attempted, bool Succeeded, string Note)> TryClearBlueprintVoxelDataAsync(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        var gameplayCandidates = new List<Uri>();
        try
        {
            gameplayCandidates.AddRange(BuildBlueprintImportEndpointCandidates(
                endpointTemplate,
                blueprintImportEndpoint,
                2UL,
                0UL));
        }
        catch
        {
            gameplayCandidates.Add(new Uri("http://127.0.0.1:10111/blueprint/import", UriKind.Absolute));
            gameplayCandidates.Add(new Uri("http://localhost:10111/blueprint/import", UriKind.Absolute));
            gameplayCandidates.Add(new Uri("http://[::1]:10111/blueprint/import", UriKind.Absolute));
        }

        if (gameplayCandidates.Count == 0)
        {
            return (false, false, "Voxel cleanup skipped: no gameplay endpoint candidate available.");
        }

        var voxelCandidates = new List<Uri>();
        foreach (Uri gameplayCandidate in gameplayCandidates)
        {
            voxelCandidates.AddRange(BuildVoxelServiceJsonImportEndpointCandidates(
                gameplayCandidate,
                blueprintId,
                clearExistingCells: true));
        }

        var deduplicatedVoxelCandidates = new List<Uri>(voxelCandidates.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri voxelCandidate in voxelCandidates)
        {
            if (seen.Add(voxelCandidate.AbsoluteUri))
            {
                deduplicatedVoxelCandidates.Add(voxelCandidate);
            }
        }

        if (deduplicatedVoxelCandidates.Count == 0)
        {
            return (false, false, "Voxel cleanup skipped: no voxel endpoint candidate available.");
        }

        byte[] clearPayload = Encoding.UTF8.GetBytes("""{"pipeline":null,"cells":[]}""");
        var failures = new List<string>();
        foreach (Uri voxelEndpoint in deduplicatedVoxelCandidates)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, voxelEndpoint)
                {
                    Version = HttpVersion.Version11,
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
                    Content = new ByteArrayContent(clearPayload)
                };
                request.Content.Headers.ContentType =
                    new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

                using HttpResponseMessage response = await _httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseContentRead,
                    cancellationToken);

                byte[] responseBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                string responsePreview = BuildImportResponsePreview(
                    responseBytes,
                    response.Content.Headers.ContentType?.MediaType);

                if (!response.IsSuccessStatusCode)
                {
                    failures.Add(
                        $"'{voxelEndpoint}' => HTTP {(int)response.StatusCode} {response.StatusCode}, body={BuildHttpBodyPreview(responsePreview)}");
                    continue;
                }

                int dumpCellCount = await VerifyVoxelBlueprintDumpCellCountAsync(
                    voxelEndpoint,
                    blueprintId,
                    cancellationToken);
                return (
                    true,
                    dumpCellCount == 0,
                    dumpCellCount == 0
                        ? $"Voxel cleanup verified via '{voxelEndpoint}' (dump cells=0)."
                        : $"Voxel cleanup request sent via '{voxelEndpoint}', but dump still reports {dumpCellCount.ToString(CultureInfo.InvariantCulture)} cell(s).");
            }
            catch (Exception ex)
            {
                failures.Add($"'{voxelEndpoint}' => {BuildSingleLineExceptionPreview(ex)}");
            }
        }

        return (
            true,
            false,
            $"Voxel cleanup failed on all candidates: {string.Join(" | ", failures)}");
    }

    private async Task<(int ElementRowsCopied, int ElementPropertyRowsCopied, string CopyNote)> CopyBlueprintElementsAndPropertiesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ulong sourceBlueprintId,
        ulong targetBlueprintId,
        CancellationToken cancellationToken)
    {
        if (!await TableExistsAsync(connection, "element", cancellationToken))
        {
            return (0, 0, "Element copy skipped: table 'element' was not found.");
        }

        IReadOnlyList<string> elementColumns = await GetTableColumnNamesAsync(
            connection,
            transaction,
            "element",
            cancellationToken);

        bool hasElementIdColumn = elementColumns.Any(static column => IsIdColumn(column));
        bool hasElementBlueprintIdColumn = elementColumns.Any(
            column => string.Equals(column, "blueprint_id", StringComparison.OrdinalIgnoreCase));
        if (!hasElementBlueprintIdColumn)
        {
            return (0, 0, "Element copy skipped: column 'blueprint_id' missing on table 'element'.");
        }

        if (!hasElementIdColumn)
        {
            return (0, 0, "Element copy skipped: column 'id' missing on table 'element'.");
        }

        string? elementIdSequence = await GetSerialSequenceNameAsync(
            connection,
            transaction,
            "element",
            "id",
            cancellationToken);
        if (string.IsNullOrWhiteSpace(elementIdSequence))
        {
            return (0, 0, "Element copy skipped: no serial/identity sequence found for element.id.");
        }

        const string createMapSql = """
            CREATE TEMP TABLE tmp_blueprint_element_copy_map (
                old_id bigint PRIMARY KEY,
                new_id bigint NOT NULL
            ) ON COMMIT DROP;
            """;
        await using (var createMapCmd = new NpgsqlCommand(createMapSql, connection, transaction))
        {
            await createMapCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        const string seedMapSql = """
            INSERT INTO tmp_blueprint_element_copy_map (old_id, new_id)
            SELECT e.id, nextval(@sequenceName)
            FROM element e
            WHERE e.blueprint_id = @sourceBlueprintId
            ORDER BY e.id;
            """;
        await using (var seedMapCmd = new NpgsqlCommand(seedMapSql, connection, transaction))
        {
            seedMapCmd.Parameters.AddWithValue("sequenceName", elementIdSequence);
            seedMapCmd.Parameters.AddWithValue("sourceBlueprintId", (long)sourceBlueprintId);
            await seedMapCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        string elementInsertColumnsSql = string.Join(", ", elementColumns.Select(QuoteIdentifier));
        string elementSelectExpressionsSql = string.Join(", ", elementColumns.Select(column =>
        {
            if (IsIdColumn(column))
            {
                return "m.new_id";
            }

            if (string.Equals(column, "blueprint_id", StringComparison.OrdinalIgnoreCase))
            {
                return "@targetBlueprintId";
            }

            return $"e.{QuoteIdentifier(column)}";
        }));

        string copyElementSql = $"""
            INSERT INTO element ({elementInsertColumnsSql})
            SELECT {elementSelectExpressionsSql}
            FROM element e
            JOIN tmp_blueprint_element_copy_map m ON m.old_id = e.id
            WHERE e.blueprint_id = @sourceBlueprintId;
            """;
        int copiedElementRows;
        await using (var copyElementCmd = new NpgsqlCommand(copyElementSql, connection, transaction))
        {
            copyElementCmd.Parameters.AddWithValue("sourceBlueprintId", (long)sourceBlueprintId);
            copyElementCmd.Parameters.AddWithValue("targetBlueprintId", (long)targetBlueprintId);
            copiedElementRows = await copyElementCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        if (!await TableExistsAsync(connection, "element_property", cancellationToken))
        {
            return (copiedElementRows, 0, "Property copy skipped: table 'element_property' was not found.");
        }

        IReadOnlyList<string> propertyColumns = await GetTableColumnNamesAsync(
            connection,
            transaction,
            "element_property",
            cancellationToken);
        bool hasElementIdReferenceColumn = propertyColumns.Any(
            column => string.Equals(column, "element_id", StringComparison.OrdinalIgnoreCase));
        if (!hasElementIdReferenceColumn)
        {
            return (copiedElementRows, 0, "Property copy skipped: column 'element_id' missing on table 'element_property'.");
        }

        List<string> propertyInsertColumns = propertyColumns
            .Where(static column => !IsIdColumn(column))
            .ToList();
        if (propertyInsertColumns.Count == 0)
        {
            return (copiedElementRows, 0, "Property copy skipped: no writable columns found on table 'element_property'.");
        }

        string propertyInsertColumnsSql = string.Join(", ", propertyInsertColumns.Select(QuoteIdentifier));
        string propertySelectExpressionsSql = string.Join(", ", propertyInsertColumns.Select(column =>
            string.Equals(column, "element_id", StringComparison.OrdinalIgnoreCase)
                ? "m.new_id"
                : $"ep.{QuoteIdentifier(column)}"));

        string copyPropertiesSql = $"""
            INSERT INTO element_property ({propertyInsertColumnsSql})
            SELECT {propertySelectExpressionsSql}
            FROM element_property ep
            JOIN tmp_blueprint_element_copy_map m ON m.old_id = ep.element_id;
            """;

        int copiedPropertyRows;
        await using (var copyPropertiesCmd = new NpgsqlCommand(copyPropertiesSql, connection, transaction))
        {
            copiedPropertyRows = await copyPropertiesCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return (copiedElementRows, copiedPropertyRows, string.Empty);
    }

    private static async Task<IReadOnlyList<string>> GetTableColumnNamesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = @tableName
              AND n.nspname = current_schema()
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
            """;
        await using var cmd = new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("tableName", tableName);

        var columns = new List<string>();
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0))
            {
                continue;
            }

            string columnName = reader.GetString(0);
            if (!string.IsNullOrWhiteSpace(columnName))
            {
                columns.Add(columnName);
            }
        }

        return columns;
    }

    private static async Task<string?> GetSerialSequenceNameAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT pg_get_serial_sequence(@tableName, @columnName);";
        await using var cmd = new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddWithValue("tableName", tableName);
        cmd.Parameters.AddWithValue("columnName", columnName);
        object? scalar = await cmd.ExecuteScalarAsync(cancellationToken);
        return scalar is string sequenceName && !string.IsNullOrWhiteSpace(sequenceName)
            ? sequenceName
            : null;
    }

    private static bool IsIdColumn(string columnName)
    {
        return string.Equals(columnName, "id", StringComparison.OrdinalIgnoreCase);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
