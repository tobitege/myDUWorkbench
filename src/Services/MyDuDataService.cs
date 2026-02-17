// Helper Index:
// - LoadConstructSnapshotAsync: Loads construct metadata, transforms, and decoded element properties from PostgreSQL.
// - GetUserConstructsAsync: Lists user-owned constructs by core type (dynamic/static/space) with configurable sorting.
// - SearchConstructsByNameAsync: Returns construct id/name suggestions via ILIKE matching.
// - ParseBlueprintJson: Flattens blueprint JSON into grid-friendly element property records.
// - ProbeEndpointAsync: Probes construct endpoint payloads and attempts JSON/binary decoding.
using myDUWorker.Models;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorker.Services;

public sealed class MyDuDataService
{
    private static readonly string[] DefaultCoreKindFilter = { "dynamic", "static", "space" };
    private readonly HttpClient _httpClient;

    public MyDuDataService(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
    }

    public async Task<DatabaseConstructSnapshot> LoadConstructSnapshotAsync(
        DataConnectionOptions options,
        ulong? constructId,
        ulong? playerId,
        int propertyLimit,
        CancellationToken cancellationToken)
    {
        if (propertyLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(propertyLimit), "Property limit must be > 0.");
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        ulong? resolvedConstructId = constructId;
        ulong? resolvedPlayerId = null;
        string? playerName = null;
        ulong? playerConstructId = null;

        if (playerId.HasValue)
        {
            (resolvedPlayerId, playerName, playerConstructId) = await QueryPlayerAsync(
                connection,
                playerId.Value,
                cancellationToken);

            if (!resolvedConstructId.HasValue || resolvedConstructId.Value == 0UL)
            {
                resolvedConstructId = playerConstructId;
            }
        }

        if (!resolvedConstructId.HasValue || resolvedConstructId.Value == 0UL)
        {
            throw new InvalidOperationException(
                "No construct id available. Provide ConstructId or a PlayerId mapped to a construct.");
        }

        (string constructName, Vec3 position, Quat rotation) = await QueryConstructAsync(
            connection,
            resolvedConstructId.Value,
            cancellationToken);

        List<ElementPropertyRecord> properties = await QueryElementPropertiesAsync(
            connection,
            resolvedConstructId.Value,
            options.ServerRootPath,
            propertyLimit,
            cancellationToken);

        double? constructMass = TryReadDoubleProperty(properties, "construct_mass_total");
        double? currentMass = TryReadDoubleProperty(properties, "current_mass");
        double? speedFactor = TryReadDoubleProperty(properties, "speedFactor");
        Vec3? linearVelocity = TryReadVec3Property(properties, "resumeLinearVelocity");
        Vec3? angularVelocity = TryReadVec3Property(properties, "resumeAngularVelocity");

        return new DatabaseConstructSnapshot(
            resolvedConstructId.Value,
            constructName,
            position,
            rotation,
            resolvedPlayerId,
            playerName,
            playerConstructId,
            constructMass,
            currentMass,
            speedFactor,
            linearVelocity,
            angularVelocity,
            properties);
    }

    public async Task<LuaPropertyRawRecord?> GetLuaPropertyRawAsync(
        DataConnectionOptions options,
        ulong elementId,
        string propertyName,
        CancellationToken cancellationToken)
    {
        if (elementId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(elementId), "Element id must be > 0.");
        }

        if (string.IsNullOrWhiteSpace(propertyName))
        {
            throw new ArgumentException("Property name is required.", nameof(propertyName));
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT property_type, value
            FROM element_property
            WHERE element_id = @elementId
              AND name = @propertyName
            LIMIT 1;
            """;
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("elementId", (long)elementId);
        cmd.Parameters.AddWithValue("propertyName", propertyName);

        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        int propertyType = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
        byte[] value = reader.IsDBNull(1) ? Array.Empty<byte>() : (byte[])reader.GetValue(1);
        return new LuaPropertyRawRecord(elementId, propertyName, propertyType, value);
    }

    public async Task<LuaDbSaveResult> SaveLuaPropertyAsync(
        DataConnectionOptions options,
        ulong elementId,
        string propertyName,
        string editedCombinedLua,
        byte[] expectedOriginalDbValue,
        string serverRootPath,
        CancellationToken cancellationToken)
    {
        if (elementId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(elementId), "Element id must be > 0.");
        }

        if (string.IsNullOrWhiteSpace(propertyName))
        {
            throw new ArgumentException("Property name is required.", nameof(propertyName));
        }

        if (expectedOriginalDbValue is null || expectedOriginalDbValue.Length == 0)
        {
            throw new ArgumentException("Expected original DB value is required.", nameof(expectedOriginalDbValue));
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string selectSql = """
            SELECT property_type, value
            FROM element_property
            WHERE element_id = @elementId
              AND name = @propertyName
            FOR UPDATE;
            """;
        await using var selectCommand = new NpgsqlCommand(selectSql, connection, transaction);
        selectCommand.Parameters.AddWithValue("elementId", (long)elementId);
        selectCommand.Parameters.AddWithValue("propertyName", propertyName);

        int propertyType;
        byte[] currentDbValue;
        await using (NpgsqlDataReader reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"Property {propertyName} for element {elementId} was not found.");
            }

            propertyType = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            currentDbValue = reader.IsDBNull(1) ? Array.Empty<byte>() : (byte[])reader.GetValue(1);
            if (await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"Duplicate rows found for element {elementId} and property {propertyName}.");
            }
        }

        if (!currentDbValue.SequenceEqual(expectedOriginalDbValue))
        {
            throw new InvalidOperationException("DB value changed since editor opened. Reload LUA blocks before saving to DB.");
        }

        if (!DpuLuaEditorCodec.TryReencodeCombinedLua(
                currentDbValue,
                propertyType,
                serverRootPath,
                editedCombinedLua ?? string.Empty,
                out DpuLuaReencodeResult? encoded,
                out string? encodeError) ||
            encoded is null)
        {
            throw new InvalidOperationException($"Lua re-encode failed: {encodeError ?? "unknown error"}");
        }

        if (!DpuLuaEditorCodec.TryBuildCombinedLuaFromDbValue(
                encoded.DbValue,
                serverRootPath,
                out _,
                out int verifiedSections,
                out string? verifyError))
        {
            throw new InvalidOperationException($"Post-encode verification failed: {verifyError ?? "unknown error"}");
        }

        const string updateSql = """
            UPDATE element_property
            SET value = @newValue,
                property_type = @propertyType
            WHERE element_id = @elementId
              AND name = @propertyName;
            """;
        await using var updateCommand = new NpgsqlCommand(updateSql, connection, transaction);
        updateCommand.Parameters.AddWithValue("newValue", encoded.DbValue);
        updateCommand.Parameters.AddWithValue("propertyType", propertyType);
        updateCommand.Parameters.AddWithValue("elementId", (long)elementId);
        updateCommand.Parameters.AddWithValue("propertyName", propertyName);
        int affected = await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        if (affected != 1)
        {
            throw new InvalidOperationException($"Expected exactly one updated row, got {affected}.");
        }

        await transaction.CommitAsync(cancellationToken);
        return new LuaDbSaveResult(
            encoded.DbValue,
            propertyType,
            encoded.UsesHashReference,
            encoded.HashReference,
            verifiedSections);
    }

    public async Task<DestroyedRepairResult> RepairDestroyedPropertiesAsync(
        DataConnectionOptions options,
        ulong constructId,
        IProgress<DestroyedRepairProgress>? progress,
        CancellationToken cancellationToken)
    {
        if (constructId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(constructId), "Construct id must be > 0.");
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string selectSql = """
            SELECT ep.element_id, ep.name
            FROM element_property ep
            JOIN element e ON e.id = ep.element_id
            WHERE e.construct_id = @constructId
              AND lower(ep.name) IN ('destroyed', 'restorecount')
            ORDER BY ep.element_id, ep.name;
            """;
        await using var selectCommand = new NpgsqlCommand(selectSql, connection, transaction);
        selectCommand.Parameters.AddWithValue("constructId", (long)constructId);

        var rows = new List<(ulong ElementId, string PropertyName)>();
        await using (NpgsqlDataReader reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                ulong elementId = TryGetUInt64(reader, 0) ?? 0UL;
                string propertyName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                rows.Add((elementId, propertyName));
            }
        }

        int total = rows.Count;
        if (total == 0)
        {
            progress?.Report(new DestroyedRepairProgress(0, 0));
            await transaction.CommitAsync(cancellationToken);
            return new DestroyedRepairResult(0, 0);
        }

        const string deleteSql = """
            DELETE FROM element_property
            WHERE element_id = @elementId
              AND lower(name) = @propertyName;
            """;
        await using var deleteCommand = new NpgsqlCommand(deleteSql, connection, transaction);
        deleteCommand.Parameters.Add("elementId", NpgsqlTypes.NpgsqlDbType.Bigint);
        deleteCommand.Parameters.Add("propertyName", NpgsqlTypes.NpgsqlDbType.Text);

        int processed = 0;
        int updated = 0;
        foreach ((ulong elementId, string propertyName) in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string normalizedPropertyName = propertyName.Trim().ToLowerInvariant();
            deleteCommand.Parameters["elementId"].Value = (long)elementId;
            deleteCommand.Parameters["propertyName"].Value = normalizedPropertyName;
            updated += await deleteCommand.ExecuteNonQueryAsync(cancellationToken);

            processed++;
            progress?.Report(new DestroyedRepairProgress(processed, total));
        }

        await transaction.CommitAsync(cancellationToken);
        return new DestroyedRepairResult(total, updated);
    }

    public async Task<IReadOnlyList<ConstructNameLookupRecord>> SearchConstructsByNameAsync(
        DataConnectionOptions options,
        string searchInput,
        int maxRows,
        CancellationToken cancellationToken)
    {
        if (maxRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRows), "Search result limit must be > 0.");
        }

        if (string.IsNullOrWhiteSpace(searchInput))
        {
            return Array.Empty<ConstructNameLookupRecord>();
        }

        string sqlLikePattern = BuildSqlLikePattern(searchInput);

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT id, name
            FROM construct
            WHERE name ILIKE @namePattern
            ORDER BY name
            LIMIT @maxRows;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("namePattern", sqlLikePattern);
        cmd.Parameters.AddWithValue("maxRows", maxRows);

        var records = new List<ConstructNameLookupRecord>(maxRows);
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong constructId = TryGetUInt64(reader, 0) ?? 0UL;
            string constructName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            records.Add(new ConstructNameLookupRecord(constructId, constructName));
        }

        return records;
    }

    public async Task<IReadOnlyList<PlayerNameLookupRecord>> SearchPlayersByNameAsync(
        DataConnectionOptions options,
        string searchInput,
        int maxRows,
        CancellationToken cancellationToken)
    {
        if (maxRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRows), "Search result limit must be > 0.");
        }

        if (string.IsNullOrWhiteSpace(searchInput))
        {
            return Array.Empty<PlayerNameLookupRecord>();
        }

        string sqlLikePattern = BuildSqlLikePattern(searchInput);

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT id, display_name
            FROM player
            WHERE display_name ILIKE @namePattern
            ORDER BY display_name, id
            LIMIT @maxRows;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("namePattern", sqlLikePattern);
        cmd.Parameters.AddWithValue("maxRows", maxRows);

        var records = new List<PlayerNameLookupRecord>(maxRows);
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong? playerId = TryGetUInt64(reader, 0);
            string playerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            records.Add(new PlayerNameLookupRecord(playerId, playerName));
        }

        return records;
    }

    public async Task<IReadOnlyList<PlayerNameLookupRecord>> GetPlayersSortedByNameAsync(
        DataConnectionOptions options,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT id, display_name
            FROM player
            ORDER BY display_name, id;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);

        var records = new List<PlayerNameLookupRecord>();
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong? playerId = TryGetUInt64(reader, 0);
            string playerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            records.Add(new PlayerNameLookupRecord(playerId, playerName));
        }

        return records;
    }

    public Task<IReadOnlyList<UserConstructRecord>> GetUserConstructsSortedByNameAsync(
        DataConnectionOptions options,
        ulong userId,
        int maxRows,
        CancellationToken cancellationToken)
    {
        return GetUserConstructsAsync(
            options,
            userId,
            null,
            ConstructListSort.Name,
            descending: false,
            maxRows,
            cancellationToken);
    }

    public Task<IReadOnlyList<UserConstructRecord>> GetUserConstructsSortedByIdAsync(
        DataConnectionOptions options,
        ulong userId,
        int maxRows,
        CancellationToken cancellationToken)
    {
        return GetUserConstructsAsync(
            options,
            userId,
            null,
            ConstructListSort.Id,
            descending: false,
            maxRows,
            cancellationToken);
    }

    public Task<IReadOnlyList<UserConstructRecord>> GetUserConstructsAsync(
        DataConnectionOptions options,
        ulong userId,
        ConstructListSort sortBy,
        bool descending,
        int maxRows,
        CancellationToken cancellationToken)
    {
        return GetUserConstructsAsync(
            options,
            userId,
            null,
            sortBy,
            descending,
            maxRows,
            cancellationToken);
    }

    public async Task<IReadOnlyList<UserConstructRecord>> GetUserConstructsAsync(
        DataConnectionOptions options,
        ulong userId,
        IReadOnlyCollection<ConstructCoreKind>? coreKinds,
        ConstructListSort sortBy,
        bool descending,
        int maxRows,
        CancellationToken cancellationToken)
    {
        if (maxRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRows), "Construct list limit must be > 0.");
        }

        string[] coreKindFilter = BuildCoreKindFilter(coreKinds);
        string orderClause = BuildConstructOrderClause(sortBy, descending);

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        bool includeOrganizationOwned = await TableExistsAsync(
            connection,
            "public.organization_member_player",
            cancellationToken);

        string ownerScopePredicate = includeOrganizationOwned
            ? """
                (
                    o.player_id = @userId
                    OR (
                        o.organization_id IS NOT NULL
                        AND EXISTS (
                            SELECT 1
                            FROM organization_member_player omp
                            WHERE omp.organization_id = o.organization_id
                              AND omp.player_id = @userId
                        )
                    )
                )
                """
            : "o.player_id = @userId";

        string sql = $"""
            SELECT c.id,
                   c.name,
                   o.player_id,
                   o.organization_id,
                   COALESCE(core.core_kind, 'unknown') AS core_kind,
                   core.core_element_id,
                   core.core_element_type_id,
                   COALESCE(core.core_display_name, '')
            FROM construct c
            JOIN ownership o ON o.id = c.owner_entity_id
            LEFT JOIN LATERAL (
                SELECT
                    CASE
                        WHEN COALESCE(i.name, '') ILIKE '%space core%' OR COALESCE(i.yaml, '') ILIKE '%space core%' THEN 'space'
                        WHEN COALESCE(i.name, '') ILIKE '%static core%' OR COALESCE(i.yaml, '') ILIKE '%static core%' THEN 'static'
                        WHEN COALESCE(i.name, '') ILIKE '%dynamic core%' OR COALESCE(i.yaml, '') ILIKE '%dynamic core%' THEN 'dynamic'
                        ELSE 'unknown'
                    END AS core_kind,
                    e.id AS core_element_id,
                    e.element_type_id AS core_element_type_id,
                    COALESCE(
                        NULLIF(substring(i.yaml from E'displayName:\\s*([^\\n\\r]+)'), ''),
                        NULLIF(i.name, ''),
                        'type_' || e.element_type_id::text
                    ) AS core_display_name
                FROM element e
                LEFT JOIN item_definition i ON i.id = e.element_type_id
                WHERE e.construct_id = c.id
                  AND (
                      COALESCE(i.name, '') ILIKE '%dynamic core%' OR
                      COALESCE(i.name, '') ILIKE '%static core%' OR
                      COALESCE(i.name, '') ILIKE '%space core%' OR
                      COALESCE(i.yaml, '') ILIKE '%dynamic core%' OR
                      COALESCE(i.yaml, '') ILIKE '%static core%' OR
                      COALESCE(i.yaml, '') ILIKE '%space core%'
                  )
                ORDER BY e.id
                LIMIT 1
            ) core ON TRUE
            WHERE c.deleted_at IS NULL
              AND {ownerScopePredicate}
              AND COALESCE(core.core_kind, 'unknown') = ANY(@coreKinds)
            ORDER BY {orderClause}
            LIMIT @maxRows;
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("userId", (long)userId);
        cmd.Parameters.AddWithValue("coreKinds", coreKindFilter);
        cmd.Parameters.AddWithValue("maxRows", maxRows);

        var records = new List<UserConstructRecord>(maxRows);
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong constructId = TryGetUInt64(reader, 0) ?? 0UL;
            string constructName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            ulong? ownerPlayerId = TryGetUInt64(reader, 2);
            ulong? ownerOrganizationId = TryGetUInt64(reader, 3);
            string rawCoreKind = reader.IsDBNull(4) ? "unknown" : reader.GetString(4);
            ulong? coreElementId = TryGetUInt64(reader, 5);
            ulong? coreElementTypeId = TryGetUInt64(reader, 6);
            string coreElementDisplayName = reader.IsDBNull(7) ? string.Empty : reader.GetString(7);
            ConstructCoreKind parsedCoreKind = ParseCoreKind(rawCoreKind);

            records.Add(new UserConstructRecord(
                constructId,
                constructName,
                ownerPlayerId,
                ownerOrganizationId,
                parsedCoreKind,
                coreElementId,
                coreElementTypeId,
                coreElementDisplayName));
        }

        return records;
    }

    public async Task<bool> IsDatabaseAvailableAsync(
        DataConnectionOptions options,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            await using var connection = new NpgsqlConnection(BuildConnectionString(options));
            await connection.OpenAsync(timeoutCts.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public BlueprintImportResult ParseBlueprintJson(string jsonContent, string sourceName)
    {
        if (string.IsNullOrWhiteSpace(jsonContent))
        {
            throw new ArgumentException("Blueprint JSON content is empty.", nameof(jsonContent));
        }

        using JsonDocument document = JsonDocument.Parse(
            jsonContent,
            new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            });

        JsonElement root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("Blueprint root must be a JSON object.");
        }

        JsonElement model = default;
        bool hasModel = root.TryGetProperty("Model", out model) && model.ValueKind == JsonValueKind.Object;
        JsonElement elements = default;
        bool hasElements = root.TryGetProperty("Elements", out elements) && elements.ValueKind == JsonValueKind.Array;

        ulong? blueprintId = hasModel ? TryReadUInt64(model, "Id") : null;
        string blueprintName = hasModel ? TryReadString(model, "Name") ?? string.Empty : string.Empty;
        if (string.IsNullOrWhiteSpace(blueprintName))
        {
            blueprintName = string.IsNullOrWhiteSpace(sourceName) ? "Blueprint import" : sourceName;
        }

        var records = new List<ElementPropertyRecord>();
        int elementCount = 0;

        if (hasElements)
        {
            int fallbackElementId = 0;
            foreach (JsonElement element in elements.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                fallbackElementId++;
                ulong elementId = TryReadUInt64(element, "elementId") ?? (ulong)fallbackElementId;
                string elementDisplayName = BuildBlueprintElementDisplayName(element, elementId);

                foreach (JsonProperty property in element.EnumerateObject())
                {
                    AddBlueprintPropertyRecord(records, elementId, elementDisplayName, property.Name, property.Value);
                }

                elementCount++;
            }
        }

        const ulong modelPseudoElementId = 0UL;
        if (hasModel)
        {
            foreach (JsonProperty property in model.EnumerateObject())
            {
                AddBlueprintPropertyRecord(
                    records,
                    modelPseudoElementId,
                    "BlueprintModel [0]",
                    $"model.{property.Name}",
                    property.Value);
            }
        }

        foreach (JsonProperty property in root.EnumerateObject())
        {
            if (property.NameEquals("Elements") || property.NameEquals("Model"))
            {
                continue;
            }

            AddBlueprintPropertyRecord(
                records,
                modelPseudoElementId,
                "BlueprintRoot [0]",
                $"root.{property.Name}",
                property.Value);
        }

        if (records.Count == 0)
        {
            throw new InvalidOperationException("No readable element or model properties were found in blueprint JSON.");
        }

        return new BlueprintImportResult(
            sourceName ?? string.Empty,
            blueprintName,
            blueprintId,
            elementCount,
            records);
    }

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
        string normalized = searchInput.Trim().Replace('*', '%').Replace('?', '_');
        if (normalized.IndexOf('%') < 0 && normalized.IndexOf('_') < 0)
        {
            normalized += "%";
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
        ulong? localId = TryReadUInt64(element, "localId");
        ulong displayId = localId ?? elementId;

        string typeLabel = "BlueprintElement";
        if (element.TryGetProperty("elementType", out JsonElement elementType))
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
        JsonElement value)
    {
        string decodedValue = RenderBlueprintJsonValue(value);
        int propertyType = InferBlueprintPropertyType(value);
        int byteLength = Encoding.UTF8.GetByteCount(decodedValue);

        records.Add(new ElementPropertyRecord(
            elementId,
            elementDisplayName,
            propertyName,
            propertyType,
            decodedValue,
            byteLength));
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
            string serialized = JsonSerializer.Serialize(value);
            if (serialized.Length <= 1024)
            {
                return serialized;
            }
        }

        return $"array(length={length.ToString(CultureInfo.InvariantCulture)})";
    }

    private static string RenderBlueprintObjectValue(JsonElement value)
    {
        string serialized = JsonSerializer.Serialize(value);
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

    private static ulong? TryReadUInt64(JsonElement jsonObject, string propertyName)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object ||
            !jsonObject.TryGetProperty(propertyName, out JsonElement value))
        {
            return null;
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

        return null;
    }

    private static string? TryReadString(JsonElement jsonObject, string propertyName)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object ||
            !jsonObject.TryGetProperty(propertyName, out JsonElement value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => value.GetRawText(),
            _ => null
        };
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
