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

    public async Task<IReadOnlyDictionary<ulong, string>> GetItemDefinitionDisplayNamesAsync(
        DataConnectionOptions options,
        IReadOnlyCollection<ulong> itemDefinitionIds,
        CancellationToken cancellationToken)
    {
        if (itemDefinitionIds is null || itemDefinitionIds.Count == 0)
        {
            return new Dictionary<ulong, string>();
        }

        long[] ids = itemDefinitionIds
            .Where(id => id > 0UL && id <= long.MaxValue)
            .Select(id => (long)id)
            .Distinct()
            .ToArray();
        if (ids.Length == 0)
        {
            return new Dictionary<ulong, string>();
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT id,
                   COALESCE(
                       NULLIF(substring(yaml from E'displayName:\\s*([^\\n\\r]+)'), ''),
                       NULLIF(name, ''),
                       ''
                   ) AS display_name
            FROM item_definition
            WHERE id = ANY(@ids);
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("ids", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint, ids);

        var result = new Dictionary<ulong, string>();
        await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ulong? id = TryGetUInt64(reader, 0);
            if (!id.HasValue)
            {
                continue;
            }

            string displayName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1).Trim();
            if (string.IsNullOrWhiteSpace(displayName))
            {
                continue;
            }

            result[id.Value] = displayName;
        }

        return result;
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

    public BlueprintImportResult ParseBlueprintJson(
        string jsonContent,
        string sourceName,
        string? serverRootPath = null,
        string? nqUtilsDllPath = null)
    {
        if (string.IsNullOrWhiteSpace(jsonContent))
        {
            throw new ArgumentException("Blueprint JSON content is empty.", nameof(jsonContent));
        }

        NqBlueprintProbe nqProbe = ProbeBlueprintWithNqDll(jsonContent, serverRootPath, nqUtilsDllPath);
        BlueprintImportResult projected = ParseBlueprintJsonLegacy(jsonContent, sourceName, serverRootPath);

        string importPipeline;
        string importNotes;
        if (nqProbe.Success)
        {
            importPipeline = "NQutils.dll preflight + JSON projection";
            importNotes =
                $"Validated via {nqProbe.DllPath}; elements={nqProbe.ElementCount}, links={nqProbe.LinkCount}, voxelData={(nqProbe.HasVoxelData ? "present" : "missing")}.";
        }
        else if (nqProbe.DllUnavailable)
        {
            importPipeline = "Legacy JSON projection (NQ DLL unavailable)";
            importNotes = nqProbe.Message;
        }
        else
        {
            importPipeline = "Legacy JSON projection (NQ preflight warning)";
            importNotes = $"NQ preflight warning: {nqProbe.Message}";
        }

        ulong? blueprintId = NormalizeBlueprintId(projected.BlueprintId ?? nqProbe.BlueprintId);
        string blueprintName = projected.BlueprintName;
        if (string.IsNullOrWhiteSpace(blueprintName) && !string.IsNullOrWhiteSpace(nqProbe.BlueprintName))
        {
            blueprintName = nqProbe.BlueprintName;
        }

        int elementCount = nqProbe.Success && nqProbe.ElementCount > 0
            ? nqProbe.ElementCount
            : projected.ElementCount;

        return projected with
        {
            BlueprintName = blueprintName,
            BlueprintId = blueprintId,
            ElementCount = elementCount,
            ImportPipeline = importPipeline,
            ImportNotes = importNotes
        };
    }

    public BlueprintImportResult ParseBlueprintJsonFile(
        string blueprintFilePath,
        string sourceName,
        string? serverRootPath = null,
        string? nqUtilsDllPath = null)
    {
        if (string.IsNullOrWhiteSpace(blueprintFilePath))
        {
            throw new ArgumentException("Blueprint file path is required.", nameof(blueprintFilePath));
        }

        string fullPath = Path.GetFullPath(blueprintFilePath);
        using var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read);

        if (stream.Length <= MaxBytesForInMemoryNqPreflight)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            string jsonContent = reader.ReadToEnd();
            return ParseBlueprintJson(jsonContent, sourceName, serverRootPath, nqUtilsDllPath);
        }

        BlueprintImportResult projected = ParseBlueprintJsonLegacy(stream, sourceName, serverRootPath);
        string fileSize = FormatByteLength(stream.Length);
        return projected with
        {
            ImportPipeline = "Legacy JSON projection (NQ preflight skipped for large file)",
            ImportNotes =
                $"NQ preflight skipped to avoid full file read for large JSON ({fileSize}). " +
                $"Threshold={FormatByteLength(MaxBytesForInMemoryNqPreflight)}."
        };
    }

    public async Task<BlueprintGameDatabaseImportResult> ImportBlueprintIntoGameDatabaseAsync(
        string jsonContent,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        bool appendDateIfExists,
        DataConnectionOptions? nameCollisionLookupOptions,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(jsonContent))
        {
            throw new ArgumentException("Blueprint JSON content is empty.", nameof(jsonContent));
        }

        byte[] payload = Encoding.UTF8.GetBytes(jsonContent);
        return await ImportBlueprintPayloadToGameDatabaseAsync(
            payload,
            endpointTemplate,
            blueprintImportEndpoint,
            creatorPlayerId,
            creatorOrganizationId,
            appendDateIfExists,
            nameCollisionLookupOptions,
            cancellationToken);
    }

    public async Task<BlueprintGameDatabaseImportResult> ImportBlueprintFileIntoGameDatabaseAsync(
        string blueprintFilePath,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        bool appendDateIfExists,
        DataConnectionOptions? nameCollisionLookupOptions,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(blueprintFilePath))
        {
            throw new ArgumentException("Blueprint file path is required.", nameof(blueprintFilePath));
        }

        string fullPath = Path.GetFullPath(blueprintFilePath);
        byte[] payload = await File.ReadAllBytesAsync(fullPath, cancellationToken);
        return await ImportBlueprintPayloadToGameDatabaseAsync(
            payload,
            endpointTemplate,
            blueprintImportEndpoint,
            creatorPlayerId,
            creatorOrganizationId,
            appendDateIfExists,
            nameCollisionLookupOptions,
            cancellationToken);
    }

    private async Task<BlueprintGameDatabaseImportResult> ImportBlueprintPayloadToGameDatabaseAsync(
        byte[] blueprintJsonUtf8Payload,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        bool appendDateIfExists,
        DataConnectionOptions? nameCollisionLookupOptions,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<Uri> endpoints = BuildBlueprintImportEndpointCandidates(
            endpointTemplate,
            blueprintImportEndpoint,
            creatorPlayerId,
            creatorOrganizationId);

        var endpointFailures = new List<string>();
        Exception? lastException = null;
        foreach (Uri endpoint in endpoints)
        {
            try
            {
                return await SendBlueprintImportRequestAsync(
                    endpoint,
                    blueprintJsonUtf8Payload,
                    creatorPlayerId,
                    creatorOrganizationId,
                    appendDateIfExists,
                    nameCollisionLookupOptions,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                lastException = ex;
                endpointFailures.Add($"'{endpoint}': {BuildEndpointAttemptError(ex)}");
            }
        }

        throw new InvalidOperationException(
            $"Game DB blueprint import failed for all endpoint candidates: {string.Join(" | ", endpointFailures)}",
            lastException);
    }

    private async Task<BlueprintGameDatabaseImportResult> SendBlueprintImportRequestAsync(
        Uri endpoint,
        byte[] blueprintJsonUtf8Payload,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        bool appendDateIfExists,
        DataConnectionOptions? nameCollisionLookupOptions,
        CancellationToken cancellationToken)
    {
        if (blueprintJsonUtf8Payload is null || blueprintJsonUtf8Payload.Length == 0)
        {
            throw new InvalidOperationException("Blueprint payload is empty.");
        }

        (byte[] requestPayload, string requestNotes) =
            PrepareBlueprintPayloadForGameDatabaseImport(
                blueprintJsonUtf8Payload,
                creatorPlayerId,
                creatorOrganizationId);

        (requestPayload, requestNotes) = await TryApplyNameCollisionDateSuffixAsync(
            requestPayload,
            requestNotes,
            appendDateIfExists,
            nameCollisionLookupOptions,
            cancellationToken);

        try
        {
            return await SendBlueprintImportRequestCoreAsync(
                endpoint,
                requestPayload,
                requestNotes,
                cancellationToken);
        }
        catch (Exception primaryFailure)
            when (ShouldAttemptNoVoxelDataFallback(primaryFailure) &&
                  TryBuildNoVoxelDataFallbackPayload(
                      requestPayload,
                      out byte[] noVoxelPayload,
                      out string fallbackNote))
        {
            string fallbackRequestNotes = AppendRequestNotes(requestNotes, fallbackNote);
            try
            {
                BlueprintGameDatabaseImportResult fallbackResult = await SendBlueprintImportRequestCoreAsync(
                    endpoint,
                    noVoxelPayload,
                    fallbackRequestNotes,
                    cancellationToken);

                return await TryBackfillVoxelDataAfterNoVoxelImportAsync(
                    endpoint,
                    requestPayload,
                    fallbackResult,
                    cancellationToken);
            }
            catch (Exception fallbackFailure)
            {
                throw new InvalidOperationException(
                    $"Game DB blueprint import failed at '{endpoint}' after no-voxel fallback. " +
                    $"Primary failure: {BuildEndpointAttemptError(primaryFailure)} | " +
                    $"Fallback failure: {BuildEndpointAttemptError(fallbackFailure)}",
                    fallbackFailure);
            }
        }
    }

    private async Task<(byte[] Payload, string Notes)> TryApplyNameCollisionDateSuffixAsync(
        byte[] requestPayload,
        string requestNotes,
        bool appendDateIfExists,
        DataConnectionOptions? nameCollisionLookupOptions,
        CancellationToken cancellationToken)
    {
        if (!appendDateIfExists)
        {
            return (requestPayload, requestNotes);
        }

        if (nameCollisionLookupOptions is null)
        {
            return (
                requestPayload,
                AppendRequestNotes(
                    requestNotes,
                    "Append-date rename requested, but DB options are unavailable; skipped."));
        }

        if (!TryReadBlueprintModelName(requestPayload, out string blueprintName, out string readReason))
        {
            return (
                requestPayload,
                AppendRequestNotes(
                    requestNotes,
                    $"Append-date rename skipped: {readReason}"));
        }

        bool exists;
        try
        {
            exists = await DoesBlueprintNameExistAsync(
                nameCollisionLookupOptions,
                blueprintName,
                cancellationToken);
        }
        catch (Exception ex)
        {
            return (
                requestPayload,
                AppendRequestNotes(
                    requestNotes,
                    $"Append-date rename skipped: name lookup failed ({BuildSingleLineExceptionPreview(ex)})"));
        }

        if (!exists)
        {
            return (requestPayload, requestNotes);
        }

        string suffix = DateTime.Now.ToString("yy-MM-dd", CultureInfo.InvariantCulture);
        string renamedBlueprintName = $"{blueprintName}-{suffix}";
        if (!TryRenameBlueprintModelName(
                requestPayload,
                renamedBlueprintName,
                out byte[] renamedPayload,
                out string renameReason))
        {
            return (
                requestPayload,
                AppendRequestNotes(
                    requestNotes,
                    $"Append-date rename skipped: {renameReason}"));
        }

        string renameNote =
            $"Blueprint name collision detected for '{blueprintName}'; renamed to '{renamedBlueprintName}'.";
        return (renamedPayload, AppendRequestNotes(requestNotes, renameNote));
    }

    private async Task<bool> DoesBlueprintNameExistAsync(
        DataConnectionOptions options,
        string blueprintName,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT EXISTS(
                SELECT 1
                FROM blueprint
                WHERE lower(name) = lower(@name)
            );
            """;

        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("name", blueprintName);
        object? result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is bool exists && exists;
    }

    private static bool TryReadBlueprintModelName(
        byte[] payload,
        out string blueprintName,
        out string reason)
    {
        blueprintName = string.Empty;
        reason = "blueprint model name not found";

        try
        {
            string jsonText = Encoding.UTF8.GetString(payload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is not JsonObject rootObject ||
                !TryGetJsonPropertyIgnoreCase(rootObject, "model", out _, out JsonNode? modelNode) ||
                modelNode is not JsonObject modelObject ||
                !TryGetJsonPropertyIgnoreCase(modelObject, "name", out _, out JsonNode? nameNode) ||
                !TryReadJsonString(nameNode, out string modelName))
            {
                return false;
            }

            blueprintName = modelName.Trim();
            if (string.IsNullOrWhiteSpace(blueprintName))
            {
                reason = "blueprint model name is empty";
                return false;
            }

            reason = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            reason = $"invalid blueprint JSON ({BuildSingleLineExceptionPreview(ex)})";
            return false;
        }
    }

    private static bool TryRenameBlueprintModelName(
        byte[] sourcePayload,
        string renamedBlueprintName,
        out byte[] renamedPayload,
        out string reason)
    {
        renamedPayload = sourcePayload;
        reason = "blueprint model name not found";

        try
        {
            string jsonText = Encoding.UTF8.GetString(sourcePayload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is not JsonObject rootObject ||
                !TryGetJsonPropertyIgnoreCase(rootObject, "model", out _, out JsonNode? modelNode) ||
                modelNode is not JsonObject modelObject ||
                !TryGetJsonPropertyIgnoreCase(modelObject, "name", out string nameKey, out JsonNode? nameNode) ||
                !TryReadJsonString(nameNode, out string previousName))
            {
                return false;
            }

            string oldName = previousName.Trim();
            string newName = (renamedBlueprintName ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(newName))
            {
                reason = "renamed blueprint name is empty";
                return false;
            }

            if (string.Equals(oldName, newName, StringComparison.Ordinal))
            {
                reason = "name already has requested suffix";
                return false;
            }

            modelObject[nameKey] = JsonValue.Create(newName);
            string renamedJson = rootObject.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });

            byte[] candidate = Encoding.UTF8.GetBytes(renamedJson);
            if (candidate.Length == 0 || sourcePayload.SequenceEqual(candidate))
            {
                reason = "payload unchanged after rename";
                return false;
            }

            renamedPayload = candidate;
            reason = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            reason = $"rename failed ({BuildSingleLineExceptionPreview(ex)})";
            return false;
        }
    }

    private async Task<BlueprintGameDatabaseImportResult> SendBlueprintImportRequestCoreAsync(
        Uri endpoint,
        byte[] requestPayload,
        string requestNotes,
        CancellationToken cancellationToken)
    {
        var attemptNotes = new List<string>();
        if (!string.IsNullOrWhiteSpace(requestNotes))
        {
            attemptNotes.Add(requestNotes);
        }

        string requestNotesSuffix = string.IsNullOrWhiteSpace(requestNotes)
            ? string.Empty
            : $" Notes: {requestNotes}";
        ImportRequestPayloadKind[] attempts =
        {
            ImportRequestPayloadKind.JsonBase64ByteArray
        };

        const int maxTransportRecoveryRetries = 1;
        for (int attemptIndex = 0; attemptIndex < attempts.Length; attemptIndex++)
        {
            ImportRequestPayloadKind payloadKind = attempts[attemptIndex];
            bool hasFallbackAttempt = attemptIndex < attempts.Length - 1;

            for (int transportAttempt = 0; transportAttempt <= maxTransportRecoveryRetries; transportAttempt++)
            {
                try
                {
                    using HttpRequestMessage request = BuildBlueprintImportRequest(endpoint, requestPayload, payloadKind);
                    using HttpResponseMessage response = await _httpClient.SendAsync(
                        request,
                        HttpCompletionOption.ResponseContentRead,
                        cancellationToken);
                    byte[] responseBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                    string? responseMediaType = response.Content.Headers.ContentType?.MediaType;
                    string responseText = BuildImportResponsePreview(responseBytes, responseMediaType);
                    ulong? importedBlueprintId = TryParseBlueprintIdFromImportResponse(
                        responseText,
                        responseBytes,
                        responseMediaType);

                    if (response.IsSuccessStatusCode)
                    {
                        return new BlueprintGameDatabaseImportResult(
                            endpoint,
                            (int)response.StatusCode,
                            importedBlueprintId,
                            responseText,
                            requestNotes);
                    }

                    string httpFailure =
                        $"{GetPayloadKindDisplayName(payloadKind)}: HTTP {(int)response.StatusCode} {response.StatusCode}, body={BuildHttpBodyPreview(responseText)}";
                    if (hasFallbackAttempt)
                    {
                        attemptNotes.Add(httpFailure);
                        break;
                    }

                    throw new InvalidOperationException(
                        $"Game DB blueprint import failed at '{endpoint}' ({httpFailure}).{requestNotesSuffix}");
                }
                catch (HttpRequestException ex) when (hasFallbackAttempt)
                {
                    attemptNotes.Add($"{GetPayloadKindDisplayName(payloadKind)}: transport error ({BuildTransportErrorPreview(ex)})");
                    break;
                }
                catch (HttpRequestException ex) when (ShouldAttemptTransportRecovery(ex) && transportAttempt < maxTransportRecoveryRetries)
                {
                    attemptNotes.Add(
                        $"{GetPayloadKindDisplayName(payloadKind)}: transport disruption detected ({BuildTransportErrorPreview(ex)}), waiting for backend recovery and retrying.");

                    await WaitForEndpointPortRecoveryAsync(endpoint, TimeSpan.FromSeconds(60), cancellationToken);
                    continue;
                }
                catch (HttpRequestException ex) when (IsConnectionResetException(ex))
                {
                    string payloadSize = FormatByteLength(requestPayload.LongLength);
                    throw new InvalidOperationException(
                        $"Game DB endpoint closed the connection during blueprint import at '{endpoint}' " +
                        $"(payload {payloadSize}, format={GetPayloadKindDisplayName(payloadKind)}). " +
                        $"Check backend logs for import exceptions and verify the endpoint/port.{requestNotesSuffix}",
                        ex);
                }
                catch (HttpRequestException ex)
                {
                    throw new InvalidOperationException(
                        $"Game DB blueprint import transport failed at '{endpoint}' " +
                        $"(format={GetPayloadKindDisplayName(payloadKind)}): {BuildTransportErrorPreview(ex)}.{requestNotesSuffix}",
                        ex);
                }
            }
        }

        string attemptsSummary = attemptNotes.Count == 0
            ? "No further details."
            : string.Join(" | ", attemptNotes);
        throw new InvalidOperationException(
            $"Game DB blueprint import failed at '{endpoint}'. Attempts: {attemptsSummary}");
    }

    private async Task<BlueprintGameDatabaseImportResult> TryBackfillVoxelDataAfterNoVoxelImportAsync(
        Uri gameplayImportEndpoint,
        byte[] sourcePayloadWithVoxelData,
        BlueprintGameDatabaseImportResult fallbackResult,
        CancellationToken cancellationToken)
    {
        ulong? importedBlueprintId = NormalizeBlueprintId(fallbackResult.BlueprintId);
        if (!importedBlueprintId.HasValue)
        {
            string noIdNote =
                "Voxel backfill skipped: imported blueprint id was not returned by backend response.";
            return fallbackResult with
            {
                RequestNotes = AppendRequestNotes(fallbackResult.RequestNotes, noIdNote)
            };
        }

        try
        {
            string backfillNote = await BackfillVoxelDataViaVoxelServiceAsync(
                gameplayImportEndpoint,
                importedBlueprintId.Value,
                sourcePayloadWithVoxelData,
                clearExistingCells: true,
                cancellationToken);

            return fallbackResult with
            {
                RequestNotes = AppendRequestNotes(fallbackResult.RequestNotes, backfillNote)
            };
        }
        catch (Exception ex)
        {
            string warn =
                $"Voxel backfill failed via voxel service: {BuildSingleLineExceptionPreview(ex)}";
            return fallbackResult with
            {
                RequestNotes = AppendRequestNotes(fallbackResult.RequestNotes, warn)
            };
        }
    }

    private async Task<string> BackfillVoxelDataViaVoxelServiceAsync(
        Uri gameplayImportEndpoint,
        ulong targetBlueprintId,
        byte[] sourcePayloadWithVoxelData,
        bool clearExistingCells,
        CancellationToken cancellationToken)
    {
        if (!TryBuildVoxelServiceImportPayload(
                sourcePayloadWithVoxelData,
                targetBlueprintId,
                out byte[] voxelImportPayload,
                out int sourceCellCount,
                out string skipReason))
        {
            return $"Voxel backfill skipped: {skipReason}";
        }

        IReadOnlyList<Uri> importCandidates = BuildVoxelServiceJsonImportEndpointCandidates(
            gameplayImportEndpoint,
            targetBlueprintId,
            clearExistingCells);

        var failures = new List<string>();
        foreach (Uri importEndpoint in importCandidates)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, importEndpoint)
                {
                    Version = HttpVersion.Version11,
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
                    Content = new ByteArrayContent(voxelImportPayload)
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
                        $"'{importEndpoint}' => HTTP {(int)response.StatusCode} {response.StatusCode}, body={BuildHttpBodyPreview(responsePreview)}");
                    continue;
                }

                int dumpCellCount = await VerifyVoxelBlueprintDumpCellCountAsync(
                    importEndpoint,
                    targetBlueprintId,
                    cancellationToken);

                string clearFlag = clearExistingCells ? "1" : "0";
                return
                    $"Voxel backfill applied via voxel service at '{importEndpoint}' " +
                    $"(clear={clearFlag}, source cells={sourceCellCount.ToString(CultureInfo.InvariantCulture)}, " +
                    $"dump cells={dumpCellCount.ToString(CultureInfo.InvariantCulture)}).";
            }
            catch (Exception ex)
            {
                failures.Add($"'{importEndpoint}' => {BuildSingleLineExceptionPreview(ex)}");
            }
        }

        throw new InvalidOperationException(
            $"Voxel backfill via voxel service failed for all endpoint candidates: {string.Join(" | ", failures)}");
    }

    private async Task<int> VerifyVoxelBlueprintDumpCellCountAsync(
        Uri voxelJsonImportEndpoint,
        ulong blueprintId,
        CancellationToken cancellationToken)
    {
        Uri dumpEndpoint = BuildVoxelServiceDumpEndpoint(voxelJsonImportEndpoint, blueprintId);
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
            string preview = BuildHttpBodyPreview(dumpText);
            throw new InvalidOperationException(
                $"voxel dump verification failed at '{dumpEndpoint}' " +
                $"(HTTP {(int)response.StatusCode} {response.StatusCode}, body={preview})");
        }

        JsonNode? dumpNode = JsonNode.Parse(dumpText);
        if (dumpNode is not JsonObject dumpObject ||
            !TryGetJsonPropertyIgnoreCase(dumpObject, "cells", out _, out JsonNode? cellsNode) ||
            cellsNode is not JsonArray cellsArray)
        {
            throw new InvalidOperationException(
                $"voxel dump verification returned no 'cells' array at '{dumpEndpoint}'.");
        }

        return cellsArray.Count;
    }

    private static bool TryBuildVoxelServiceImportPayload(
        byte[] sourceBlueprintPayload,
        ulong targetBlueprintId,
        out byte[] voxelImportPayload,
        out int sourceCellCount,
        out string reason)
    {
        voxelImportPayload = Array.Empty<byte>();
        sourceCellCount = 0;
        reason = "source blueprint has no VoxelData array";

        try
        {
            string json = Encoding.UTF8.GetString(sourceBlueprintPayload);
            JsonNode? root = JsonNode.Parse(json);
            if (root is not JsonObject rootObject ||
                !TryGetJsonPropertyIgnoreCase(rootObject, "VoxelData", out _, out JsonNode? voxelNode) ||
                voxelNode is not JsonArray voxelArray)
            {
                return false;
            }

            if (voxelArray.Count == 0)
            {
                reason = "source blueprint VoxelData is empty";
                return false;
            }

            var retargetedCells = new JsonArray();
            foreach (JsonNode? sourceCell in voxelArray)
            {
                JsonNode? clonedCell = sourceCell?.DeepClone();
                if (clonedCell is JsonObject cellObject)
                {
                    JsonObject targetOid = BuildMongoNumberLongObject(targetBlueprintId);
                    if (TryGetJsonPropertyIgnoreCase(cellObject, "oid", out string oidName, out JsonNode? oidNode))
                    {
                        if (oidNode is JsonObject existingOidObject)
                        {
                            if (TryGetJsonPropertyIgnoreCase(
                                    existingOidObject,
                                    "$numberLong",
                                    out string numberLongKey,
                                    out _))
                            {
                                existingOidObject[numberLongKey] =
                                    JsonValue.Create(targetBlueprintId.ToString(CultureInfo.InvariantCulture));
                                cellObject[oidName] = existingOidObject;
                            }
                            else
                            {
                                cellObject[oidName] = targetOid;
                            }
                        }
                        else
                        {
                            cellObject[oidName] = targetOid;
                        }
                    }
                    else
                    {
                        cellObject["oid"] = targetOid;
                    }
                }

                retargetedCells.Add(clonedCell);
            }

            sourceCellCount = retargetedCells.Count;
            var voxelImportObject = new JsonObject
            {
                ["pipeline"] = null,
                ["cells"] = retargetedCells
            };

            string importJson = voxelImportObject.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });

            voxelImportPayload = Encoding.UTF8.GetBytes(importJson);
            reason = string.Empty;
            return voxelImportPayload.Length > 0;
        }
        catch (Exception ex)
        {
            reason = $"unable to parse VoxelData ({BuildSingleLineExceptionPreview(ex)})";
            return false;
        }
    }

    private static JsonObject BuildMongoNumberLongObject(ulong value)
    {
        return new JsonObject
        {
            ["$numberLong"] = JsonValue.Create(value.ToString(CultureInfo.InvariantCulture))
        };
    }

    private static IReadOnlyList<Uri> BuildVoxelServiceJsonImportEndpointCandidates(
        Uri gameplayImportEndpoint,
        ulong blueprintId,
        bool clearExistingCells)
    {
        var candidates = new List<Uri>();

        static void AddCandidate(List<Uri> list, Uri source, string host, ulong id, bool clear)
        {
            var builder = new UriBuilder(source)
            {
                Host = host,
                Port = 8081,
                Path = "/voxels/jsonImport",
                Query =
                    $"kind=blueprint&ids={id.ToString(CultureInfo.InvariantCulture)}&clear={(clear ? "1" : "0")}"
            };
            list.Add(builder.Uri);
        }

        string primaryHost = gameplayImportEndpoint.Host;
        AddCandidate(candidates, gameplayImportEndpoint, primaryHost, blueprintId, clearExistingCells);

        if (string.Equals(primaryHost, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            AddCandidate(candidates, gameplayImportEndpoint, "127.0.0.1", blueprintId, clearExistingCells);
            AddCandidate(candidates, gameplayImportEndpoint, "::1", blueprintId, clearExistingCells);
        }
        else if (string.Equals(primaryHost, "127.0.0.1", StringComparison.OrdinalIgnoreCase))
        {
            AddCandidate(candidates, gameplayImportEndpoint, "::1", blueprintId, clearExistingCells);
            AddCandidate(candidates, gameplayImportEndpoint, "localhost", blueprintId, clearExistingCells);
        }
        else if (string.Equals(primaryHost, "::1", StringComparison.OrdinalIgnoreCase))
        {
            AddCandidate(candidates, gameplayImportEndpoint, "127.0.0.1", blueprintId, clearExistingCells);
            AddCandidate(candidates, gameplayImportEndpoint, "localhost", blueprintId, clearExistingCells);
        }

        var deduplicated = new List<Uri>(candidates.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri candidate in candidates)
        {
            if (seen.Add(candidate.AbsoluteUri))
            {
                deduplicated.Add(candidate);
            }
        }

        return deduplicated;
    }

    private static Uri BuildVoxelServiceDumpEndpoint(Uri voxelJsonImportEndpoint, ulong blueprintId)
    {
        var builder = new UriBuilder(voxelJsonImportEndpoint)
        {
            Path = $"/voxels/blueprints/{blueprintId.ToString(CultureInfo.InvariantCulture)}/dump.json",
            Query = string.Empty
        };
        return builder.Uri;
    }

    private static string AppendRequestNotes(string currentNotes, string additionalNote)
    {
        if (string.IsNullOrWhiteSpace(additionalNote))
        {
            return currentNotes;
        }

        if (string.IsNullOrWhiteSpace(currentNotes))
        {
            return additionalNote.Trim();
        }

        string trimmedCurrent = currentNotes.Trim();
        return trimmedCurrent.EndsWith(".", StringComparison.Ordinal)
            ? $"{trimmedCurrent} {additionalNote.Trim()}"
            : $"{trimmedCurrent}; {additionalNote.Trim()}";
    }

    private static bool ShouldAttemptNoVoxelDataFallback(Exception ex)
    {
        if (ex is HttpRequestException httpEx && IsConnectionResetException(httpEx))
        {
            return true;
        }

        if (ex is InvalidOperationException invalidOperationException)
        {
            string message = invalidOperationException.Message;
            if (message.Contains("closed the connection during blueprint import", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("Unknown Exception got in server", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return ex.InnerException is not null && ShouldAttemptNoVoxelDataFallback(ex.InnerException);
    }

    private static bool TryBuildNoVoxelDataFallbackPayload(
        byte[] sourcePayload,
        out byte[] fallbackPayload,
        out string fallbackNote)
    {
        fallbackPayload = sourcePayload;
        fallbackNote = string.Empty;

        try
        {
            string jsonText = Encoding.UTF8.GetString(sourcePayload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is not JsonObject rootObject)
            {
                return false;
            }

            if (!TryGetJsonPropertyIgnoreCase(rootObject, "VoxelData", out string voxelDataName, out JsonNode? voxelDataNode))
            {
                return false;
            }

            int voxelEntryCount = voxelDataNode is JsonArray voxelArray ? voxelArray.Count : -1;
            if (voxelDataNode is JsonArray existingVoxelArray && existingVoxelArray.Count == 0)
            {
                return false;
            }

            rootObject[voxelDataName] = new JsonArray();
            string fallbackJson = rootObject.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });

            byte[] candidatePayload = Encoding.UTF8.GetBytes(fallbackJson);
            if (candidatePayload.Length == 0 || sourcePayload.SequenceEqual(candidatePayload))
            {
                return false;
            }

            fallbackPayload = candidatePayload;
            string countText = voxelEntryCount >= 0
                ? $"{voxelEntryCount.ToString(CultureInfo.InvariantCulture)} entries"
                : "non-array value";
            fallbackNote =
                $"Fallback applied: stripped top-level VoxelData ({countText}) after backend voxel import failure.";
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildEndpointAttemptError(Exception ex)
    {
        string message = ex.Message;
        if (message.Length > 260)
        {
            return message[..257] + "...";
        }

        return message;
    }

    private static HttpRequestMessage BuildBlueprintImportRequest(
        Uri endpoint,
        byte[] blueprintJsonUtf8Payload,
        ImportRequestPayloadKind payloadKind)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        };
        request.Headers.ConnectionClose = true;
        request.Headers.ExpectContinue = false;

        switch (payloadKind)
        {
            case ImportRequestPayloadKind.JsonBase64ByteArray:
            {
                string encodedPayload = Convert.ToBase64String(blueprintJsonUtf8Payload);
                string requestBody = System.Text.Json.JsonSerializer.Serialize(encodedPayload);
                request.Content = new StringContent(requestBody, Encoding.UTF8, "application/json");
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported import payload kind: {payloadKind}");
        }

        return request;
    }

    private static (byte[] Payload, string Notes) PrepareBlueprintPayloadForGameDatabaseImport(
        byte[] originalPayload,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        if (originalPayload.Length == 0)
        {
            return (originalPayload, string.Empty);
        }

        try
        {
            string jsonText = Encoding.UTF8.GetString(originalPayload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is null)
            {
                return (originalPayload, string.Empty);
            }

            var notes = new List<string>();
            int normalizedElementMaps = NormalizeElementPropertyMaps(
                root,
                out int removedMalformedServerProperties);
            if (normalizedElementMaps > 0)
            {
                notes.Add(
                    $"normalized {normalizedElementMaps.ToString(CultureInfo.InvariantCulture)} element property maps");
            }
            if (removedMalformedServerProperties > 0)
            {
                notes.Add(
                    $"repaired malformed serverProperties in {removedMalformedServerProperties.ToString(CultureInfo.InvariantCulture)} elements");
            }

            string normalizedJson = root.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });
            byte[] normalizedPayload = Encoding.UTF8.GetBytes(normalizedJson);
            if (normalizedPayload.Length < originalPayload.Length)
            {
                notes.Add(
                    $"minified JSON payload from {FormatByteLength(originalPayload.LongLength)} to {FormatByteLength(normalizedPayload.LongLength)}");
            }

            long estimatedRequestBodyBytes = EstimateJsonBase64RequestBodyLength(normalizedPayload.LongLength);
            if (estimatedRequestBodyBytes > EstimatedDefaultJsonRequestBodyLimitBytes)
            {
                notes.Add(
                    $"request body remains large after base64 (~{FormatByteLength(estimatedRequestBodyBytes)}); backend may reject it unless request size limits are increased");
            }

            bool payloadChanged = normalizedElementMaps > 0 ||
                                  removedMalformedServerProperties > 0 ||
                                  normalizedPayload.Length < originalPayload.Length;
            if (!payloadChanged)
            {
                return (originalPayload, string.Empty);
            }

            string noteText = notes.Count == 0
                ? "Runtime normalization applied."
                : $"Runtime normalization applied: {string.Join("; ", notes)}.";
            return (normalizedPayload, noteText);
        }
        catch (Exception ex)
        {
            return (
                originalPayload,
                $"Runtime normalization skipped: {BuildSingleLineExceptionPreview(ex)}");
        }
    }

}
