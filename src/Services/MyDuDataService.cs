// Helper Index:
// - LoadConstructSnapshotAsync: Loads construct metadata, transforms, and decoded element properties from PostgreSQL.
// - GetUserConstructsAsync: Lists user-owned constructs by core type (dynamic/static/space) with configurable sorting.
// - SearchConstructsByNameAsync: Returns construct id/name suggestions via ILIKE matching.
// - ParseBlueprintJson: Flattens blueprint JSON into grid-friendly element property records.
// - ProbeEndpointAsync: Probes construct endpoint payloads and attempts JSON/binary decoding.
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

public sealed class MyDuDataService
{
    private const long MaxBytesForInMemoryNqPreflight = 30L * 1024L * 1024L;
    private const long EstimatedDefaultJsonRequestBodyLimitBytes = 30_000_000L;
    private static readonly string[] DefaultCoreKindFilter = { "dynamic", "static", "space" };
    private static readonly string[] DefaultNqUtilsDllPaths =
    {
        @"D:\MyDUserver\wincs\all\NQutils.dll",
        @"d:\MyDUserver\wincs\all\NQutils.dll",
        @"D:\github\NQUtils\NQutils\bin\Debug\NQutils.dll",
        @"D:\github\NQUtils\NQutils\bin\Release\NQutils.dll"
    };

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

    private static int NormalizeElementPropertyMaps(
        JsonNode root,
        out int removedMalformedServerProperties)
    {
        removedMalformedServerProperties = 0;
        if (root is not JsonObject rootObject)
        {
            return 0;
        }

        if (!TryGetJsonPropertyIgnoreCase(rootObject, "elements", out string elementsKey, out JsonNode? elementsNode) ||
            elementsNode is not JsonArray elementsArray)
        {
            return 0;
        }

        int normalizedCount = 0;
        for (int i = 0; i < elementsArray.Count; i++)
        {
            if (elementsArray[i] is not JsonObject elementObject)
            {
                continue;
            }

            normalizedCount += NormalizeElementPropertiesField(elementObject);
            normalizedCount += NormalizeElementServerPropertiesField(
                elementObject,
                out bool removedServerProperties);
            if (removedServerProperties)
            {
                removedMalformedServerProperties++;
            }
        }

        // Keep case/style as found in source document.
        rootObject[elementsKey] = elementsArray;
        return normalizedCount;
    }

    private static int NormalizeElementPropertiesField(JsonObject elementObject)
    {
        if (!TryGetJsonPropertyIgnoreCase(elementObject, "properties", out string actualName, out JsonNode? node))
        {
            return 0;
        }

        // ElementInfo.properties uses PropertyMapConverter and must stay in array form:
        // [ ["name", { "type": ..., "value": ... }], ... ].
        if (node is JsonArray propertyArray)
        {
            int fixes = CanonicalizePropertyEntryArray(propertyArray);
            if (fixes > 0)
            {
                elementObject[actualName] = propertyArray;
            }

            return fixes;
        }

        if (node is JsonObject propertyMapObject)
        {
            JsonArray converted = ConvertPropertyObjectToArray(propertyMapObject);
            elementObject[actualName] = converted;
            return 1;
        }

        if (node is null)
        {
            elementObject[actualName] = new JsonArray();
            return 1;
        }

        // Unsupported scalar payload: reset to empty, valid property map array.
        elementObject[actualName] = new JsonArray();
        return 1;
    }

    private static int NormalizeElementServerPropertiesField(
        JsonObject elementObject,
        out bool removedServerProperties)
    {
        removedServerProperties = false;
        if (!TryGetJsonPropertyIgnoreCase(elementObject, "serverProperties", out string actualName, out JsonNode? node))
        {
            return 0;
        }

        if (node is JsonObject)
        {
            int fixes = NormalizePropertyMapObjectValues((JsonObject)node);
            if (fixes > 0)
            {
                elementObject[actualName] = node;
            }

            return fixes;
        }

        if (node is JsonArray arrayNode)
        {
            JsonObject converted = ConvertPropertyArrayToObject(arrayNode, out int droppedEntries);
            elementObject[actualName] = converted;
            return 1 + droppedEntries;
        }

        if (node is null)
        {
            elementObject[actualName] = new JsonObject();
            removedServerProperties = true;
            return 1;
        }

        // serverProperties is a Dictionary<string, PropertyValue> without PropertyMapConverter.
        // Scalars are irrecoverable for that shape; coerce to empty object map.
        elementObject[actualName] = new JsonObject();
        removedServerProperties = true;
        return 1;
    }

    private static int NormalizePropertyMapObjectValues(JsonObject propertyMap)
    {
        if (propertyMap.Count == 0)
        {
            return 0;
        }

        var toRemove = new List<string>();
        var toReplace = new List<KeyValuePair<string, JsonNode?>>();

        foreach (KeyValuePair<string, JsonNode?> kvp in propertyMap)
        {
            if (string.IsNullOrWhiteSpace(kvp.Key) || kvp.Value is null)
            {
                toRemove.Add(kvp.Key);
                continue;
            }

            JsonNode normalized = NormalizePropertyPayloadForMap(kvp.Value, out bool payloadChanged);
            if (payloadChanged || !JsonNode.DeepEquals(kvp.Value, normalized))
            {
                toReplace.Add(new KeyValuePair<string, JsonNode?>(kvp.Key, normalized));
            }
        }

        int fixes = toRemove.Count + toReplace.Count;
        if (fixes == 0)
        {
            return 0;
        }

        foreach (string key in toRemove)
        {
            propertyMap.Remove(key);
        }

        foreach (KeyValuePair<string, JsonNode?> kvp in toReplace)
        {
            propertyMap[kvp.Key] = kvp.Value;
        }

        return fixes;
    }

    private static JsonObject ConvertPropertyArrayToObject(JsonArray source, out int droppedEntries)
    {
        droppedEntries = 0;
        var map = new JsonObject();
        foreach (JsonNode? entry in source)
        {
            if (!TryReadPropertyMapEntry(entry, out string key, out JsonNode? payloadNode) ||
                string.IsNullOrWhiteSpace(key) ||
                payloadNode is null)
            {
                droppedEntries++;
                continue;
            }

            JsonNode normalized = NormalizePropertyPayloadForMap(payloadNode, out _);
            map[key] = normalized;
        }

        return map;
    }

    private static bool TryReadPropertyMapEntry(
        JsonNode? entry,
        out string key,
        out JsonNode? payloadNode)
    {
        key = string.Empty;
        payloadNode = null;

        if (entry is JsonArray pair)
        {
            if (pair.Count < 2 || !TryReadNonEmptyJsonString(pair[0], out key))
            {
                return false;
            }

            payloadNode = pair[1]?.DeepClone();
            return payloadNode is not null;
        }

        if (entry is JsonObject obj)
        {
            if (TryGetJsonPropertyIgnoreCase(obj, "name", out _, out JsonNode? nameNode) &&
                TryReadNonEmptyJsonString(nameNode, out string parsedName) &&
                TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? valueNode))
            {
                key = parsedName;
                payloadNode = valueNode?.DeepClone();
                return payloadNode is not null;
            }

            if (TryGetJsonPropertyIgnoreCase(obj, "key", out _, out JsonNode? keyNode) &&
                TryReadNonEmptyJsonString(keyNode, out string parsedKey) &&
                TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? keyedValueNode))
            {
                key = parsedKey;
                payloadNode = keyedValueNode?.DeepClone();
                return payloadNode is not null;
            }

            if (obj.Count == 1)
            {
                KeyValuePair<string, JsonNode?> single = obj.First();
                if (string.IsNullOrWhiteSpace(single.Key) || single.Value is null)
                {
                    return false;
                }

                key = single.Key;
                payloadNode = single.Value.DeepClone();
                return true;
            }
        }

        return false;
    }

    private static JsonNode NormalizePropertyPayloadForMap(JsonNode payloadNode, out bool changed)
    {
        changed = false;
        if (payloadNode is JsonObject payloadObj &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "type", out _, out JsonNode? typeNode) &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "value", out _, out JsonNode? valueNode))
        {
            if (!TryGetIntFromJsonNode(typeNode, out int parsedType))
            {
                return CanonicalizePropertyPayload(payloadNode, out changed);
            }

            int normalizedType = parsedType;
            JsonNode? normalizedValue;
            bool valueChanged = false;
            switch (parsedType)
            {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                case 255:
                    normalizedValue = CoercePropertyValueForType(
                        ref normalizedType,
                        valueNode?.DeepClone(),
                        out valueChanged);
                    break;
                default:
                    normalizedValue = valueNode?.DeepClone();
                    break;
            }

            bool alreadyCanonical =
                payloadObj.Count == 2 &&
                payloadObj.ContainsKey("type") &&
                payloadObj.ContainsKey("value");
            bool typeChanged = normalizedType != parsedType;
            if (alreadyCanonical &&
                !typeChanged &&
                !valueChanged &&
                JsonNode.DeepEquals(payloadObj["value"], normalizedValue))
            {
                return payloadObj.DeepClone();
            }

            changed = true;
            return new JsonObject
            {
                ["type"] = JsonValue.Create(normalizedType),
                ["value"] = normalizedValue
            };
        }

        return CanonicalizePropertyPayload(payloadNode, out changed);
    }

    private static bool TryReadNonEmptyJsonString(JsonNode? node, out string value)
    {
        value = string.Empty;
        if (!TryReadJsonString(node, out string parsed) || string.IsNullOrWhiteSpace(parsed))
        {
            return false;
        }

        value = parsed;
        return true;
    }

    private static JsonArray ConvertPropertyObjectToArray(JsonObject source)
    {
        var array = new JsonArray();
        foreach (KeyValuePair<string, JsonNode?> kvp in source)
        {
            JsonObject payload = CanonicalizePropertyPayload(kvp.Value, out _);
            array.Add(new JsonArray
            {
                JsonValue.Create(kvp.Key),
                payload
            });
        }

        return array;
    }

    private static int CanonicalizePropertyEntryArray(JsonArray propertyArray)
    {
        int fixes = 0;
        for (int index = 0; index < propertyArray.Count; index++)
        {
            JsonNode? entry = propertyArray[index];
            if (!TryCanonicalizePropertyArrayEntry(entry, index, out JsonArray canonicalEntry, out bool changed))
            {
                continue;
            }

            if (changed)
            {
                propertyArray[index] = canonicalEntry;
                fixes++;
            }
        }

        return fixes;
    }

    private static bool TryCanonicalizePropertyArrayEntry(
        JsonNode? entry,
        int index,
        out JsonArray canonicalEntry,
        out bool changed)
    {
        changed = false;
        string key = $"_idx{index.ToString(CultureInfo.InvariantCulture)}";
        JsonNode? rawPayload = null;

        if (entry is JsonArray pair)
        {
            if (pair.Count > 0 && TryReadJsonString(pair[0], out string parsedKey))
            {
                key = parsedKey;
            }
            else
            {
                changed = true;
            }

            if (pair.Count > 1)
            {
                rawPayload = pair[1];
            }
            else
            {
                changed = true;
            }

            if (pair.Count != 2)
            {
                changed = true;
            }
        }
        else if (entry is JsonObject obj)
        {
            if (TryGetJsonPropertyIgnoreCase(obj, "name", out _, out JsonNode? nameNode) &&
                TryReadJsonString(nameNode, out string parsedName))
            {
                key = parsedName;
            }
            else if (TryGetJsonPropertyIgnoreCase(obj, "key", out _, out JsonNode? keyNode) &&
                     TryReadJsonString(keyNode, out string parsedKey))
            {
                key = parsedKey;
            }
            else if (obj.Count == 1)
            {
                KeyValuePair<string, JsonNode?> single = obj.First();
                key = single.Key;
                rawPayload = single.Value;
                changed = true;
            }
            else
            {
                changed = true;
            }

            if (rawPayload is null)
            {
                if (TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? valueNode))
                {
                    rawPayload = valueNode;
                }
                else
                {
                    rawPayload = obj.DeepClone();
                    changed = true;
                }
            }
        }
        else
        {
            rawPayload = entry?.DeepClone();
            changed = true;
        }

        JsonObject canonicalPayload = CanonicalizePropertyPayload(rawPayload, out bool payloadChanged);
        changed |= payloadChanged;

        canonicalEntry = new JsonArray
        {
            JsonValue.Create(key),
            canonicalPayload
        };

        if (!changed &&
            entry is JsonArray originalPair &&
            originalPair.Count == 2 &&
            JsonNode.DeepEquals(originalPair[0], canonicalEntry[0]) &&
            JsonNode.DeepEquals(originalPair[1], canonicalEntry[1]))
        {
            return true;
        }

        if (!changed && entry is not JsonArray)
        {
            changed = true;
        }

        return true;
    }

    private static JsonObject CanonicalizePropertyPayload(JsonNode? payloadNode, out bool changed)
    {
        changed = false;
        int type;
        JsonNode? valueNode;

        if (payloadNode is JsonObject payloadObj &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "type", out _, out JsonNode? typeNode))
        {
            if (!TryGetIntFromJsonNode(typeNode, out type))
            {
                type = InferPropertyTypeFromNode(TryGetValueNode(payloadObj));
                changed = true;
            }

            valueNode = TryGetValueNode(payloadObj);
            if (valueNode is null)
            {
                valueNode = null;
                changed = true;
            }

            if (!IsCanonicalPayloadObject(payloadObj))
            {
                changed = true;
            }
        }
        else
        {
            valueNode = payloadNode;
            type = InferPropertyTypeFromNode(valueNode);
            changed = true;
        }

        JsonNode? canonicalValue = CoercePropertyValueForType(ref type, valueNode, out bool valueChanged);
        changed |= valueChanged;

        var canonicalPayload = new JsonObject
        {
            ["type"] = JsonValue.Create(type),
            ["value"] = canonicalValue
        };

        if (!changed &&
            payloadNode is JsonObject originalObject &&
            !JsonNode.DeepEquals(originalObject, canonicalPayload))
        {
            changed = true;
        }

        return canonicalPayload;
    }

    private static JsonNode? TryGetValueNode(JsonObject payloadObj)
    {
        return TryGetJsonPropertyIgnoreCase(payloadObj, "value", out _, out JsonNode? valueNode)
            ? valueNode
            : null;
    }

    private static bool IsCanonicalPayloadObject(JsonObject payloadObj)
    {
        if (payloadObj.Count != 2)
        {
            return false;
        }

        KeyValuePair<string, JsonNode?> first = payloadObj.ElementAt(0);
        KeyValuePair<string, JsonNode?> second = payloadObj.ElementAt(1);
        return string.Equals(first.Key, "type", StringComparison.Ordinal) &&
               string.Equals(second.Key, "value", StringComparison.Ordinal);
    }

    private static int InferPropertyTypeFromNode(JsonNode? node)
    {
        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<bool>(out _))
            {
                return 1;
            }

            if (scalar.TryGetValue<long>(out _) || scalar.TryGetValue<int>(out _))
            {
                return 2;
            }

            if (scalar.TryGetValue<double>(out double floatValue))
            {
                return Math.Abs(floatValue % 1d) < 1e-9 ? 2 : 3;
            }

            return 4;
        }

        if (node is JsonArray array)
        {
            if (array.Count == 4)
            {
                return 5;
            }

            if (array.Count == 3)
            {
                return 6;
            }

            return 4;
        }

        if (node is JsonObject obj)
        {
            bool hasQuat = TryGetJsonPropertyIgnoreCase(obj, "w", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "x", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "y", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "z", out _, out _);
            if (hasQuat)
            {
                return 5;
            }

            bool hasVec3 = TryGetJsonPropertyIgnoreCase(obj, "x", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "y", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "z", out _, out _);
            if (hasVec3)
            {
                return 6;
            }
        }

        return 4;
    }

    private static JsonNode? CoercePropertyValueForType(ref int type, JsonNode? valueNode, out bool changed)
    {
        changed = false;
        switch (type)
        {
            case 1:
                if (TryGetBoolFromJsonNode(valueNode, out bool boolValue))
                {
                    if (!IsNodeBoolean(valueNode, boolValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(boolValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 2:
                if (TryGetLongFromJsonNode(valueNode, out long intValue))
                {
                    if (!IsNodeInteger(valueNode, intValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(intValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 3:
                if (TryGetDoubleFromJsonNode(valueNode, out double doubleValue))
                {
                    if (!IsNodeDouble(valueNode, doubleValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(doubleValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 5:
                if (TryNormalizeQuatNode(valueNode, out JsonObject quatNode, out bool quatChanged))
                {
                    changed = quatChanged;
                    return quatNode;
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 6:
                if (TryNormalizeVec3Node(valueNode, out JsonObject vec3Node, out bool vec3Changed))
                {
                    changed = vec3Changed;
                    return vec3Node;
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 4:
            case 7:
            case 255:
                if (valueNode is JsonValue scalar && scalar.TryGetValue<string>(out string? stringValue))
                {
                    return JsonValue.Create(stringValue ?? string.Empty);
                }

                changed = true;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            default:
                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
        }
    }

    private static bool TryNormalizeQuatNode(JsonNode? node, out JsonObject result, out bool changed)
    {
        changed = false;
        result = new JsonObject();
        if (node is JsonObject obj &&
            TryGetDoubleByName(obj, "w", out double w) &&
            TryGetDoubleByName(obj, "x", out double x) &&
            TryGetDoubleByName(obj, "y", out double y) &&
            TryGetDoubleByName(obj, "z", out double z))
        {
            result["w"] = JsonValue.Create(w);
            result["x"] = JsonValue.Create(x);
            result["y"] = JsonValue.Create(y);
            result["z"] = JsonValue.Create(z);
            changed = !IsCanonicalQuatObject(obj, w, x, y, z);
            return true;
        }

        if (node is JsonArray arr &&
            arr.Count >= 4 &&
            TryGetDoubleFromJsonNode(arr[0], out double aw) &&
            TryGetDoubleFromJsonNode(arr[1], out double ax) &&
            TryGetDoubleFromJsonNode(arr[2], out double ay) &&
            TryGetDoubleFromJsonNode(arr[3], out double az))
        {
            result["w"] = JsonValue.Create(aw);
            result["x"] = JsonValue.Create(ax);
            result["y"] = JsonValue.Create(ay);
            result["z"] = JsonValue.Create(az);
            changed = true;
            return true;
        }

        return false;
    }

    private static long EstimateJsonBase64RequestBodyLength(long payloadBytes)
    {
        if (payloadBytes <= 0)
        {
            return 2;
        }

        long base64Length = ((payloadBytes + 2L) / 3L) * 4L;
        return base64Length + 2L; // JSON string quotes.
    }

    private static bool TryNormalizeVec3Node(JsonNode? node, out JsonObject result, out bool changed)
    {
        changed = false;
        result = new JsonObject();
        if (node is JsonObject obj &&
            TryGetDoubleByName(obj, "x", out double x) &&
            TryGetDoubleByName(obj, "y", out double y) &&
            TryGetDoubleByName(obj, "z", out double z))
        {
            result["x"] = JsonValue.Create(x);
            result["y"] = JsonValue.Create(y);
            result["z"] = JsonValue.Create(z);
            changed = !IsCanonicalVec3Object(obj, x, y, z);
            return true;
        }

        if (node is JsonArray arr &&
            arr.Count >= 3 &&
            TryGetDoubleFromJsonNode(arr[0], out double ax) &&
            TryGetDoubleFromJsonNode(arr[1], out double ay) &&
            TryGetDoubleFromJsonNode(arr[2], out double az))
        {
            result["x"] = JsonValue.Create(ax);
            result["y"] = JsonValue.Create(ay);
            result["z"] = JsonValue.Create(az);
            changed = true;
            return true;
        }

        return false;
    }

    private static bool IsCanonicalQuatObject(JsonObject obj, double w, double x, double y, double z)
    {
        if (obj.Count != 4)
        {
            return false;
        }

        return string.Equals(obj.ElementAt(0).Key, "w", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(1).Key, "x", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(2).Key, "y", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(3).Key, "z", StringComparison.Ordinal) &&
               TryGetDoubleFromJsonNode(obj.ElementAt(0).Value, out double cw) && Math.Abs(cw - w) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(1).Value, out double cx) && Math.Abs(cx - x) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(2).Value, out double cy) && Math.Abs(cy - y) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(3).Value, out double cz) && Math.Abs(cz - z) < 1e-9;
    }

    private static bool IsCanonicalVec3Object(JsonObject obj, double x, double y, double z)
    {
        if (obj.Count != 3)
        {
            return false;
        }

        return string.Equals(obj.ElementAt(0).Key, "x", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(1).Key, "y", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(2).Key, "z", StringComparison.Ordinal) &&
               TryGetDoubleFromJsonNode(obj.ElementAt(0).Value, out double cx) && Math.Abs(cx - x) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(1).Value, out double cy) && Math.Abs(cy - y) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(2).Value, out double cz) && Math.Abs(cz - z) < 1e-9;
    }

    private static bool TryGetDoubleByName(JsonObject obj, string name, out double value)
    {
        if (TryGetJsonPropertyIgnoreCase(obj, name, out _, out JsonNode? node) &&
            TryGetDoubleFromJsonNode(node, out value))
        {
            return true;
        }

        value = 0d;
        return false;
    }

    private static bool TryGetIntFromJsonNode(JsonNode? node, out int value)
    {
        value = 0;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l) && l >= int.MinValue && l <= int.MaxValue)
        {
            value = (int)l;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetLongFromJsonNode(JsonNode? node, out long value)
    {
        value = 0L;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l;
            return true;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<double>(out double d) && Math.Abs(d % 1d) < 1e-9 &&
            d >= long.MinValue && d <= long.MaxValue)
        {
            value = (long)d;
            return true;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b ? 1L : 0L;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetDoubleFromJsonNode(JsonNode? node, out double value)
    {
        value = 0d;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<double>(out double d))
        {
            value = d;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l;
            return true;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b ? 1d : 0d;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetBoolFromJsonNode(JsonNode? node, out bool value)
    {
        value = false;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l != 0L;
            return true;
        }

        if (scalar.TryGetValue<double>(out double d))
        {
            value = Math.Abs(d) > double.Epsilon;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s))
        {
            if (bool.TryParse(s, out bool parsedBool))
            {
                value = parsedBool;
                return true;
            }

            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedLong))
            {
                value = parsedLong != 0L;
                return true;
            }
        }

        return false;
    }

    private static bool IsNodeBoolean(JsonNode? node, bool value)
    {
        return node is JsonValue scalar && scalar.TryGetValue<bool>(out bool existing) && existing == value;
    }

    private static bool IsNodeInteger(JsonNode? node, long value)
    {
        return node is JsonValue scalar && scalar.TryGetValue<long>(out long existing) && existing == value;
    }

    private static bool IsNodeDouble(JsonNode? node, double value)
    {
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<double>(out double existing))
        {
            return Math.Abs(existing - value) < 1e-12;
        }

        if (scalar.TryGetValue<long>(out long existingLong))
        {
            return Math.Abs(existingLong - value) < 1e-12;
        }

        return false;
    }

    private static string ConvertJsonNodeToString(JsonNode? node)
    {
        if (node is null)
        {
            return string.Empty;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out string? s))
            {
                return s ?? string.Empty;
            }

            if (scalar.TryGetValue<bool>(out bool b))
            {
                return b ? "true" : "false";
            }

            if (scalar.TryGetValue<long>(out long l))
            {
                return l.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<double>(out double d))
            {
                return d.ToString("R", CultureInfo.InvariantCulture);
            }
        }

        return node.ToJsonString();
    }

    private static bool ApplyCreatorOverrideToBlueprintPayload(
        JsonNode root,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        out string note)
    {
        note = string.Empty;
        if (creatorPlayerId == 0UL && creatorOrganizationId == 0UL)
        {
            return false;
        }

        if (root is not JsonObject rootObject ||
            !TryGetJsonPropertyIgnoreCase(rootObject, "model", out string modelKey, out JsonNode? modelNode))
        {
            note =
                "creator override requested but no blueprint model was found; endpoint query uses configured creator ids.";
            return true;
        }

        JsonObject modelObject = modelNode as JsonObject ?? new JsonObject();
        if (modelNode is not JsonObject)
        {
            rootObject[modelKey] = modelObject;
        }

        SetJsonPropertyCaseAware(
            modelObject,
            "CreatorId",
            JsonValue.Create(creatorPlayerId == 0UL ? 2UL : creatorPlayerId));

        JsonObject jsonPropertiesObject = GetOrCreateJsonObjectProperty(modelObject, "JsonProperties");
        JsonObject serverPropertiesObject = GetOrCreateJsonObjectProperty(jsonPropertiesObject, "serverProperties");

        JsonObject creatorIdObject = GetOrCreateJsonObjectProperty(serverPropertiesObject, "creatorId");
        SetJsonPropertyCaseAware(creatorIdObject, "playerId", JsonValue.Create(creatorPlayerId));
        SetJsonPropertyCaseAware(creatorIdObject, "organizationId", JsonValue.Create(creatorOrganizationId));

        note =
            $"applied creator override in payload (playerId={creatorPlayerId.ToString(CultureInfo.InvariantCulture)}, " +
            $"organizationId={creatorOrganizationId.ToString(CultureInfo.InvariantCulture)})";
        return true;
    }

    private static JsonObject GetOrCreateJsonObjectProperty(JsonObject owner, string propertyName)
    {
        if (TryGetJsonPropertyIgnoreCase(owner, propertyName, out string actualName, out JsonNode? value))
        {
            if (value is JsonObject existingObject)
            {
                return existingObject;
            }

            var replacement = new JsonObject();
            owner[actualName] = replacement;
            return replacement;
        }

        var created = new JsonObject();
        owner[propertyName] = created;
        return created;
    }

    private static void SetJsonPropertyCaseAware(JsonObject owner, string propertyName, JsonNode? value)
    {
        if (TryGetJsonPropertyIgnoreCase(owner, propertyName, out string actualName, out _))
        {
            owner[actualName] = value;
            return;
        }

        owner[propertyName] = value;
    }

    private static bool TryGetJsonPropertyIgnoreCase(
        JsonObject obj,
        string propertyName,
        out string actualName,
        out JsonNode? value)
    {
        foreach (KeyValuePair<string, JsonNode?> kvp in obj)
        {
            if (string.Equals(kvp.Key, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                actualName = kvp.Key;
                value = kvp.Value;
                return true;
            }
        }

        actualName = string.Empty;
        value = null;
        return false;
    }

    private static bool TryReadJsonString(JsonNode? node, out string value)
    {
        value = string.Empty;
        if (node is JsonValue scalar && scalar.TryGetValue<string>(out string? stringValue) &&
            !string.IsNullOrWhiteSpace(stringValue))
        {
            value = stringValue;
            return true;
        }

        return false;
    }

    private static string BuildSingleLineExceptionPreview(Exception ex)
    {
        string message = ex.Message ?? string.Empty;
        message = message.Replace("\r", " ").Replace("\n", " ").Trim();
        if (message.Length <= 220)
        {
            return message;
        }

        return message[..217] + "...";
    }

    private static bool IsConnectionResetException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException)
            {
                if (socketException.SocketErrorCode == SocketError.ConnectionReset ||
                    socketException.SocketErrorCode == SocketError.ConnectionAborted)
                {
                    return true;
                }
            }

            if (current is IOException ioException &&
                ioException.Message.Contains("closed", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsConnectionRefusedException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException &&
                socketException.SocketErrorCode == SocketError.ConnectionRefused)
            {
                return true;
            }
        }

        return false;
    }

    private static bool ShouldAttemptTransportRecovery(HttpRequestException ex)
    {
        return IsConnectionResetException(ex) || IsConnectionRefusedException(ex);
    }

    private static async Task WaitForEndpointPortRecoveryAsync(
        Uri endpoint,
        TimeSpan maxWait,
        CancellationToken cancellationToken)
    {
        DateTime startedAt = DateTime.UtcNow;
        while (DateTime.UtcNow - startedAt < maxWait)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await CanConnectTcpAsync(endpoint.Host, endpoint.Port, cancellationToken))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
    }

    private static async Task<bool> CanConnectTcpAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var client = new TcpClient();
            Task connectTask = client.ConnectAsync(host, port);
            Task completed = await Task.WhenAny(connectTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));
            if (!ReferenceEquals(completed, connectTask))
            {
                return false;
            }

            await connectTask;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildTransportErrorPreview(HttpRequestException ex)
    {
        string message = ex.Message;
        if (ex.InnerException is SocketException socketException)
        {
            return $"{message} (socket {(int)socketException.SocketErrorCode}: {socketException.SocketErrorCode})";
        }

        return message;
    }

    private static string GetPayloadKindDisplayName(ImportRequestPayloadKind payloadKind)
    {
        return payloadKind switch
        {
            ImportRequestPayloadKind.JsonBase64ByteArray => "json-base64-byte-array",
            _ => payloadKind.ToString()
        };
    }

    private enum ImportRequestPayloadKind
    {
        JsonBase64ByteArray
    }

    private static Uri BuildBlueprintImportEndpoint(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        Uri baseUri;
        if (!string.IsNullOrWhiteSpace(blueprintImportEndpoint))
        {
            string explicitCandidate = blueprintImportEndpoint.Trim()
                .Replace("{id}", "0", StringComparison.OrdinalIgnoreCase);
            if (!Uri.TryCreate(explicitCandidate, UriKind.Absolute, out Uri? parsedExplicitUri) || parsedExplicitUri is null)
            {
                throw new InvalidOperationException(
                    $"Blueprint import endpoint is not a valid absolute URI: {blueprintImportEndpoint}");
            }
            baseUri = parsedExplicitUri;

            if (string.IsNullOrWhiteSpace(baseUri.AbsolutePath) || baseUri.AbsolutePath == "/")
            {
                var explicitBuilder = new UriBuilder(baseUri)
                {
                    Path = "/blueprint/import"
                };
                baseUri = explicitBuilder.Uri;
            }
        }
        else
        {
            if (string.IsNullOrWhiteSpace(endpointTemplate))
            {
                throw new InvalidOperationException(
                    "Endpoint template is empty; cannot resolve blueprint import endpoint.");
            }

            string candidate = endpointTemplate.Trim()
                .Replace("{id}", "0", StringComparison.OrdinalIgnoreCase);
            if (!Uri.TryCreate(candidate, UriKind.Absolute, out Uri? parsedFallbackUri) || parsedFallbackUri is null)
            {
                throw new InvalidOperationException($"Endpoint template is not a valid absolute URI: {endpointTemplate}");
            }
            baseUri = parsedFallbackUri;

            var fallbackBuilder = new UriBuilder(baseUri)
            {
                Path = "/blueprint/import"
            };
            baseUri = fallbackBuilder.Uri;
        }

        var builder = new UriBuilder(baseUri);
        string existingQuery = string.IsNullOrWhiteSpace(builder.Query)
            ? string.Empty
            : builder.Query.TrimStart('?').Trim();
        string appendQuery =
            $"creatorPlayerId={creatorPlayerId.ToString(CultureInfo.InvariantCulture)}&" +
            $"creatorOrganizationId={creatorOrganizationId.ToString(CultureInfo.InvariantCulture)}";
        builder.Query = string.IsNullOrWhiteSpace(existingQuery)
            ? appendQuery
            : $"{existingQuery}&{appendQuery}";

        return builder.Uri;
    }

    private static IReadOnlyList<Uri> BuildBlueprintImportEndpointCandidates(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        Uri primary = BuildBlueprintImportEndpoint(
            endpointTemplate,
            blueprintImportEndpoint,
            creatorPlayerId,
            creatorOrganizationId);

        var candidates = new List<Uri> { primary };

        if (TryBuildGameplayServiceFallbackEndpoint(primary, out Uri? fallback) && fallback is not null)
        {
            candidates.Add(fallback);
        }

        // Add loopback host variants because some local installs bind only IPv4 or only IPv6.
        int snapshotCount = candidates.Count;
        for (int i = 0; i < snapshotCount; i++)
        {
            Uri candidate = candidates[i];
            if (TryBuildLoopbackHostVariant(candidate, out Uri? loopbackVariant) && loopbackVariant is not null)
            {
                candidates.Add(loopbackVariant);
            }
        }

        var deduplicated = new List<Uri>(candidates.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri candidate in candidates)
        {
            string key = candidate.AbsoluteUri;
            if (seen.Add(key))
            {
                deduplicated.Add(candidate);
            }
        }

        return deduplicated;
    }

    private static bool TryBuildLoopbackHostVariant(Uri source, out Uri? loopbackVariant)
    {
        loopbackVariant = null;
        string host = source.Host;
        if (!string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var builder = new UriBuilder(source)
        {
            Host = "127.0.0.1"
        };
        loopbackVariant = builder.Uri;
        return true;
    }

    private static bool TryBuildGameplayServiceFallbackEndpoint(Uri primaryEndpoint, out Uri? fallbackEndpoint)
    {
        fallbackEndpoint = null;
        if (primaryEndpoint is null)
        {
            return false;
        }

        if (primaryEndpoint.Port != 12003)
        {
            return false;
        }

        var builder = new UriBuilder(primaryEndpoint)
        {
            Port = 10111,
            Path = "/blueprint/import"
        };
        fallbackEndpoint = builder.Uri;
        return true;
    }

    private static ulong? TryParseBlueprintIdFromImportResponse(string? responseText)
    {
        return TryParseBlueprintIdFromImportResponse(responseText, null, null);
    }

    private static ulong? TryParseBlueprintIdFromImportResponse(
        string? responseText,
        byte[]? responseBytes,
        string? responseMediaType)
    {
        if (!string.IsNullOrWhiteSpace(responseText))
        {
            string trimmed = responseText.Trim();
            if (ulong.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong direct))
            {
                return NormalizeBlueprintId(direct);
            }

            try
            {
                using JsonDocument document = JsonDocument.Parse(trimmed);
                JsonElement root = document.RootElement;

                if (root.ValueKind == JsonValueKind.Number && root.TryGetUInt64(out ulong numericRoot))
                {
                    return NormalizeBlueprintId(numericRoot);
                }

                if (root.ValueKind == JsonValueKind.Object)
                {
                    if (TryReadUInt64(root, "blueprintId", "BlueprintId", "id", "Id") is ulong id)
                    {
                        return NormalizeBlueprintId(id);
                    }
                }
            }
            catch
            {
            }
        }

        if (responseBytes is null || responseBytes.Length == 0)
        {
            return null;
        }

        if (!IsNovaquarkBinaryMediaType(responseMediaType))
        {
            return null;
        }

        if (!TryDecodeVarUInt64(responseBytes, out ulong rawValue))
        {
            return null;
        }

        // `application/vnd.novaquark.binary` responses carry a zig-zag encoded integer id.
        return NormalizeBlueprintId(rawValue >> 1);
    }

    private static string BuildImportResponsePreview(byte[] responseBytes, string? responseMediaType)
    {
        if (responseBytes is null || responseBytes.Length == 0)
        {
            return string.Empty;
        }

        if (IsNovaquarkBinaryMediaType(responseMediaType))
        {
            string hex = Convert.ToHexString(responseBytes);
            if (TryDecodeVarUInt64(responseBytes, out ulong raw))
            {
                ulong decodedId = raw >> 1;
                return $"binary(varint={raw.ToString(CultureInfo.InvariantCulture)}, decodedId={decodedId.ToString(CultureInfo.InvariantCulture)}, hex={hex})";
            }

            return $"binary(hex={hex})";
        }

        return DecodeResponseText(responseBytes);
    }

    private static string DecodeResponseText(byte[] responseBytes)
    {
        if (responseBytes is null || responseBytes.Length == 0)
        {
            return string.Empty;
        }

        string utf8 = Encoding.UTF8.GetString(responseBytes);
        if (utf8.IndexOf('\0') >= 0)
        {
            return Convert.ToBase64String(responseBytes);
        }

        return utf8;
    }

    private static bool IsNovaquarkBinaryMediaType(string? mediaType)
    {
        return !string.IsNullOrWhiteSpace(mediaType) &&
               mediaType.Contains("application/vnd.novaquark.binary", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryDecodeVarUInt64(byte[] bytes, out ulong value)
    {
        value = 0UL;
        if (bytes is null || bytes.Length == 0)
        {
            return false;
        }

        int shift = 0;
        for (int i = 0; i < bytes.Length && i < 10; i++)
        {
            byte current = bytes[i];
            value |= (ulong)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
            {
                return true;
            }

            shift += 7;
        }

        value = 0UL;
        return false;
    }

    private static string BuildHttpBodyPreview(string? responseText)
    {
        if (string.IsNullOrWhiteSpace(responseText))
        {
            return "empty response body";
        }

        string[] tokens = responseText
            .Split(new[] {'\r', '\n', '\t'}, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string singleLine = tokens.Length == 0 ? responseText.Trim() : string.Join(" ", tokens);
        const int maxLength = 320;
        if (singleLine.Length <= maxLength)
        {
            return singleLine;
        }

        return singleLine[..(maxLength - 3)] + "...";
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(string jsonContent, string sourceName, string? serverRootPath)
    {
        using JsonDocument document = JsonDocument.Parse(jsonContent, CreateBlueprintJsonDocumentOptions());
        return ParseBlueprintJsonLegacy(document.RootElement, sourceName, serverRootPath);
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(Stream jsonStream, string sourceName, string? serverRootPath)
    {
        if (jsonStream is null)
        {
            throw new ArgumentNullException(nameof(jsonStream));
        }

        if (jsonStream.CanSeek && jsonStream.Position != 0L)
        {
            jsonStream.Seek(0L, SeekOrigin.Begin);
        }

        using JsonDocument document = JsonDocument.Parse(jsonStream, CreateBlueprintJsonDocumentOptions());
        return ParseBlueprintJsonLegacy(document.RootElement, sourceName, serverRootPath);
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(JsonElement root, string sourceName, string? serverRootPath)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("Blueprint root must be a JSON object.");
        }

        JsonElement model = default;
        bool hasModel = TryGetPropertyIgnoreCase(root, "Model", out model) && model.ValueKind == JsonValueKind.Object;
        JsonElement elements = default;
        bool hasElements = TryGetPropertyIgnoreCase(root, "Elements", out elements) && elements.ValueKind == JsonValueKind.Array;

        ulong? blueprintId = hasModel
            ? NormalizeBlueprintId(TryReadUInt64(model, "Id", "id", "blueprintId", "blueprint_id"))
            : null;
        string blueprintName = hasModel ? TryReadString(model, "Name", "name") ?? string.Empty : string.Empty;
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
                ulong elementId = TryReadUInt64(element, "elementId", "element_id", "id") ?? (ulong)fallbackElementId;
                string elementDisplayName = BuildBlueprintElementDisplayName(element, elementId);

                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (string.Equals(property.Name, "properties", StringComparison.OrdinalIgnoreCase) &&
                        property.Value.ValueKind == JsonValueKind.Array &&
                        TryExpandBlueprintElementProperties(records, elementId, elementDisplayName, property.Value, serverRootPath))
                    {
                        continue;
                    }

                    AddBlueprintPropertyRecord(
                        records,
                        elementId,
                        elementDisplayName,
                        property.Name,
                        property.Value,
                        propertyTypeOverride: null,
                        serverRootPath: serverRootPath);
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
                    property.Value,
                    propertyTypeOverride: null,
                    serverRootPath: serverRootPath);
            }
        }

        foreach (JsonProperty property in root.EnumerateObject())
        {
            if (string.Equals(property.Name, "Elements", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(property.Name, "Model", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            AddBlueprintPropertyRecord(
                records,
                modelPseudoElementId,
                "BlueprintRoot [0]",
                $"root.{property.Name}",
                property.Value,
                propertyTypeOverride: null,
                serverRootPath: serverRootPath);
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
            records,
            string.Empty,
            string.Empty);
    }

    private static JsonDocumentOptions CreateBlueprintJsonDocumentOptions()
    {
        return new JsonDocumentOptions
        {
            AllowTrailingCommas = true,
            CommentHandling = JsonCommentHandling.Skip
        };
    }

    private static string FormatByteLength(long length)
    {
        const double kib = 1024d;
        const double mib = kib * 1024d;
        const double gib = mib * 1024d;

        if (length < kib)
        {
            return $"{length.ToString(CultureInfo.InvariantCulture)} B";
        }

        if (length < mib)
        {
            return $"{(length / kib).ToString("0.##", CultureInfo.InvariantCulture)} KiB";
        }

        if (length < gib)
        {
            return $"{(length / mib).ToString("0.##", CultureInfo.InvariantCulture)} MiB";
        }

        return $"{(length / gib).ToString("0.##", CultureInfo.InvariantCulture)} GiB";
    }

    private sealed record NqBlueprintProbe(
        bool Success,
        bool DllUnavailable,
        string Message,
        string DllPath,
        ulong? BlueprintId,
        string BlueprintName,
        int ElementCount,
        int LinkCount,
        bool HasVoxelData);

    private static NqBlueprintProbe ProbeBlueprintWithNqDll(
        string jsonContent,
        string? serverRootPath,
        string? nqUtilsDllPath)
    {
        if (!TryResolveNqUtilsDllPath(serverRootPath, nqUtilsDllPath, out string dllPath, out string resolveMessage))
        {
            return new NqBlueprintProbe(
                Success: false,
                DllUnavailable: true,
                Message: resolveMessage,
                DllPath: string.Empty,
                BlueprintId: null,
                BlueprintName: string.Empty,
                ElementCount: 0,
                LinkCount: 0,
                HasVoxelData: false);
        }

        try
        {
            Assembly nqAssembly = LoadNqUtilsAssembly(dllPath);
            Type? blueprintType = nqAssembly.GetType("NQ.BlueprintData", throwOnError: false);
            if (blueprintType is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "Type NQ.BlueprintData was not found in NQutils.dll.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: false);
            }

            object? blueprint = JsonConvert.DeserializeObject(jsonContent, blueprintType);
            if (blueprint is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "JsonConvert returned null when deserializing NQ.BlueprintData.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: false);
            }

            object? model = GetObjectProperty(blueprint, "Model");
            if (model is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "NQ.BlueprintData.Model is null after deserialization.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: GetObjectProperty(blueprint, "VoxelData") is not null);
            }

            ulong? blueprintId = NormalizeBlueprintId(TryConvertToUInt64(GetObjectProperty(model, "Id")));
            string blueprintName = Convert.ToString(GetObjectProperty(model, "Name"), CultureInfo.InvariantCulture) ?? string.Empty;
            int elementCount = CountEnumerable(GetObjectProperty(blueprint, "Elements"));
            int linkCount = CountEnumerable(GetObjectProperty(blueprint, "Links"));
            bool hasVoxelData = GetObjectProperty(blueprint, "VoxelData") is not null;

            return new NqBlueprintProbe(
                Success: true,
                DllUnavailable: false,
                Message: "Validated with NQutils.dll.",
                DllPath: dllPath,
                BlueprintId: blueprintId,
                BlueprintName: blueprintName,
                ElementCount: elementCount,
                LinkCount: linkCount,
                HasVoxelData: hasVoxelData);
        }
        catch (Exception ex)
        {
            return new NqBlueprintProbe(
                Success: false,
                DllUnavailable: false,
                Message: BuildNqPreflightWarningMessage(ex),
                DllPath: dllPath,
                BlueprintId: null,
                BlueprintName: string.Empty,
                ElementCount: 0,
                LinkCount: 0,
                HasVoxelData: false);
        }
    }

    private static string BuildNqPreflightWarningMessage(Exception ex)
    {
        if (ex is JsonSerializationException jsonEx)
        {
            string path = string.IsNullOrWhiteSpace(jsonEx.Path) ? "<unknown>" : jsonEx.Path;
            string line = jsonEx.LineNumber > 0 ? jsonEx.LineNumber.ToString(CultureInfo.InvariantCulture) : "?";
            string position = jsonEx.LinePosition > 0 ? jsonEx.LinePosition.ToString(CultureInfo.InvariantCulture) : "?";

            if (path.Contains("serverProperties", StringComparison.OrdinalIgnoreCase))
            {
                return
                    $"Schema mismatch at '{path}' (line {line}, position {position}): " +
                    "serverProperties is an array, but NQ preflight expects an object dictionary.";
            }

            return $"Schema mismatch at '{path}' (line {line}, position {position}): {ExtractFirstSentence(jsonEx.Message)}";
        }

        return ExtractFirstSentence(ex.Message);
    }

    private static string ExtractFirstSentence(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return "unknown preflight error";
        }

        string flattened = message.Replace("\r", " ").Replace("\n", " ").Trim();
        int periodIndex = flattened.IndexOf('.', StringComparison.Ordinal);
        if (periodIndex > 0 && periodIndex < flattened.Length - 1)
        {
            return flattened[..(periodIndex + 1)].Trim();
        }

        const int maxLength = 280;
        if (flattened.Length <= maxLength)
        {
            return flattened;
        }

        return flattened[..(maxLength - 3)] + "...";
    }

    private static bool TryResolveNqUtilsDllPath(
        string? serverRootPath,
        string? nqUtilsDllPath,
        out string dllPath,
        out string message)
    {
        var candidates = new List<string>();
        if (!string.IsNullOrWhiteSpace(nqUtilsDllPath))
        {
            candidates.Add(nqUtilsDllPath);
        }

        string? pathFromEnv = Environment.GetEnvironmentVariable("MYDU_NQUTILS_DLL_PATH");
        if (!string.IsNullOrWhiteSpace(pathFromEnv))
        {
            candidates.Add(pathFromEnv);
        }

        string? dirFromEnv = Environment.GetEnvironmentVariable("MYDU_NQUTILS_DLL_DIR");
        if (!string.IsNullOrWhiteSpace(dirFromEnv))
        {
            candidates.Add(Path.Combine(dirFromEnv, "NQutils.dll"));
        }

        if (!string.IsNullOrWhiteSpace(serverRootPath))
        {
            candidates.Add(Path.Combine(serverRootPath, "wincs", "all", "NQutils.dll"));
        }

        candidates.AddRange(DefaultNqUtilsDllPaths);

        foreach (string candidate in candidates.Where(path => !string.IsNullOrWhiteSpace(path)))
        {
            string fullPath;
            try
            {
                fullPath = Path.GetFullPath(candidate);
            }
            catch
            {
                continue;
            }

            if (File.Exists(fullPath))
            {
                dllPath = fullPath;
                message = string.Empty;
                return true;
            }
        }

        dllPath = string.Empty;
        message =
            "NQutils.dll not found. Configure an explicit NQutils.dll path in the Config tab, " +
            "or set MYDU_NQUTILS_DLL_PATH / MYDU_NQUTILS_DLL_DIR, " +
            "or point Server Root Path to your myDU server folder.";
        return false;
    }

    private static Assembly LoadNqUtilsAssembly(string dllPath)
    {
        string fullPath = Path.GetFullPath(dllPath);
        Assembly? loaded = AppDomain.CurrentDomain.GetAssemblies()
            .FirstOrDefault(a =>
            {
                try
                {
                    string location = a.Location ?? string.Empty;
                    return location.Length > 0 &&
                           string.Equals(Path.GetFullPath(location), fullPath, StringComparison.OrdinalIgnoreCase);
                }
                catch
                {
                    return false;
                }
            });

        return loaded ?? Assembly.LoadFrom(fullPath);
    }

    private static object? GetObjectProperty(object target, string propertyName)
    {
        if (target is null)
        {
            return null;
        }

        PropertyInfo? property = target.GetType().GetProperty(
            propertyName,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

        return property?.GetValue(target);
    }

    private static int CountEnumerable(object? value)
    {
        if (value is null)
        {
            return 0;
        }

        if (value is ICollection collection)
        {
            return collection.Count;
        }

        if (value is IEnumerable enumerable)
        {
            int count = 0;
            foreach (object? _ in enumerable)
            {
                count++;
            }

            return count;
        }

        return 0;
    }

    private static ulong? TryConvertToUInt64(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is ulong u)
        {
            return u;
        }

        if (value is long l && l >= 0)
        {
            return (ulong)l;
        }

        if (value is int i && i >= 0)
        {
            return (ulong)i;
        }

        return ulong.TryParse(
            Convert.ToString(value, CultureInfo.InvariantCulture),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out ulong parsed)
            ? parsed
            : null;
    }

    private static ulong? NormalizeBlueprintId(ulong? blueprintId)
    {
        return blueprintId.HasValue && blueprintId.Value > 0UL
            ? blueprintId
            : null;
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
