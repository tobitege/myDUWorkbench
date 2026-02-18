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
}
