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
}
