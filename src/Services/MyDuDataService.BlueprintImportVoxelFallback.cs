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

        string primaryHost = NormalizeLoopbackHost(gameplayImportEndpoint.Host);
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

    private static string NormalizeLoopbackHost(string host)
    {
        string normalized = (host ?? string.Empty).Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) &&
            normalized.EndsWith("]", StringComparison.Ordinal) &&
            normalized.Length > 2)
        {
            normalized = normalized[1..^1];
        }

        if (string.Equals(normalized, "0:0:0:0:0:0:0:1", StringComparison.OrdinalIgnoreCase))
        {
            return "::1";
        }

        return normalized;
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
}
