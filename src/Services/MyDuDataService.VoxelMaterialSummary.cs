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
    public async Task<string> ExportBlueprintVoxelMaterialSummaryJsonAsync(
        ulong blueprintId,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (blueprintId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(blueprintId), "Blueprint id must be > 0.");
        }

        (JsonArray cells, Uri sourceEndpoint, string fetchNote) = await FetchVoxelDumpCellsAsync(
            endpointTemplate,
            blueprintImportEndpoint,
            "blueprints",
            blueprintId,
            cancellationToken);

        VoxelMaterialSummary summary = BlueprintVoxelMaterialDecoder.Summarize(cells);
        summary = await EnrichVoxelMaterialNamesAsync(summary, nameLookupOptions, cancellationToken);
        return BuildVoxelMaterialSummaryJson(
            scope: "blueprint",
            targetId: blueprintId,
            summary);
    }

    public async Task<string> ExportConstructVoxelMaterialSummaryJsonAsync(
        ulong constructId,
        string endpointTemplate,
        string? blueprintImportEndpoint,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (constructId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(constructId), "Construct id must be > 0.");
        }

        (JsonArray cells, Uri sourceEndpoint, string fetchNote) = await FetchVoxelDumpCellsAsync(
            endpointTemplate,
            blueprintImportEndpoint,
            "constructs",
            constructId,
            cancellationToken);

        VoxelMaterialSummary summary = BlueprintVoxelMaterialDecoder.Summarize(cells);
        summary = await EnrichVoxelMaterialNamesAsync(summary, nameLookupOptions, cancellationToken);
        return BuildVoxelMaterialSummaryJson(
            scope: "construct",
            targetId: constructId,
            summary);
    }

    private async Task<VoxelMaterialSummary> EnrichVoxelMaterialNamesAsync(
        VoxelMaterialSummary summary,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (nameLookupOptions is null || summary.Materials.Count == 0)
        {
            return summary;
        }

        ulong[] numericIds = summary.Materials
            .Select(material => material.MaterialId)
            .Where(id => ulong.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            .Select(id => ulong.Parse(id, NumberStyles.Integer, CultureInfo.InvariantCulture))
            .Where(id => id > 0UL)
            .Distinct()
            .ToArray();
        if (numericIds.Length == 0)
        {
            return summary;
        }

        IReadOnlyDictionary<ulong, string> displayNamesById;
        try
        {
            displayNamesById = await GetItemDefinitionDisplayNamesAsync(
                nameLookupOptions,
                numericIds,
                cancellationToken);
        }
        catch
        {
            return summary;
        }

        if (displayNamesById.Count == 0)
        {
            return summary;
        }

        var enriched = new List<VoxelMaterialEntry>(summary.Materials.Count);
        foreach (VoxelMaterialEntry material in summary.Materials)
        {
            if (!ulong.TryParse(
                    material.MaterialId,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out ulong numericId) ||
                !displayNamesById.TryGetValue(numericId, out string? displayName) ||
                string.IsNullOrWhiteSpace(displayName))
            {
                enriched.Add(material);
                continue;
            }

            string resolved = displayName.Trim();
            enriched.Add(material with { MaterialName = resolved });
        }

        return summary with { Materials = enriched };
    }

    private async Task<(JsonArray Cells, Uri SourceEndpoint, string FetchNote)> FetchVoxelDumpCellsAsync(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        string dumpKindSegment,
        ulong objectId,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<Uri> dumpCandidates = BuildVoxelDumpEndpointCandidates(
            endpointTemplate,
            blueprintImportEndpoint,
            objectId,
            dumpKindSegment);
        if (dumpCandidates.Count == 0)
        {
            throw new InvalidOperationException("No voxel dump endpoint candidates were generated.");
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

                JsonNode? parsedDump;
                try
                {
                    parsedDump = JsonNode.Parse(dumpText);
                }
                catch (Exception parseEx)
                {
                    failures.Add(
                        $"'{dumpEndpoint}' => invalid JSON dump ({BuildSingleLineExceptionPreview(parseEx)})");
                    continue;
                }

                if (!TryExtractVoxelCellsArray(parsedDump, out JsonArray cells, out string shapeNote))
                {
                    failures.Add($"'{dumpEndpoint}' => dump JSON does not contain a cells array.");
                    continue;
                }

                string fetchNote = string.IsNullOrWhiteSpace(shapeNote)
                    ? $"Voxel dump exported from '{dumpEndpoint}'."
                    : $"Voxel dump exported from '{dumpEndpoint}' ({shapeNote}).";
                return (cells, dumpEndpoint, fetchNote);
            }
            catch (Exception ex)
            {
                failures.Add($"'{dumpEndpoint}' => {BuildSingleLineExceptionPreview(ex)}");
            }
        }

        throw new InvalidOperationException(
            $"Voxel dump unavailable for object {objectId.ToString(CultureInfo.InvariantCulture)} " +
            $"(kind={dumpKindSegment}): {string.Join(" | ", failures)}");
    }

    private static bool TryExtractVoxelCellsArray(
        JsonNode? parsedDump,
        out JsonArray cells,
        out string note)
    {
        cells = new JsonArray();
        note = string.Empty;

        if (parsedDump is JsonObject dumpObject &&
            TryGetJsonPropertyIgnoreCase(dumpObject, "cells", out _, out JsonNode? cellsNode) &&
            cellsNode is JsonArray cellsArray)
        {
            cells = cellsArray;
            return true;
        }

        if (parsedDump is JsonArray rootArray)
        {
            cells = rootArray;
            note = "root is array";
            return true;
        }

        return false;
    }

    private IReadOnlyList<Uri> BuildVoxelDumpEndpointCandidates(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong objectId,
        string dumpKindSegment)
    {
        var gameplayCandidates = new List<Uri>();
        try
        {
            gameplayCandidates.AddRange(BuildBlueprintImportEndpointCandidates(
                endpointTemplate,
                blueprintImportEndpoint,
                creatorPlayerId: 2UL,
                creatorOrganizationId: 0UL));
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
            IReadOnlyList<Uri> voxelImportCandidates = BuildVoxelServiceJsonImportEndpointCandidates(
                gameplayCandidate,
                objectId,
                clearExistingCells: false);
            foreach (Uri voxelImportCandidate in voxelImportCandidates)
            {
                var builder = new UriBuilder(voxelImportCandidate)
                {
                    Path = $"/voxels/{dumpKindSegment}/{objectId.ToString(CultureInfo.InvariantCulture)}/dump.json",
                    Query = string.Empty
                };

                Uri dumpCandidate = builder.Uri;
                if (seen.Add(dumpCandidate.AbsoluteUri))
                {
                    dumpCandidates.Add(dumpCandidate);
                }
            }
        }

        return dumpCandidates;
    }

    private static string BuildVoxelMaterialSummaryJson(
        string scope,
        ulong targetId,
        VoxelMaterialSummary summary)
    {
        var root = new JsonObject
        {
            ["scope"] = scope,
            ["targetId"] = targetId.ToString(CultureInfo.InvariantCulture),
            ["chunksTotal"] = summary.ChunkCount,
            ["chunksParsed"] = summary.ParsedChunkCount,
            ["chunksFailed"] = summary.FailedChunkCount,
            ["totalVoxelBlocks"] = summary.TotalVoxelBlocks,
            ["totalVolumeLiters"] = summary.TotalVolumeLiters
        };

        var materialsArray = new JsonArray();
        foreach (VoxelMaterialEntry material in summary.Materials)
        {
            materialsArray.Add(new JsonObject
            {
                ["materialId"] = material.MaterialId,
                ["materialName"] = material.MaterialName,
                ["voxelBlocks"] = material.VoxelBlocks,
                ["volumeLiters"] = material.VolumeLiters
            });
        }

        root["materials"] = materialsArray;

        if (summary.Warnings.Count > 0)
        {
            var warningsArray = new JsonArray();
            foreach (string warning in summary.Warnings)
            {
                warningsArray.Add(warning);
            }

            root["warnings"] = warningsArray;
        }

        return root.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }
}
