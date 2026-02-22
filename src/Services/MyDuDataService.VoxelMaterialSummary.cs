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
    private const double MetadataQuantityUnitsPerCubicMeter = 16777216d; // 2^24 fixed-point m^3
    private const string VolumeSourceDecodedEstimate = "decoded_estimate";
    private const string VolumeSourceMetaBlobOffline = "meta_blob_offline";
    private const string VolumeSourceMetadataEndpoint = "voxel_metadata_endpoint";

    private sealed record VoxelMetadataMaterialSummary(
        string SourceKind,
        Uri? SourceEndpoint,
        IReadOnlyDictionary<ulong, ulong> MaterialQuantities,
        IReadOnlyDictionary<ulong, string> MaterialTokens,
        double TotalVolumeLiters);

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
        VoxelMetadataMaterialSummary? decodedMeta = TryDecodeMetaBlobMaterialSummary(cells);
        VoxelMetadataMaterialSummary? endpointMetadata = await TryFetchVoxelMetadataMaterialSummaryAsync(
            endpointTemplate,
            blueprintImportEndpoint,
            "blueprints",
            blueprintId,
            cancellationToken);
        VoxelMetadataMaterialSummary? metadata = endpointMetadata ?? decodedMeta;
        IReadOnlyDictionary<ulong, string> materialNameMap = await BuildMaterialNameMapForSummaryAsync(
            summary,
            metadata,
            nameLookupOptions,
            cancellationToken);
        return BuildVoxelMaterialSummaryJson(
            scope: "blueprint",
            targetId: blueprintId,
            summary,
            metadata,
            materialNameMap);
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
        VoxelMetadataMaterialSummary? decodedMeta = TryDecodeMetaBlobMaterialSummary(cells);
        VoxelMetadataMaterialSummary? endpointMetadata = await TryFetchVoxelMetadataMaterialSummaryAsync(
            endpointTemplate,
            blueprintImportEndpoint,
            "constructs",
            constructId,
            cancellationToken);
        VoxelMetadataMaterialSummary? metadata = endpointMetadata ?? decodedMeta;
        IReadOnlyDictionary<ulong, string> materialNameMap = await BuildMaterialNameMapForSummaryAsync(
            summary,
            metadata,
            nameLookupOptions,
            cancellationToken);
        return BuildVoxelMaterialSummaryJson(
            scope: "construct",
            targetId: constructId,
            summary,
            metadata,
            materialNameMap);
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
        VoxelMaterialSummary summary,
        VoxelMetadataMaterialSummary? metadata,
        IReadOnlyDictionary<ulong, string> metadataMaterialNameMap)
    {
        double decodedTotalLiters = summary.TotalVolumeLiters;
        double? metadataTotalLiters = metadata?.TotalVolumeLiters;
        double primaryTotalLiters = metadataTotalLiters ?? decodedTotalLiters;

        var root = new JsonObject
        {
            ["scope"] = scope,
            ["targetId"] = targetId.ToString(CultureInfo.InvariantCulture),
            ["chunksTotal"] = summary.ChunkCount,
            ["chunksParsed"] = summary.ParsedChunkCount,
            ["chunksFailed"] = summary.FailedChunkCount,
            ["totalVoxelBlocks"] = summary.TotalVoxelBlocks,
            ["totalVolumeLiters"] = primaryTotalLiters,
            ["totalVolumeLitersDecoded"] = decodedTotalLiters,
            ["totalVolumeLitersMetadata"] = metadataTotalLiters is null
                ? null
                : JsonValue.Create(metadataTotalLiters.Value),
            ["volumeSource"] = metadata?.SourceKind ?? VolumeSourceDecodedEstimate
        };
        if (metadata is not null)
        {
            root["metadataSourceKind"] = metadata.SourceKind;
            if (metadata.SourceEndpoint is not null)
            {
                root["metadataSourceEndpoint"] = metadata.SourceEndpoint.AbsoluteUri;
            }

            root["metadataQuantityUnitsPerCubicMeter"] = MetadataQuantityUnitsPerCubicMeter;
        }

        var decodedById = new Dictionary<string, VoxelMaterialEntry>(StringComparer.OrdinalIgnoreCase);
        foreach (VoxelMaterialEntry material in summary.Materials)
        {
            decodedById[material.MaterialId] = material;
        }

        var allMaterialIds = new HashSet<string>(decodedById.Keys, StringComparer.OrdinalIgnoreCase);
        if (metadata is not null)
        {
            foreach (ulong metadataMaterialId in metadata.MaterialQuantities.Keys)
            {
                allMaterialIds.Add(metadataMaterialId.ToString(CultureInfo.InvariantCulture));
            }
        }

        List<string> orderedIds = allMaterialIds
            .OrderByDescending(id =>
            {
                double decoded = decodedById.TryGetValue(id, out VoxelMaterialEntry? entry)
                    ? entry.VolumeLiters
                    : 0d;
                if (metadata is null ||
                    !ulong.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong numericId) ||
                    !metadata.MaterialQuantities.TryGetValue(numericId, out ulong quantity))
                {
                    return decoded;
                }

                return ConvertMetadataQuantityToLiters(quantity);
            })
            .ThenBy(static id => id, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var materialsArray = new JsonArray();
        foreach (string materialId in orderedIds)
        {
            decodedById.TryGetValue(materialId, out VoxelMaterialEntry? decodedMaterial);
            long decodedVoxelBlocks = decodedMaterial?.VoxelBlocks ?? 0L;
            double decodedVolumeLiters = decodedMaterial?.VolumeLiters ?? 0d;
            string materialName = decodedMaterial?.MaterialName ?? string.Empty;

            ulong? numericMaterialId = ulong.TryParse(
                materialId,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out ulong parsedMaterialId)
                ? parsedMaterialId
                : null;

            ulong? metadataQuantity = null;
            double? metadataVolumeLiters = null;
            if (metadata is not null &&
                numericMaterialId.HasValue &&
                metadata.MaterialQuantities.TryGetValue(numericMaterialId.Value, out ulong quantity))
            {
                metadataQuantity = quantity;
                metadataVolumeLiters = ConvertMetadataQuantityToLiters(quantity);
                if (string.IsNullOrWhiteSpace(materialName) &&
                    metadataMaterialNameMap.TryGetValue(numericMaterialId.Value, out string? resolvedName) &&
                    !string.IsNullOrWhiteSpace(resolvedName))
                {
                    materialName = resolvedName.Trim();
                }
            }

            if (string.IsNullOrWhiteSpace(materialName))
            {
                materialName = numericMaterialId.HasValue
                    ? $"Unknown[{materialId}]"
                    : materialId;
            }

            double volumeLiters = metadataVolumeLiters ?? decodedVolumeLiters;
            double deltaLiters = metadataVolumeLiters.HasValue
                ? decodedVolumeLiters - metadataVolumeLiters.Value
                : 0d;

            materialsArray.Add(new JsonObject
            {
                ["materialId"] = materialId,
                ["materialName"] = materialName,
                ["voxelBlocks"] = decodedVoxelBlocks,
                ["volumeLiters"] = volumeLiters,
                ["volumeLitersDecoded"] = decodedVolumeLiters,
                ["volumeLitersMetadata"] = metadataVolumeLiters is null
                    ? null
                    : JsonValue.Create(metadataVolumeLiters.Value),
                ["metadataQuantity"] = metadataQuantity is null
                    ? null
                    : JsonValue.Create(metadataQuantity.Value),
                ["volumeSource"] = metadataVolumeLiters is null
                    ? VolumeSourceDecodedEstimate
                    : metadata?.SourceKind ?? VolumeSourceDecodedEstimate,
                ["volumeDeltaLitersDecodedMinusMetadata"] = metadataVolumeLiters is null
                    ? null
                    : JsonValue.Create(deltaLiters)
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

    private async Task<IReadOnlyDictionary<ulong, string>> BuildMaterialNameMapForSummaryAsync(
        VoxelMaterialSummary summary,
        VoxelMetadataMaterialSummary? metadata,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        var byId = new Dictionary<ulong, string>();

        foreach (VoxelMaterialEntry material in summary.Materials)
        {
            if (!ulong.TryParse(
                    material.MaterialId,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out ulong numericId) ||
                numericId == 0UL)
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(material.MaterialName))
            {
                byId[numericId] = material.MaterialName.Trim();
            }
        }

        if (metadata is not null)
        {
            foreach ((ulong materialId, string token) in metadata.MaterialTokens)
            {
                if (materialId == 0UL || string.IsNullOrWhiteSpace(token))
                {
                    continue;
                }

                if (!byId.ContainsKey(materialId))
                {
                    byId[materialId] = token.Trim();
                }
            }
        }

        if (metadata is null || metadata.MaterialQuantities.Count == 0 || nameLookupOptions is null)
        {
            return byId;
        }

        ulong[] missingIds = metadata.MaterialQuantities.Keys
            .Where(id => id > 0UL && !byId.ContainsKey(id))
            .Distinct()
            .ToArray();
        if (missingIds.Length == 0)
        {
            return byId;
        }

        try
        {
            IReadOnlyDictionary<ulong, string> resolved = await GetItemDefinitionDisplayNamesAsync(
                nameLookupOptions,
                missingIds,
                cancellationToken);
            foreach ((ulong id, string displayName) in resolved)
            {
                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    byId[id] = displayName.Trim();
                }
            }
        }
        catch
        {
            // Keep summary names if metadata-name lookup fails.
        }

        return byId;
    }

    private static VoxelMetadataMaterialSummary? TryDecodeMetaBlobMaterialSummary(JsonArray cells)
    {
        if (cells is null || cells.Count == 0)
        {
            return null;
        }

        if (!BlueprintVoxelMaterialDecoder.TrySummarizeMetaMaterialQuantities(
                cells,
                out VoxelMetaMaterialSummary metaSummary,
                out _))
        {
            return null;
        }

        if (metaSummary.MaterialQuantities.Count == 0)
        {
            return null;
        }

        double totalLiters = metaSummary.MaterialQuantities
            .Values
            .Sum(static quantity => ConvertMetadataQuantityToLiters(quantity));
        return new VoxelMetadataMaterialSummary(
            SourceKind: VolumeSourceMetaBlobOffline,
            SourceEndpoint: null,
            MaterialQuantities: metaSummary.MaterialQuantities,
            MaterialTokens: metaSummary.MaterialTokens,
            TotalVolumeLiters: totalLiters);
    }

    private async Task<VoxelMetadataMaterialSummary?> TryFetchVoxelMetadataMaterialSummaryAsync(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        string dumpKindSegment,
        ulong objectId,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<Uri> metadataCandidates = BuildVoxelMetadataEndpointCandidates(
            endpointTemplate,
            blueprintImportEndpoint,
            objectId,
            dumpKindSegment);
        if (metadataCandidates.Count == 0)
        {
            return null;
        }

        foreach (Uri metadataEndpoint in metadataCandidates)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, metadataEndpoint)
                {
                    Version = HttpVersion.Version11,
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
                };
                request.Headers.TryAddWithoutValidation("Accept", "application/json");

                using HttpResponseMessage response = await _httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseContentRead,
                    cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    continue;
                }

                string payload = await response.Content.ReadAsStringAsync(cancellationToken);
                JsonNode? parsed = JsonNode.Parse(payload);
                if (!TryParseVoxelMetadataMaterialQuantities(parsed, out Dictionary<ulong, ulong> quantities))
                {
                    continue;
                }

                double totalLiters = quantities.Values.Sum(static quantity => ConvertMetadataQuantityToLiters(quantity));
                return new VoxelMetadataMaterialSummary(
                    SourceKind: VolumeSourceMetadataEndpoint,
                    SourceEndpoint: metadataEndpoint,
                    MaterialQuantities: quantities,
                    MaterialTokens: new Dictionary<ulong, string>(),
                    TotalVolumeLiters: totalLiters);
            }
            catch
            {
                // Try next endpoint candidate.
            }
        }

        return null;
    }

    private IReadOnlyList<Uri> BuildVoxelMetadataEndpointCandidates(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong objectId,
        string dumpKindSegment)
    {
        IReadOnlyList<Uri> dumpCandidates = BuildVoxelDumpEndpointCandidates(
            endpointTemplate,
            blueprintImportEndpoint,
            objectId,
            dumpKindSegment);

        var metadataCandidates = new List<Uri>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri dumpCandidate in dumpCandidates)
        {
            var builder = new UriBuilder(dumpCandidate)
            {
                Path = $"/voxels/{dumpKindSegment}/{objectId.ToString(CultureInfo.InvariantCulture)}/metadata",
                Query = string.Empty
            };

            Uri metadataEndpoint = builder.Uri;
            if (seen.Add(metadataEndpoint.AbsoluteUri))
            {
                metadataCandidates.Add(metadataEndpoint);
            }
        }

        return metadataCandidates;
    }

    private static bool TryParseVoxelMetadataMaterialQuantities(
        JsonNode? parsed,
        out Dictionary<ulong, ulong> quantitiesByMaterialId)
    {
        quantitiesByMaterialId = new Dictionary<ulong, ulong>();
        if (parsed is not JsonObject root ||
            root["materialStats"] is not JsonArray statsArray)
        {
            return false;
        }

        foreach (JsonNode? statNode in statsArray)
        {
            if (statNode is not JsonObject statObject ||
                !TryReadUInt64(statObject["id"], out ulong materialId) ||
                materialId == 0UL ||
                !TryReadUInt64(statObject["quantity"], out ulong quantity))
            {
                continue;
            }

            quantitiesByMaterialId[materialId] = quantitiesByMaterialId.TryGetValue(materialId, out ulong current)
                ? current + quantity
                : quantity;
        }

        return quantitiesByMaterialId.Count > 0;
    }

    private static bool TryReadUInt64(JsonNode? node, out ulong value)
    {
        value = 0UL;
        if (node is null)
        {
            return false;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<ulong>(out ulong asUlong))
            {
                value = asUlong;
                return true;
            }

            if (scalar.TryGetValue<long>(out long asLong) && asLong >= 0L)
            {
                value = (ulong)asLong;
                return true;
            }

            if (scalar.TryGetValue<int>(out int asInt) && asInt >= 0)
            {
                value = (uint)asInt;
                return true;
            }

            if (scalar.TryGetValue<string>(out string? asString) &&
                !string.IsNullOrWhiteSpace(asString) &&
                ulong.TryParse(asString.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed))
            {
                value = parsed;
                return true;
            }
        }

        return false;
    }

    private static double ConvertMetadataQuantityToLiters(ulong quantity)
    {
        return (quantity / MetadataQuantityUnitsPerCubicMeter) * 1000d;
    }
}
