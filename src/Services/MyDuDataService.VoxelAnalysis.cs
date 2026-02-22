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
    private const double DecodedLitersPerVoxelBlockEstimate = 15.625d;

    public async Task<string> ExportConstructVoxelAnalysisJsonAsync(
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

        JsonObject analysis = BlueprintVoxelAnalyzer.AnalyzeVoxelCells(cells);
        JsonObject summary = BlueprintVoxelAnalyzer.BuildSimplifiedSummary(cells, analysis);
        await EnrichVoxelAnalysisMaterialNamesAsync(summary, nameLookupOptions, cancellationToken);
        VoxelMetadataMaterialSummary? decodedMeta = TryDecodeMetaBlobMaterialSummary(cells);
        VoxelMetadataMaterialSummary? endpointMetadata = await TryFetchVoxelMetadataMaterialSummaryAsync(
            endpointTemplate,
            blueprintImportEndpoint,
            "constructs",
            constructId,
            cancellationToken);
        VoxelMetadataMaterialSummary? metadata = endpointMetadata ?? decodedMeta;
        await AttachMaterialTotalsToAnalysisSummaryAsync(summary, metadata, nameLookupOptions, cancellationToken);
        var root = new JsonObject
        {
            ["scope"] = "construct",
            ["targetId"] = constructId.ToString(CultureInfo.InvariantCulture),
            ["sourceEndpoint"] = sourceEndpoint.AbsoluteUri,
            ["fetchNote"] = fetchNote,
            ["summary"] = summary,
            ["analysis"] = analysis
        };

        return root.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    public async Task<string> ExportBlueprintVoxelAnalysisJsonFromJsonContentAsync(
        string jsonContent,
        string sourceName,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(jsonContent))
        {
            throw new ArgumentException("Blueprint JSON content is empty.", nameof(jsonContent));
        }

        JsonNode? parsed = JsonNode.Parse(jsonContent);
        if (parsed is not JsonObject document)
        {
            throw new InvalidOperationException("Blueprint JSON root must be an object.");
        }

        JsonObject analysis = BlueprintVoxelAnalyzer.AnalyzeBlueprintDocument(document);
        JsonArray cells = ResolveVoxelCellsFromDocument(document);
        JsonObject summary = BlueprintVoxelAnalyzer.BuildSimplifiedSummary(cells, analysis);
        await EnrichVoxelAnalysisMaterialNamesAsync(summary, nameLookupOptions, cancellationToken);
        VoxelMetadataMaterialSummary? decodedMeta = TryDecodeMetaBlobMaterialSummary(cells);
        await AttachMaterialTotalsToAnalysisSummaryAsync(summary, decodedMeta, nameLookupOptions, cancellationToken);
        JsonObject extracted = BlueprintVoxelAnalyzer.ExtractCoreAndVoxels(document);

        var root = new JsonObject
        {
            ["scope"] = "blueprint_json_file",
            ["sourceName"] = sourceName ?? string.Empty,
            ["summary"] = summary,
            ["analysis"] = analysis,
            ["extracted"] = extracted
        };

        return root.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    private async Task EnrichVoxelAnalysisMaterialNamesAsync(
        JsonObject summary,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (summary is null ||
            nameLookupOptions is null ||
            TryGetJsonProperty(summary, "blob_instances") is not JsonArray instances ||
            instances.Count == 0)
        {
            return;
        }

        var numericIds = new HashSet<ulong>();
        foreach (JsonNode? instanceNode in instances)
        {
            if (instanceNode is not JsonObject instance ||
                TryGetJsonProperty(instance, "materials") is not JsonArray materials)
            {
                continue;
            }

            foreach (JsonNode? materialNode in materials)
            {
                if (materialNode is not JsonObject material)
                {
                    continue;
                }

                string? materialIdText = ReadJsonScalarString(TryGetJsonProperty(material, "material_id"));
                if (string.IsNullOrWhiteSpace(materialIdText))
                {
                    continue;
                }

                if (ulong.TryParse(
                        materialIdText.Trim(),
                        NumberStyles.Integer,
                        CultureInfo.InvariantCulture,
                        out ulong materialId) &&
                    materialId > 0UL)
                {
                    numericIds.Add(materialId);
                }
            }
        }

        if (numericIds.Count == 0)
        {
            return;
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
            return;
        }

        if (displayNamesById.Count == 0)
        {
            return;
        }

        foreach (JsonNode? instanceNode in instances)
        {
            if (instanceNode is not JsonObject instance ||
                TryGetJsonProperty(instance, "materials") is not JsonArray materials)
            {
                continue;
            }

            foreach (JsonNode? materialNode in materials)
            {
                if (materialNode is not JsonObject material)
                {
                    continue;
                }

                string? materialIdText = ReadJsonScalarString(TryGetJsonProperty(material, "material_id"));
                if (string.IsNullOrWhiteSpace(materialIdText))
                {
                    continue;
                }

                if (!ulong.TryParse(
                        materialIdText.Trim(),
                        NumberStyles.Integer,
                        CultureInfo.InvariantCulture,
                        out ulong materialId) ||
                    materialId == 0UL ||
                    !displayNamesById.TryGetValue(materialId, out string? displayName) ||
                    string.IsNullOrWhiteSpace(displayName))
                {
                    continue;
                }

                material["material_name"] = displayName.Trim();
            }
        }
    }

    private async Task AttachMaterialTotalsToAnalysisSummaryAsync(
        JsonObject summary,
        VoxelMetadataMaterialSummary? metadata,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        if (summary is null || TryGetJsonProperty(summary, "blob_instances") is not JsonArray instances)
        {
            return;
        }

        var decodedBlocksByMaterialId = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var materialNameByNumericId = new Dictionary<ulong, string>();
        foreach (JsonNode? instanceNode in instances)
        {
            if (instanceNode is not JsonObject instance ||
                TryGetJsonProperty(instance, "materials") is not JsonArray materials)
            {
                continue;
            }

            foreach (JsonNode? materialNode in materials)
            {
                if (materialNode is not JsonObject material)
                {
                    continue;
                }

                string materialId = ReadJsonScalarString(TryGetJsonProperty(material, "material_id"))?.Trim() ?? string.Empty;
                if (string.IsNullOrWhiteSpace(materialId))
                {
                    continue;
                }

                long voxelBlocks = ReadJsonInt64(TryGetJsonProperty(material, "voxel_blocks")) ?? 0L;
                decodedBlocksByMaterialId[materialId] = decodedBlocksByMaterialId.TryGetValue(materialId, out long current)
                    ? current + voxelBlocks
                    : voxelBlocks;

                if (!ulong.TryParse(materialId, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong numericId) ||
                    numericId == 0UL)
                {
                    continue;
                }

                string? materialName = ReadJsonScalarString(TryGetJsonProperty(material, "material_name"));
                if (!string.IsNullOrWhiteSpace(materialName))
                {
                    materialNameByNumericId[numericId] = materialName.Trim();
                }
            }
        }

        IReadOnlyDictionary<ulong, string> resolvedNames = await ResolveVoxelAnalysisMaterialNamesAsync(
            decodedBlocksByMaterialId.Keys,
            metadata,
            materialNameByNumericId,
            nameLookupOptions,
            cancellationToken);

        var allMaterialIds = new HashSet<string>(decodedBlocksByMaterialId.Keys, StringComparer.OrdinalIgnoreCase);
        if (metadata is not null)
        {
            foreach (ulong metadataId in metadata.MaterialQuantities.Keys)
            {
                allMaterialIds.Add(metadataId.ToString(CultureInfo.InvariantCulture));
            }
        }

        var mergedRows = new List<JsonObject>(allMaterialIds.Count);
        foreach (string materialId in allMaterialIds)
        {
            long decodedBlocks = decodedBlocksByMaterialId.TryGetValue(materialId, out long blocks) ? blocks : 0L;
            double decodedLiters = decodedBlocks * DecodedLitersPerVoxelBlockEstimate;

            ulong? numericId = ulong.TryParse(materialId, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsedId)
                ? parsedId
                : null;

            ulong? metadataQuantity = null;
            double? metadataLiters = null;
            if (metadata is not null &&
                numericId.HasValue &&
                metadata.MaterialQuantities.TryGetValue(numericId.Value, out ulong quantity))
            {
                metadataQuantity = quantity;
                metadataLiters = ConvertMetadataQuantityToLiters(quantity);
            }

            string materialName = string.Empty;
            if (numericId.HasValue && resolvedNames.TryGetValue(numericId.Value, out string? resolvedName))
            {
                materialName = resolvedName;
            }
            else if (numericId.HasValue && materialNameByNumericId.TryGetValue(numericId.Value, out string? existingName))
            {
                materialName = existingName;
            }
            else
            {
                materialName = $"Unknown[{materialId}]";
            }

            mergedRows.Add(new JsonObject
            {
                ["material_id"] = materialId,
                ["material_name"] = materialName,
                ["decoded_voxel_blocks"] = decodedBlocks,
                ["decoded_volume_liters"] = decodedLiters,
                ["metadata_volume_liters"] = metadataLiters is null ? null : JsonValue.Create(metadataLiters.Value),
                ["metadata_quantity"] = metadataQuantity is null ? null : JsonValue.Create(metadataQuantity.Value),
                ["selected_volume_liters"] = metadataLiters ?? decodedLiters,
                ["volume_source"] = metadataLiters is null
                    ? VolumeSourceDecodedEstimate
                    : metadata?.SourceKind ?? VolumeSourceDecodedEstimate
            });
        }

        JsonArray materialsArray = new();
        foreach (JsonObject row in mergedRows
                     .OrderByDescending(static row => ReadJsonDouble(row["selected_volume_liters"]) ?? 0d)
                     .ThenBy(static row => ReadJsonScalarString(row["material_name"]), StringComparer.OrdinalIgnoreCase))
        {
            materialsArray.Add(row);
        }

        long decodedTotalBlocks = decodedBlocksByMaterialId.Values.Sum();
        double decodedTotalLiters = decodedTotalBlocks * DecodedLitersPerVoxelBlockEstimate;
        double? metadataTotalLiters = metadata?.TotalVolumeLiters;

        summary["material_totals"] = new JsonObject
        {
            ["decoded_total_voxel_blocks"] = decodedTotalBlocks,
            ["decoded_total_volume_liters"] = decodedTotalLiters,
            ["decoded_liters_per_voxel_block"] = DecodedLitersPerVoxelBlockEstimate,
            ["metadata_total_volume_liters"] = metadataTotalLiters is null
                ? null
                : JsonValue.Create(metadataTotalLiters.Value),
            ["selected_total_volume_liters"] = metadataTotalLiters ?? decodedTotalLiters,
            ["volume_source"] = metadata?.SourceKind ?? VolumeSourceDecodedEstimate,
            ["metadata_source_kind"] = metadata?.SourceKind,
            ["metadata_source_endpoint"] = metadata?.SourceEndpoint?.AbsoluteUri,
            ["metadata_quantity_units_per_cubic_meter"] = metadata is null
                ? null
                : JsonValue.Create(MetadataQuantityUnitsPerCubicMeter),
            ["materials"] = materialsArray
        };
    }

    private async Task<IReadOnlyDictionary<ulong, string>> ResolveVoxelAnalysisMaterialNamesAsync(
        IEnumerable<string> decodedMaterialIds,
        VoxelMetadataMaterialSummary? metadata,
        IReadOnlyDictionary<ulong, string> existingNames,
        DataConnectionOptions? nameLookupOptions,
        CancellationToken cancellationToken)
    {
        var byId = new Dictionary<ulong, string>(existingNames);
        if (decodedMaterialIds is not null)
        {
            foreach (string materialId in decodedMaterialIds)
            {
                if (string.IsNullOrWhiteSpace(materialId) ||
                    !ulong.TryParse(materialId, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong numericId) ||
                    numericId == 0UL)
                {
                    continue;
                }

                if (!byId.ContainsKey(numericId))
                {
                    byId[numericId] = $"Unknown[{numericId.ToString(CultureInfo.InvariantCulture)}]";
                }
            }
        }

        if (metadata is not null)
        {
            foreach ((ulong materialId, string token) in metadata.MaterialTokens)
            {
                if (materialId == 0UL ||
                    string.IsNullOrWhiteSpace(token) ||
                    byId.ContainsKey(materialId))
                {
                    continue;
                }

                byId[materialId] = token.Trim();
            }

            foreach (ulong materialId in metadata.MaterialQuantities.Keys)
            {
                if (materialId == 0UL || byId.ContainsKey(materialId))
                {
                    continue;
                }

                byId[materialId] = $"Unknown[{materialId.ToString(CultureInfo.InvariantCulture)}]";
            }
        }

        if (nameLookupOptions is null)
        {
            return byId;
        }

        ulong[] unresolved = byId
            .Where(static pair => pair.Key > 0UL && pair.Value.StartsWith("Unknown[", StringComparison.Ordinal))
            .Select(static pair => pair.Key)
            .Distinct()
            .ToArray();
        if (unresolved.Length == 0)
        {
            return byId;
        }

        try
        {
            IReadOnlyDictionary<ulong, string> resolved = await GetItemDefinitionDisplayNamesAsync(
                nameLookupOptions,
                unresolved,
                cancellationToken);
            foreach ((ulong id, string name) in resolved)
            {
                if (!string.IsNullOrWhiteSpace(name))
                {
                    byId[id] = name.Trim();
                }
            }
        }
        catch
        {
            // Keep existing fallback names if DB lookup fails.
        }

        return byId;
    }

    private static JsonArray ResolveVoxelCellsFromDocument(JsonObject doc)
    {
        JsonNode? voxelData = TryGetJsonProperty(doc, "VoxelData");
        if (voxelData is JsonArray topArray)
        {
            return topArray;
        }

        if (voxelData is JsonObject voxelObject &&
            TryGetJsonProperty(voxelObject, "cells") is JsonArray nestedCells)
        {
            return nestedCells;
        }

        if (TryGetJsonProperty(doc, "cells") is JsonArray rootCells)
        {
            return rootCells;
        }

        return new JsonArray();
    }

    private static JsonNode? TryGetJsonProperty(JsonObject obj, string propertyName)
    {
        foreach (KeyValuePair<string, JsonNode?> kvp in obj)
        {
            if (string.Equals(kvp.Key, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                return kvp.Value;
            }
        }

        return null;
    }

    private static string? ReadJsonScalarString(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out string? asString))
            {
                return asString;
            }

            if (scalar.TryGetValue<long>(out long asLong))
            {
                return asLong.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<int>(out int asInt))
            {
                return asInt.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<double>(out double asDouble))
            {
                return asDouble.ToString("R", CultureInfo.InvariantCulture);
            }
        }

        return node.ToJsonString();
    }

    private static long? ReadJsonInt64(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<long>(out long asLong))
            {
                return asLong;
            }

            if (scalar.TryGetValue<int>(out int asInt))
            {
                return asInt;
            }

            if (scalar.TryGetValue<double>(out double asDouble))
            {
                if (double.IsNaN(asDouble) || double.IsInfinity(asDouble))
                {
                    return null;
                }

                return (long)Math.Round(asDouble);
            }

            if (scalar.TryGetValue<string>(out string? asString) &&
                long.TryParse(asString, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    private static double? ReadJsonDouble(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<double>(out double asDouble))
            {
                return double.IsFinite(asDouble) ? asDouble : null;
            }

            if (scalar.TryGetValue<long>(out long asLong))
            {
                return asLong;
            }

            if (scalar.TryGetValue<int>(out int asInt))
            {
                return asInt;
            }

            if (scalar.TryGetValue<string>(out string? asString) &&
                double.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed))
            {
                return double.IsFinite(parsed) ? parsed : null;
            }
        }

        return null;
    }
}
