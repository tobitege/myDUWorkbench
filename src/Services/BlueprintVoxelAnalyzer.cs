using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;

namespace myDUWorkbench.Services;

public sealed record BlueprintVoxelAnalysisOptions(
    int SampleLimit = 25,
    string CoordinateOrigin = "center",
    double VoxelsPerMeter = 4d,
    bool CheckBuildingZone = false);

public static class BlueprintVoxelAnalyzer
{
    private const uint MagicVoxelMetaData = 0x9F81F3C0;

    private const string CoordOriginCenter = "center";
    private const string CoordOriginZero = "zero";
    private const string SizeSemantics = "half_extent_voxels";
    private const string CellAxisUnitAssumption = "voxels";
    private const int MaxMalformedBlobPaths = 50;

    private readonly record struct Vec3(double X, double Y, double Z);
    private readonly record struct Aabb(Vec3 Min, Vec3 Max);
    private readonly record struct CellGeom(long X, long Y, long Z, int H, double Span, Aabb Aabb);

    public static JsonObject ExtractCoreAndVoxels(JsonObject doc)
    {
        JsonNode? model = TryGetPropertyIgnoreCase(doc, "Model");
        JsonNode? voxelData = TryGetPropertyIgnoreCase(doc, "VoxelData");

        return new JsonObject
        {
            ["Model"] = model?.DeepClone(),
            ["VoxelData"] = voxelData?.DeepClone() ?? new JsonArray()
        };
    }

    public static JsonObject AnalyzeBlueprintDocument(JsonObject doc, BlueprintVoxelAnalysisOptions? options = null)
    {
        if (doc is null)
        {
            throw new ArgumentNullException(nameof(doc));
        }

        JsonArray voxelCells = ResolveVoxelCells(doc);
        JsonObject? model = ResolveModel(doc);
        return AnalyzeVoxelCells(voxelCells, model, options);
    }

    public static JsonObject BuildSimplifiedSummary(JsonArray voxelCells, JsonObject fullAnalysisReport)
    {
        if (voxelCells is null)
        {
            throw new ArgumentNullException(nameof(voxelCells));
        }

        if (fullAnalysisReport is null)
        {
            throw new ArgumentNullException(nameof(fullAnalysisReport));
        }

        var blobStats = new Dictionary<string, BlobSummaryAccumulator>(StringComparer.OrdinalIgnoreCase);
        var potentialErrors = new List<string>();
        var blobInstances = new JsonArray();
        int blobRowNumber = 0;
        int diffFlaggedBlobs = 0;

        for (int cellIndex = 0; cellIndex < voxelCells.Count; cellIndex++)
        {
            if (voxelCells[cellIndex] is not JsonObject cell)
            {
                continue;
            }

            if (TryGetPropertyIgnoreCase(cell, "records") is not JsonObject records)
            {
                continue;
            }

            foreach (KeyValuePair<string, JsonNode?> entry in records)
            {
                string recordName = string.IsNullOrWhiteSpace(entry.Key) ? "unknown" : entry.Key;
                bool isVoxelRecord = string.Equals(recordName, "voxel", StringComparison.OrdinalIgnoreCase);
                long? cellX = ReadLong(TryGetPropertyIgnoreCase(cell, "x"));
                long? cellY = ReadLong(TryGetPropertyIgnoreCase(cell, "y"));
                long? cellZ = ReadLong(TryGetPropertyIgnoreCase(cell, "z"));
                int? cellH = ReadInt(TryGetPropertyIgnoreCase(cell, "h"));

                var instance = new JsonObject
                {
                    ["row"] = ++blobRowNumber,
                    ["cell_index"] = cellIndex,
                    ["x"] = cellX is null ? null : JsonValue.Create(cellX.Value),
                    ["y"] = cellY is null ? null : JsonValue.Create(cellY.Value),
                    ["z"] = cellZ is null ? null : JsonValue.Create(cellZ.Value),
                    ["h"] = cellH is null ? null : JsonValue.Create(cellH.Value),
                    ["blob"] = recordName,
                    ["is_voxel_record"] = isVoxelRecord,
                    ["status"] = "unknown",
                    ["error"] = string.Empty
                };

                if (!blobStats.TryGetValue(recordName, out BlobSummaryAccumulator? summary))
                {
                    summary = new BlobSummaryAccumulator(recordName);
                    blobStats[recordName] = summary;
                }

                if (entry.Value is not JsonObject recordObject)
                {
                    string error = $"cell[{cellIndex}].records.{recordName}: record is not an object";
                    summary.AddParseFailure(error);
                    instance["status"] = "invalid_record";
                    instance["error"] = error;
                    blobInstances.Add(instance);
                    continue;
                }

                JsonNode? dataNode = TryGetPropertyIgnoreCase(recordObject, "data");
                if (!BlueprintVoxelMaterialDecoder.TryDecodeRecordBinaryData(
                        dataNode,
                        out byte[] raw,
                        out byte[] decoded,
                        out string decodeError))
                {
                    string error = $"cell[{cellIndex}].records.{recordName}: decode failed ({decodeError})";
                    summary.AddDecodeFailure(error);
                    instance["status"] = "decode_failed";
                    instance["error"] = error;
                    blobInstances.Add(instance);
                    continue;
                }

                summary.AddRawBlob(raw.LongLength);
                summary.AddDecodedBlob(decoded.LongLength);
                instance["raw_bytes"] = raw.LongLength;
                instance["decoded_bytes"] = decoded.LongLength;

                if (BlueprintVoxelMaterialDecoder.TryParseVoxelCellDataDetails(
                        decoded,
                        out VoxelCellDataDetails voxelDetails,
                        out string parseError))
                {
                    VoxelCellDataStatistics stats = voxelDetails.Statistics;
                    summary.AddVertexStats(stats);
                    instance["status"] = "ok";
                    instance["voxel_blocks"] = stats.VoxelBlockCount;
                    instance["vertex_samples"] = stats.VertexSampleCount;
                    instance["material_runs"] = stats.MaterialRunCount;
                    instance["vertex_runs"] = stats.VertexRunCount;
                    instance["is_diff"] = voxelDetails.IsDiff;
                    if (voxelDetails.IsDiff)
                    {
                        diffFlaggedBlobs++;
                        instance["status"] = "ok_diff";
                        instance["error"] = "is_diff=1";
                    }

                    JsonArray materials = BuildInstanceMaterialsArray(voxelDetails.Materials);
                    if (materials.Count > 0)
                    {
                        instance["materials"] = materials;
                    }

                    blobInstances.Add(instance);
                    continue;
                }

                if (isVoxelRecord)
                {
                    string error =
                        $"cell[{cellIndex}].records.{recordName}: not current VoxelCellData ({parseError})";
                    summary.AddParseFailure(error);
                    instance["status"] = "consistency_failed";
                    instance["error"] = error;
                }
                else if (string.Equals(recordName, "meta", StringComparison.OrdinalIgnoreCase))
                {
                    instance["status"] = "meta_ok";
                    instance["error"] = BuildMetaInfoText(decoded);
                }
                else
                {
                    instance["status"] = "decoded_non_voxel";
                    instance["error"] = string.IsNullOrWhiteSpace(parseError)
                        ? "Decoded non-voxel payload."
                        : $"Decoded non-voxel payload ({parseError}).";
                }

                blobInstances.Add(instance);
            }
        }

        int invalidCells = ReadNestedInt(fullAnalysisReport, "voxel_cells", "invalid_or_unparsed_count") ?? 0;
        int outsideConstruct = ReadNestedInt(fullAnalysisReport, "boundary_checks", "outside_construct_box_count") ?? 0;
        int malformedBlobCount = ReadNestedArrayCount(fullAnalysisReport, "blob_signatures", "malformed_blob_paths");

        if (invalidCells > 0)
        {
            potentialErrors.Add(
                $"{invalidCells.ToString(CultureInfo.InvariantCulture)} voxel cell(s) had invalid or incomplete coordinates.");
        }

        if (outsideConstruct > 0)
        {
            potentialErrors.Add(
                $"{outsideConstruct.ToString(CultureInfo.InvariantCulture)} cell envelope(s) extend outside the construct box.");
        }

        if (malformedBlobCount > 0)
        {
            potentialErrors.Add(
                $"{malformedBlobCount.ToString(CultureInfo.InvariantCulture)} blob payload(s) are malformed base64.");
        }

        int decodeFailures = blobStats.Values.Sum(static s => s.DecodeFailures);
        int parseFailures = blobStats.Values.Sum(static s => s.ParseFailures);
        if (decodeFailures > 0)
        {
            potentialErrors.Add(
                $"{decodeFailures.ToString(CultureInfo.InvariantCulture)} blob(s) failed decode/decompression.");
        }

        if (parseFailures > 0)
        {
            potentialErrors.Add(
                $"{parseFailures.ToString(CultureInfo.InvariantCulture)} blob(s) failed current VoxelCellData consistency checks.");
        }

        if (diffFlaggedBlobs > 0)
        {
            potentialErrors.Add(
                $"{diffFlaggedBlobs.ToString(CultureInfo.InvariantCulture)} voxel blob(s) have is_diff=1 (delta payload), which can cause edit/remove incompatibilities.");
        }

        JsonArray blobsArray = new();
        foreach (BlobSummaryAccumulator summary in blobStats.Values.OrderBy(static s => s.RecordName, StringComparer.OrdinalIgnoreCase))
        {
            blobsArray.Add(summary.ToJsonObject());
        }

        JsonArray errorArray = new();
        foreach (string error in potentialErrors.Take(25))
        {
            errorArray.Add(error);
        }

        return new JsonObject
        {
            ["overview"] = new JsonObject
            {
                ["cells_total"] = voxelCells.Count,
                ["cells_invalid_or_unparsed"] = invalidCells,
                ["blobs_total"] = blobStats.Values.Sum(static s => s.TotalBlobs),
                ["blobs_decode_failures"] = decodeFailures,
                ["blobs_consistency_failures"] = parseFailures,
                ["blobs_is_diff"] = diffFlaggedBlobs,
                ["outside_construct_box_cells"] = outsideConstruct,
                ["blob_instances_total"] = blobInstances.Count
            },
            ["blobs"] = blobsArray,
            ["blob_instances"] = blobInstances,
            ["potential_errors"] = errorArray
        };
    }

    public static JsonObject AnalyzeVoxelCells(
        JsonArray voxelCells,
        JsonObject? model = null,
        BlueprintVoxelAnalysisOptions? options = null)
    {
        if (voxelCells is null)
        {
            throw new ArgumentNullException(nameof(voxelCells));
        }

        BlueprintVoxelAnalysisOptions normalized = NormalizeOptions(options);
        JsonObject modelObject = model ?? new JsonObject();

        JsonObject jsonProperties = TryGetPropertyIgnoreCase(modelObject, "JsonProperties") as JsonObject ?? new JsonObject();
        JsonObject voxelGeometry = TryGetPropertyIgnoreCase(jsonProperties, "voxelGeometry") as JsonObject ?? new JsonObject();

        double sizeRaw = ReadDouble(TryGetPropertyIgnoreCase(voxelGeometry, "size")) ??
                         ReadDouble(TryGetPropertyIgnoreCase(modelObject, "Size")) ??
                         256d;

        bool useCenterOrigin = string.Equals(
            normalized.CoordinateOrigin,
            CoordOriginCenter,
            StringComparison.OrdinalIgnoreCase);

        double originOffsetVox;
        double constructBoxMinVox;
        double constructBoxMaxVox;
        double buildingBoxMinVox;
        double buildingBoxMaxVox;

        if (useCenterOrigin)
        {
            originOffsetVox = -sizeRaw;
            constructBoxMinVox = -sizeRaw;
            constructBoxMaxVox = sizeRaw;
            buildingBoxMinVox = -sizeRaw / 2d;
            buildingBoxMaxVox = sizeRaw / 2d;
        }
        else
        {
            originOffsetVox = 0d;
            constructBoxMinVox = 0d;
            constructBoxMaxVox = sizeRaw * 2d;
            buildingBoxMinVox = sizeRaw / 2d;
            buildingBoxMaxVox = sizeRaw * 1.5d;
        }

        double voxelsPerMeter = normalized.VoxelsPerMeter;
        double originOffsetM = originOffsetVox / voxelsPerMeter;
        double constructBoxMinM = constructBoxMinVox / voxelsPerMeter;
        double constructBoxMaxM = constructBoxMaxVox / voxelsPerMeter;
        double buildingBoxMinM = buildingBoxMinVox / voxelsPerMeter;
        double buildingBoxMaxM = buildingBoxMaxVox / voxelsPerMeter;

        int invalidCellCount = 0;
        int outsideConstructCount = 0;
        int? outsideBuildingCount = normalized.CheckBuildingZone ? 0 : null;

        var outsideConstructSamples = new JsonArray();
        JsonArray? outsideBuildingSamples = normalized.CheckBuildingZone ? new JsonArray() : null;

        var hCounts = new Dictionary<int, int>();
        long? xMin = null;
        long? yMin = null;
        long? zMin = null;
        long? xMax = null;
        long? yMax = null;
        long? zMax = null;

        for (int index = 0; index < voxelCells.Count; index++)
        {
            if (voxelCells[index] is not JsonObject cell)
            {
                invalidCellCount++;
                continue;
            }

            CellGeom? maybeGeom = BuildCellGeom(cell, originOffsetVox);
            if (!maybeGeom.HasValue)
            {
                invalidCellCount++;
                continue;
            }

            CellGeom geom = maybeGeom.Value;
            hCounts[geom.H] = hCounts.TryGetValue(geom.H, out int existing) ? existing + 1 : 1;

            xMin = xMin.HasValue ? Math.Min(xMin.Value, geom.X) : geom.X;
            yMin = yMin.HasValue ? Math.Min(yMin.Value, geom.Y) : geom.Y;
            zMin = zMin.HasValue ? Math.Min(zMin.Value, geom.Z) : geom.Z;
            xMax = xMax.HasValue ? Math.Max(xMax.Value, geom.X) : geom.X;
            yMax = yMax.HasValue ? Math.Max(yMax.Value, geom.Y) : geom.Y;
            zMax = zMax.HasValue ? Math.Max(zMax.Value, geom.Z) : geom.Z;

            Aabb aabbVox = geom.Aabb;
            Aabb aabbM = ScaleAabb(geom.Aabb, 1d / voxelsPerMeter);
            double spanVox = geom.Span;
            double spanM = geom.Span / voxelsPerMeter;

            JsonObject sample = BuildCellSample(index, geom, spanM, spanVox, aabbM, aabbVox);

            if (OutsideBox(aabbVox, constructBoxMinVox, constructBoxMaxVox))
            {
                outsideConstructCount++;
                if (outsideConstructSamples.Count < normalized.SampleLimit)
                {
                    outsideConstructSamples.Add(sample);
                }
            }

            if (normalized.CheckBuildingZone &&
                OutsideBox(aabbVox, buildingBoxMinVox, buildingBoxMaxVox))
            {
                outsideBuildingCount = (outsideBuildingCount ?? 0) + 1;
                if (outsideBuildingSamples is not null && outsideBuildingSamples.Count < normalized.SampleLimit)
                {
                    outsideBuildingSamples.Add(sample.DeepClone());
                }
            }
        }

        JsonObject blobSignatures = AnalyzeBlobShapes(voxelCells);
        JsonArray hHistogram = BuildTopPairs(
            hCounts,
            pair => JsonValue.Create(pair.Key),
            pair => JsonValue.Create(pair.Value),
            limit: 16);

        var report = new JsonObject
        {
            ["voxel_geometry"] = new JsonObject
            {
                ["coord_origin"] = useCenterOrigin ? CoordOriginCenter : CoordOriginZero,
                ["origin_offset_m"] = originOffsetM,
                ["origin_offset_vox"] = originOffsetVox,
                ["size_raw"] = sizeRaw,
                ["size_semantics"] = SizeSemantics,
                ["voxelLod0"] = ReadInt(TryGetPropertyIgnoreCase(voxelGeometry, "voxelLod0")),
                ["voxels_per_meter"] = voxelsPerMeter,
                ["construct_box"] = new JsonObject
                {
                    ["min"] = constructBoxMinM,
                    ["max"] = constructBoxMaxM
                },
                ["building_box"] = new JsonObject
                {
                    ["min"] = buildingBoxMinM,
                    ["max"] = buildingBoxMaxM
                },
                ["construct_box_vox"] = new JsonObject
                {
                    ["min"] = constructBoxMinVox,
                    ["max"] = constructBoxMaxVox
                },
                ["building_box_vox"] = new JsonObject
                {
                    ["min"] = buildingBoxMinVox,
                    ["max"] = buildingBoxMaxVox
                }
            },
            ["voxel_cells"] = new JsonObject
            {
                ["count"] = voxelCells.Count,
                ["invalid_or_unparsed_count"] = invalidCellCount,
                ["h_histogram_top"] = hHistogram,
                ["cell_index_ranges"] = new JsonObject
                {
                    ["x"] = BuildRangeArray(xMin, xMax),
                    ["y"] = BuildRangeArray(yMin, yMax),
                    ["z"] = BuildRangeArray(zMin, zMax)
                }
            },
            ["boundary_checks"] = new JsonObject
            {
                ["outside_construct_box_count"] = outsideConstructCount,
                ["outside_building_box_count"] = outsideBuildingCount is null
                    ? null
                    : JsonValue.Create(outsideBuildingCount.Value),
                ["outside_construct_box_samples"] = outsideConstructSamples,
                ["outside_building_box_samples"] = outsideBuildingSamples,
                ["note"] =
                    "Checks are performed in voxel scale using voxels_per_meter. " +
                    "Cell envelopes are used because records.voxel occupancy is not decoded, " +
                    "so partial-cell occupancy can still yield conservative false positives.",
                ["cell_axis_unit_assumption"] = CellAxisUnitAssumption,
                ["building_zone_check_enabled"] = normalized.CheckBuildingZone
            },
            ["blob_signatures"] = blobSignatures
        };

        return report;
    }

    private static BlueprintVoxelAnalysisOptions NormalizeOptions(BlueprintVoxelAnalysisOptions? options)
    {
        BlueprintVoxelAnalysisOptions source = options ?? new BlueprintVoxelAnalysisOptions();
        int sampleLimit = source.SampleLimit <= 0 ? 25 : source.SampleLimit;
        double voxelsPerMeter = source.VoxelsPerMeter <= 0d ? 4d : source.VoxelsPerMeter;

        string origin = string.Equals(source.CoordinateOrigin, CoordOriginZero, StringComparison.OrdinalIgnoreCase)
            ? CoordOriginZero
            : CoordOriginCenter;

        return new BlueprintVoxelAnalysisOptions(
            SampleLimit: sampleLimit,
            CoordinateOrigin: origin,
            VoxelsPerMeter: voxelsPerMeter,
            CheckBuildingZone: source.CheckBuildingZone);
    }

    private static JsonObject? ResolveModel(JsonObject doc)
    {
        if (TryGetPropertyIgnoreCase(doc, "Model") is JsonObject model)
        {
            return model;
        }

        if (TryGetPropertyIgnoreCase(doc, "Blueprint") is JsonObject blueprint &&
            TryGetPropertyIgnoreCase(blueprint, "Model") is JsonObject nestedModel)
        {
            return nestedModel;
        }

        return null;
    }

    private static JsonArray ResolveVoxelCells(JsonObject doc)
    {
        JsonNode? voxelDataNode = TryGetPropertyIgnoreCase(doc, "VoxelData");
        if (voxelDataNode is JsonArray topLevelArray)
        {
            return topLevelArray;
        }

        if (voxelDataNode is JsonObject voxelDataObject &&
            TryGetPropertyIgnoreCase(voxelDataObject, "cells") is JsonArray nestedCells)
        {
            return nestedCells;
        }

        if (TryGetPropertyIgnoreCase(doc, "cells") is JsonArray rootCells)
        {
            return rootCells;
        }

        return new JsonArray();
    }

    private static CellGeom? BuildCellGeom(JsonObject cell, double originOffsetVox)
    {
        long? x = ReadLong(TryGetPropertyIgnoreCase(cell, "x"));
        long? y = ReadLong(TryGetPropertyIgnoreCase(cell, "y"));
        long? z = ReadLong(TryGetPropertyIgnoreCase(cell, "z"));
        int? h = ReadInt(TryGetPropertyIgnoreCase(cell, "h"));
        if (!x.HasValue || !y.HasValue || !z.HasValue || !h.HasValue)
        {
            return null;
        }

        if (h.Value < 0 || h.Value > 62)
        {
            return null;
        }

        double span = (double)(1UL << h.Value);
        Vec3 min = new(
            x.Value * span + originOffsetVox,
            y.Value * span + originOffsetVox,
            z.Value * span + originOffsetVox);
        Vec3 max = new(
            (x.Value + 1d) * span + originOffsetVox,
            (y.Value + 1d) * span + originOffsetVox,
            (z.Value + 1d) * span + originOffsetVox);

        return new CellGeom(x.Value, y.Value, z.Value, h.Value, span, new Aabb(min, max));
    }

    private static JsonObject BuildCellSample(
        int index,
        CellGeom geom,
        double spanM,
        double spanVox,
        Aabb aabbM,
        Aabb aabbVox)
    {
        return new JsonObject
        {
            ["index"] = index,
            ["x"] = geom.X,
            ["y"] = geom.Y,
            ["z"] = geom.Z,
            ["h"] = geom.H,
            ["span_m"] = spanM,
            ["span_vox"] = spanVox,
            ["aabb_m"] = SerializeAabb(aabbM),
            ["aabb_vox"] = SerializeAabb(aabbVox)
        };
    }

    private static JsonObject SerializeAabb(Aabb aabb)
    {
        return new JsonObject
        {
            ["min"] = new JsonObject
            {
                ["x"] = aabb.Min.X,
                ["y"] = aabb.Min.Y,
                ["z"] = aabb.Min.Z
            },
            ["max"] = new JsonObject
            {
                ["x"] = aabb.Max.X,
                ["y"] = aabb.Max.Y,
                ["z"] = aabb.Max.Z
            }
        };
    }

    private static bool OutsideBox(Aabb aabb, double boxMin, double boxMax, double eps = 1e-9d)
    {
        if (aabb.Min.X < boxMin - eps || aabb.Min.Y < boxMin - eps || aabb.Min.Z < boxMin - eps)
        {
            return true;
        }

        if (aabb.Max.X > boxMax + eps || aabb.Max.Y > boxMax + eps || aabb.Max.Z > boxMax + eps)
        {
            return true;
        }

        return false;
    }

    private static Aabb ScaleAabb(Aabb aabb, double factor)
    {
        return new Aabb(
            new Vec3(
                aabb.Min.X * factor,
                aabb.Min.Y * factor,
                aabb.Min.Z * factor),
            new Vec3(
                aabb.Max.X * factor,
                aabb.Max.Y * factor,
                aabb.Max.Z * factor));
    }

    private static JsonNode? BuildRangeArray(long? min, long? max)
    {
        if (!min.HasValue || !max.HasValue)
        {
            return null;
        }

        return new JsonArray(min.Value, max.Value);
    }

    private static JsonObject AnalyzeBlobShapes(JsonArray voxelCells)
    {
        var perRecordSizes = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        var perRecordMagic = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);
        var perRecordU64At4 = new Dictionary<string, Dictionary<ulong, int>>(StringComparer.Ordinal);
        var perRecordU64At12 = new Dictionary<string, Dictionary<ulong, int>>(StringComparer.Ordinal);
        var perRecordAsciiTokens = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);
        var malformed = new List<string>();

        for (int cellIndex = 0; cellIndex < voxelCells.Count; cellIndex++)
        {
            if (voxelCells[cellIndex] is not JsonObject cell)
            {
                continue;
            }

            if (TryGetPropertyIgnoreCase(cell, "records") is not JsonObject records)
            {
                continue;
            }

            foreach (KeyValuePair<string, JsonNode?> recordEntry in records)
            {
                string recordName = recordEntry.Key;
                if (recordEntry.Value is not JsonObject recordObject)
                {
                    continue;
                }

                JsonNode? dataNode = TryGetPropertyIgnoreCase(recordObject, "data");
                if (!TryReadBinaryPayload(dataNode, out string base64Text))
                {
                    continue;
                }

                if (!TryDecodeBase64(base64Text, out byte[] raw))
                {
                    if (malformed.Count < MaxMalformedBlobPaths)
                    {
                        malformed.Add($"cell[{cellIndex.ToString(CultureInfo.InvariantCulture)}].records.{recordName}");
                    }
                    continue;
                }

                AddListValue(perRecordSizes, recordName, raw.Length);

                if (raw.Length >= 4)
                {
                    string magic = Convert.ToHexString(raw.AsSpan(0, 4)).ToLowerInvariant();
                    IncrementNested(perRecordMagic, recordName, magic);
                }

                if (raw.Length >= 12)
                {
                    ulong u64At4 = BinaryPrimitives.ReadUInt64LittleEndian(raw.AsSpan(4, 8));
                    IncrementNested(perRecordU64At4, recordName, u64At4);
                }

                if (raw.Length >= 20)
                {
                    ulong u64At12 = BinaryPrimitives.ReadUInt64LittleEndian(raw.AsSpan(12, 8));
                    IncrementNested(perRecordU64At12, recordName, u64At12);
                }

                foreach (string token in ExtractAsciiStrings(raw, minLength: 4))
                {
                    IncrementNested(perRecordAsciiTokens, recordName, token);
                }
            }
        }

        var recordsSummary = new JsonObject();
        foreach (string recordName in perRecordSizes.Keys.OrderBy(static name => name, StringComparer.Ordinal))
        {
            recordsSummary[recordName] = new JsonObject
            {
                ["size_bytes"] = SummarizeSizes(perRecordSizes[recordName]),
                ["magic_prefix_top"] = BuildTopPairs(
                    perRecordMagic.GetValueOrDefault(recordName) ?? new Dictionary<string, int>(),
                    pair => JsonValue.Create(pair.Key),
                    pair => JsonValue.Create(pair.Value),
                    limit: 5),
                ["u64_at_4_top"] = BuildTopPairs(
                    perRecordU64At4.GetValueOrDefault(recordName) ?? new Dictionary<ulong, int>(),
                    pair => JsonValue.Create(pair.Key.ToString(CultureInfo.InvariantCulture)),
                    pair => JsonValue.Create(pair.Value),
                    limit: 8),
                ["u64_at_12_top"] = BuildTopPairs(
                    perRecordU64At12.GetValueOrDefault(recordName) ?? new Dictionary<ulong, int>(),
                    pair => JsonValue.Create(pair.Key.ToString(CultureInfo.InvariantCulture)),
                    pair => JsonValue.Create(pair.Value),
                    limit: 8),
                ["ascii_tokens_top"] = BuildTopPairs(
                    perRecordAsciiTokens.GetValueOrDefault(recordName) ?? new Dictionary<string, int>(),
                    pair => JsonValue.Create(pair.Key),
                    pair => JsonValue.Create(pair.Value),
                    limit: 20)
            };
        }

        var malformedArray = new JsonArray();
        foreach (string path in malformed.Take(MaxMalformedBlobPaths))
        {
            malformedArray.Add(path);
        }

        return new JsonObject
        {
            ["records"] = recordsSummary,
            ["malformed_blob_paths"] = malformedArray
        };
    }

    private static JsonObject SummarizeSizes(List<int> sizes)
    {
        if (sizes.Count == 0)
        {
            return new JsonObject { ["count"] = 0 };
        }

        List<int> sorted = sizes.OrderBy(static value => value).ToList();
        int count = sorted.Count;
        int p95Index = Math.Max(0, (int)(count * 0.95d) - 1);

        double mean = Math.Round(sorted.Average(), 3);
        double median = count % 2 == 1
            ? sorted[count / 2]
            : (sorted[(count / 2) - 1] + sorted[count / 2]) / 2d;

        return new JsonObject
        {
            ["count"] = count,
            ["min"] = sorted[0],
            ["max"] = sorted[^1],
            ["mean"] = mean,
            ["median"] = median,
            ["p95"] = sorted[p95Index]
        };
    }

    private static string BuildMetaInfoText(byte[] decoded)
    {
        if (decoded is null || decoded.Length < 4)
        {
            return "Cell metadata block.";
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(decoded.AsSpan(0, 4));
        if (magic == MagicVoxelMetaData)
        {
            return "Cell metadata block (material-volume map).";
        }

        return "Cell metadata block.";
    }

    private static JsonArray BuildInstanceMaterialsArray(IReadOnlyList<VoxelCellMaterialEntry> materials)
    {
        var result = new JsonArray();
        if (materials is null || materials.Count == 0)
        {
            return result;
        }

        foreach (VoxelCellMaterialEntry material in materials)
        {
            result.Add(new JsonObject
            {
                ["material_id"] = material.MaterialId,
                ["material_token"] = material.MaterialToken,
                ["material_name"] = material.MaterialToken,
                ["voxel_blocks"] = material.VoxelBlocks
            });
        }

        return result;
    }

    private static JsonArray BuildTopPairs<TKey>(
        IDictionary<TKey, int> counts,
        Func<KeyValuePair<TKey, int>, JsonNode?> keyFactory,
        Func<KeyValuePair<TKey, int>, JsonNode?> valueFactory,
        int limit)
        where TKey : notnull
    {
        var result = new JsonArray();
        foreach (KeyValuePair<TKey, int> entry in counts
                     .OrderByDescending(static pair => pair.Value)
                     .ThenBy(pair => pair.Key?.ToString(), StringComparer.Ordinal)
                     .Take(limit))
        {
            result.Add(new JsonArray(keyFactory(entry), valueFactory(entry)));
        }

        return result;
    }

    private static void AddListValue<TKey>(IDictionary<TKey, List<int>> target, TKey key, int value)
        where TKey : notnull
    {
        if (!target.TryGetValue(key, out List<int>? values))
        {
            values = new List<int>();
            target[key] = values;
        }

        values.Add(value);
    }

    private static void IncrementNested<TKeyOuter, TKeyInner>(
        IDictionary<TKeyOuter, Dictionary<TKeyInner, int>> target,
        TKeyOuter outerKey,
        TKeyInner innerKey)
        where TKeyOuter : notnull
        where TKeyInner : notnull
    {
        if (!target.TryGetValue(outerKey, out Dictionary<TKeyInner, int>? nested))
        {
            nested = new Dictionary<TKeyInner, int>();
            target[outerKey] = nested;
        }

        nested[innerKey] = nested.TryGetValue(innerKey, out int count) ? count + 1 : 1;
    }

    private static bool TryReadBinaryPayload(JsonNode? dataNode, out string base64Text)
    {
        base64Text = string.Empty;

        if (dataNode is null)
        {
            return false;
        }

        if (dataNode is JsonValue scalar &&
            scalar.TryGetValue<string>(out string? scalarValue) &&
            !string.IsNullOrWhiteSpace(scalarValue))
        {
            base64Text = scalarValue.Trim();
            return true;
        }

        if (dataNode is not JsonObject dataObject)
        {
            return false;
        }

        JsonNode? binaryNode = TryGetPropertyIgnoreCase(dataObject, "$binary");
        if (binaryNode is not null && TryReadBinaryPayload(binaryNode, out base64Text))
        {
            return true;
        }

        JsonNode? base64Node = TryGetPropertyIgnoreCase(dataObject, "base64");
        if (base64Node is JsonValue base64Scalar &&
            base64Scalar.TryGetValue<string>(out string? base64Value) &&
            !string.IsNullOrWhiteSpace(base64Value))
        {
            base64Text = base64Value.Trim();
            return true;
        }

        return false;
    }

    private static int? ReadNestedInt(JsonObject root, string objectProperty, string valueProperty)
    {
        if (TryGetPropertyIgnoreCase(root, objectProperty) is not JsonObject obj)
        {
            return null;
        }

        return ReadInt(TryGetPropertyIgnoreCase(obj, valueProperty));
    }

    private static int ReadNestedArrayCount(JsonObject root, string objectProperty, string valueProperty)
    {
        if (TryGetPropertyIgnoreCase(root, objectProperty) is not JsonObject obj)
        {
            return 0;
        }

        return TryGetPropertyIgnoreCase(obj, valueProperty) is JsonArray arr
            ? arr.Count
            : 0;
    }

    private static bool TryDecodeBase64(string text, out byte[] raw)
    {
        raw = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        try
        {
            raw = Convert.FromBase64String(text.Trim());
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static IEnumerable<string> ExtractAsciiStrings(byte[] raw, int minLength)
    {
        var tokens = new List<string>();
        var buffer = new List<byte>();

        foreach (byte b in raw)
        {
            if (b >= 32 && b <= 126)
            {
                buffer.Add(b);
                continue;
            }

            if (buffer.Count >= minLength)
            {
                tokens.Add(Encoding.ASCII.GetString(buffer.ToArray()));
            }

            buffer.Clear();
        }

        if (buffer.Count >= minLength)
        {
            tokens.Add(Encoding.ASCII.GetString(buffer.ToArray()));
        }

        return tokens;
    }

    private static JsonNode? TryGetPropertyIgnoreCase(JsonObject obj, string propertyName)
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

    private static int? ReadInt(JsonNode? node)
    {
        long? value = ReadLong(node);
        if (!value.HasValue || value.Value < int.MinValue || value.Value > int.MaxValue)
        {
            return null;
        }

        return (int)value.Value;
    }

    private static long? ReadLong(JsonNode? node)
    {
        double? number = ReadDouble(node);
        if (!number.HasValue || double.IsNaN(number.Value) || double.IsInfinity(number.Value))
        {
            return null;
        }

        return (long)number.Value;
    }

    private static double? ReadDouble(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<double>(out double asDouble))
            {
                return asDouble;
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
                !string.IsNullOrWhiteSpace(asString))
            {
                if (double.TryParse(
                        asString,
                        NumberStyles.Float | NumberStyles.AllowThousands,
                        CultureInfo.InvariantCulture,
                        out double parsed))
                {
                    return parsed;
                }
            }
        }

        if (node is JsonObject wrappedObject)
        {
            string[] wrappedKeys = {"$numberLong", "$numberInt", "$numberDouble", "$numberDecimal"};
            foreach (string wrappedKey in wrappedKeys)
            {
                JsonNode? nested = TryGetPropertyIgnoreCase(wrappedObject, wrappedKey);
                if (nested is null)
                {
                    continue;
                }

                double? parsed = ReadDouble(nested);
                if (parsed.HasValue)
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    private sealed class BlobSummaryAccumulator
    {
        private readonly List<string> _sampleErrors = new();

        public BlobSummaryAccumulator(string recordName)
        {
            RecordName = recordName;
        }

        public string RecordName { get; }
        public int TotalBlobs { get; private set; }
        public int DecodedBlobs { get; private set; }
        public int DecodeFailures { get; private set; }
        public int ParseFailures { get; private set; }
        public int ParsedVertexBlobs { get; private set; }
        public long TotalRawBytes { get; private set; }
        public long TotalDecodedBytes { get; private set; }
        public long TotalVoxelBlocks { get; private set; }
        public long TotalVertexSamples { get; private set; }
        public long TotalMaterialRuns { get; private set; }
        public long TotalVertexRuns { get; private set; }

        public void AddRawBlob(long rawBytes)
        {
            TotalBlobs++;
            TotalRawBytes += Math.Max(0L, rawBytes);
        }

        public void AddDecodedBlob(long decodedBytes)
        {
            DecodedBlobs++;
            TotalDecodedBytes += Math.Max(0L, decodedBytes);
        }

        public void AddDecodeFailure(string error)
        {
            DecodeFailures++;
            AddSampleError(error);
        }

        public void AddParseFailure(string error)
        {
            ParseFailures++;
            AddSampleError(error);
        }

        public void AddVertexStats(VoxelCellDataStatistics stats)
        {
            ParsedVertexBlobs++;
            TotalVoxelBlocks += Math.Max(0L, stats.VoxelBlockCount);
            TotalVertexSamples += Math.Max(0L, stats.VertexSampleCount);
            TotalMaterialRuns += Math.Max(0, stats.MaterialRunCount);
            TotalVertexRuns += Math.Max(0, stats.VertexRunCount);
        }

        public JsonObject ToJsonObject()
        {
            double? avgVertices = ParsedVertexBlobs > 0
                ? Math.Round(TotalVertexSamples / (double)ParsedVertexBlobs, 2)
                : null;

            var sampleErrors = new JsonArray();
            foreach (string error in _sampleErrors.Take(5))
            {
                sampleErrors.Add(error);
            }

            return new JsonObject
            {
                ["blob"] = RecordName,
                ["count"] = TotalBlobs,
                ["decoded_ok"] = DecodedBlobs,
                ["decode_failures"] = DecodeFailures,
                ["consistency_failures"] = ParseFailures,
                ["vertex_blobs_parsed"] = ParsedVertexBlobs,
                ["total_voxel_blocks"] = TotalVoxelBlocks,
                ["total_vertex_samples"] = TotalVertexSamples,
                ["total_material_runs"] = TotalMaterialRuns,
                ["total_vertex_runs"] = TotalVertexRuns,
                ["avg_vertex_samples"] = avgVertices is null
                    ? null
                    : JsonValue.Create(avgVertices.Value),
                ["raw_bytes_total"] = TotalRawBytes,
                ["decoded_bytes_total"] = TotalDecodedBytes,
                ["sample_errors"] = sampleErrors
            };
        }

        private void AddSampleError(string error)
        {
            if (string.IsNullOrWhiteSpace(error))
            {
                return;
            }

            if (_sampleErrors.Count < 10)
            {
                _sampleErrors.Add(error.Trim());
            }
        }
    }

}
