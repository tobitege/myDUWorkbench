using Avalonia;
using Avalonia.Controls;
using Avalonia.Input.Platform;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace myDUWorkbench.Views;

public partial class VoxelAnalysisDialog : Window
{
    private readonly ObservableCollection<VoxelBlobInstanceRow> _rows = new();
    private readonly string _fullJson;

    public VoxelAnalysisDialog()
    {
        InitializeComponent();
        _fullJson = string.Empty;
        BlobInstancesGrid.ItemsSource = _rows;
    }

    public VoxelAnalysisDialog(string fullJson)
        : this()
    {
        _fullJson = fullJson ?? string.Empty;
        LoadFromJson(_fullJson);
    }

    private void LoadFromJson(string json)
    {
        _rows.Clear();
        OverviewTextBlock.Text = "No analysis data.";
        PotentialErrorsTextBox.Text = string.Empty;

        if (string.IsNullOrWhiteSpace(json))
        {
            return;
        }

        try
        {
            JsonNode? parsed = JsonNode.Parse(json);
            if (parsed is not JsonObject root)
            {
                OverviewTextBlock.Text = "Analysis JSON root is not an object.";
                return;
            }

            JsonObject summary = ResolveSummary(root);
            PopulateOverview(summary, root);
            PopulateErrors(summary);
            PopulateRows(summary);
        }
        catch (Exception ex)
        {
            OverviewTextBlock.Text = $"Failed to parse analysis JSON: {ex.Message}";
        }
    }

    private static JsonObject ResolveSummary(JsonObject root)
    {
        if (root["summary"] is JsonObject summary)
        {
            return summary;
        }

        return root;
    }

    private void PopulateOverview(JsonObject summary, JsonObject root)
    {
        JsonObject overview = summary["overview"] as JsonObject ?? new JsonObject();
        string scope = ReadString(root["scope"]) ?? string.Empty;
        string targetId = ReadString(root["targetId"]) ?? "-";
        string sourceName = ReadString(root["sourceName"]) ?? string.Empty;
        int cellsTotal = ReadInt(overview["cells_total"]) ?? 0;
        int blobTotal = ReadInt(overview["blobs_total"]) ?? 0;
        int verticesFailures = ReadInt(overview["blobs_consistency_failures"]) ?? 0;
        int decodeFailures = ReadInt(overview["blobs_decode_failures"]) ?? 0;
        int instanceTotal = ReadInt(overview["blob_instances_total"]) ?? 0;

        string inputLabel = BuildInputLabel(scope, sourceName);
        string constructInfo = string.Equals(scope, "construct", StringComparison.OrdinalIgnoreCase) &&
                               !string.IsNullOrWhiteSpace(targetId) &&
                               targetId != "-"
            ? $" | Construct ID: {targetId}"
            : string.Empty;

        OverviewTextBlock.Text =
            $"Input: {inputLabel}{constructInfo} | Cells: {cellsTotal.ToString(CultureInfo.InvariantCulture)} | " +
            $"Blobs: {blobTotal.ToString(CultureInfo.InvariantCulture)} | " +
            $"Rows: {instanceTotal.ToString(CultureInfo.InvariantCulture)} | " +
            $"Decode issues: {decodeFailures.ToString(CultureInfo.InvariantCulture)} | " +
            $"Consistency issues: {verticesFailures.ToString(CultureInfo.InvariantCulture)}";

        if (summary["material_totals"] is JsonObject materialTotals)
        {
            double selectedLiters = ReadDouble(materialTotals["selected_total_volume_liters"]) ??
                                    ReadDouble(materialTotals["decoded_total_volume_liters"]) ??
                                    0d;
            string source = ReadString(materialTotals["volume_source"]) ?? "decoded_estimate";
            string methodLabel = BuildVolumeSourceLabel(source);

            double selectedM3 = selectedLiters / 1000d;

            string materialSummary =
                $"Honeycomb (calculated): {selectedM3.ToString("N2", CultureInfo.InvariantCulture)} m³";

            OverviewTextBlock.Text += Environment.NewLine + materialSummary +
                                      $" | Method: {methodLabel}";
        }

        if (HasBlobType(summary, "meta"))
        {
            OverviewTextBlock.Text += Environment.NewLine +
                                      "Note: meta rows hold cell material-volume metadata; voxel rows hold geometry/material run data.";
        }
    }

    private void PopulateErrors(JsonObject summary)
    {
        if (summary["potential_errors"] is not JsonArray errors || errors.Count == 0)
        {
            PotentialErrorsTextBox.Text = "None detected.";
            return;
        }

        var lines = new List<string>();
        foreach (JsonNode? error in errors)
        {
            string text = ReadString(error) ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(text))
            {
                lines.Add("• " + text.Trim());
            }
        }

        PotentialErrorsTextBox.Text = lines.Count == 0
            ? "None detected."
            : string.Join(Environment.NewLine, lines);
    }

    private void PopulateRows(JsonObject summary)
    {
        if (summary["blob_instances"] is not JsonArray instances)
        {
            return;
        }

        foreach (JsonNode? rowNode in instances)
        {
            if (rowNode is not JsonObject row)
            {
                continue;
            }

            int rowNumber = ReadInt(row["row"]) ?? (_rows.Count + 1);
            int cellIndex = ReadInt(row["cell_index"]) ?? -1;
            long? x = ReadLong(row["x"]);
            long? y = ReadLong(row["y"]);
            long? z = ReadLong(row["z"]);
            int? h = ReadInt(row["h"]);
            string coords = x.HasValue && y.HasValue && z.HasValue
                ? $"({x.Value},{y.Value},{z.Value})"
                : "(n/a)";
            string blob = ReadString(row["blob"]) ?? "unknown";
            string status = NormalizeStatus(blob, ReadString(row["status"]) ?? "unknown");
            long? vertexSamples = ReadLong(row["vertex_samples"]);
            long? voxelBlocks = ReadLong(row["voxel_blocks"]);
            int? materialRuns = ReadInt(row["material_runs"]);
            int? vertexRuns = ReadInt(row["vertex_runs"]);
            long? rawBytes = ReadLong(row["raw_bytes"]);
            long? decodedBytes = ReadLong(row["decoded_bytes"]);
            string error = ReadString(row["error"]) ?? string.Empty;
            List<MaterialRow> materials = ParseMaterials(row["materials"]);
            if (materials.Count == 0)
            {
                _rows.Add(new VoxelBlobInstanceRow(
                    rowNumber.ToString(CultureInfo.InvariantCulture),
                    cellIndex.ToString(CultureInfo.InvariantCulture),
                    coords,
                    h?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    blob,
                    status,
                    vertexSamples?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    voxelBlocks?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    materialRuns?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    vertexRuns?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    rawBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    decodedBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                    string.Empty,
                    string.Empty,
                    error));
                continue;
            }

            for (int i = 0; i < materials.Count; i++)
            {
                MaterialRow material = materials[i];
                bool isFirst = i == 0;

                _rows.Add(new VoxelBlobInstanceRow(
                    isFirst ? rowNumber.ToString(CultureInfo.InvariantCulture) : string.Empty,
                    isFirst ? cellIndex.ToString(CultureInfo.InvariantCulture) : string.Empty,
                    isFirst ? coords : string.Empty,
                    isFirst ? (h?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? blob : string.Empty,
                    isFirst ? status : string.Empty,
                    isFirst ? (vertexSamples?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? (voxelBlocks?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? (materialRuns?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? (vertexRuns?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? (rawBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    isFirst ? (decodedBytes?.ToString(CultureInfo.InvariantCulture) ?? string.Empty) : string.Empty,
                    BuildMaterialTokenText(material),
                    BuildMaterialDisplayText(material),
                    isFirst ? error : string.Empty));
            }
        }
    }

    private async void OnSaveToClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        try
        {
            TopLevel? topLevel = TopLevel.GetTopLevel(this);
            IStorageProvider? storageProvider = topLevel?.StorageProvider;
            if (storageProvider is null || !storageProvider.CanSave)
            {
                ActionResultTextBlock.Text = "Save dialog unavailable.";
                return;
            }

            var options = new FilePickerSaveOptions
            {
                Title = "Save Voxel Analysis JSON",
                SuggestedFileName = BuildDefaultExportFileName(),
                DefaultExtension = ".json",
                FileTypeChoices = new[]
                {
                    new FilePickerFileType("JSON files")
                    {
                        Patterns = new[] { "*.json" }
                    },
                    new FilePickerFileType("All files")
                    {
                        Patterns = new[] { "*.*" }
                    }
                }
            };

            IStorageFile? file = await storageProvider.SaveFilePickerAsync(options);
            if (file?.Path is not Uri filePathUri || !filePathUri.IsFile)
            {
                return;
            }

            string outputPath = filePathUri.LocalPath;
            await File.WriteAllTextAsync(outputPath, _fullJson, new UTF8Encoding(false));
            ActionResultTextBlock.Text = $"Saved: {Path.GetFileName(outputPath)}";
        }
        catch (Exception ex)
        {
            ActionResultTextBlock.Text = $"Save failed: {ex.Message}";
        }
    }

    private async void OnCopyClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        try
        {
            TopLevel? topLevel = TopLevel.GetTopLevel(this);
            IClipboard? clipboard = topLevel?.Clipboard;
            if (clipboard is null)
            {
                ActionResultTextBlock.Text = "Clipboard unavailable.";
                return;
            }

            await ClipboardExtensions.SetTextAsync(clipboard, _fullJson);
            ActionResultTextBlock.Text = "Copied JSON.";
        }
        catch (Exception ex)
        {
            ActionResultTextBlock.Text = $"Copy failed: {ex.Message}";
        }
    }

    private void OnCloseClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close();
    }

    private string BuildDefaultExportFileName()
    {
        string baseName = string.IsNullOrWhiteSpace(Title) ? "voxel_analysis" : Title.Trim();
        baseName = Regex.Replace(baseName, @"\s+", "_");
        baseName = Regex.Replace(baseName, @"[^A-Za-z0-9._-]", string.Empty);
        if (string.IsNullOrWhiteSpace(baseName))
        {
            baseName = "voxel_analysis";
        }

        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        return $"{baseName}_{timestamp}.json";
    }

    private static string? ReadString(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out string? value))
            {
                return value;
            }

            if (scalar.TryGetValue<long>(out long longValue))
            {
                return longValue.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<int>(out int intValue))
            {
                return intValue.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<double>(out double doubleValue))
            {
                return doubleValue.ToString("R", CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<bool>(out bool boolValue))
            {
                return boolValue ? "true" : "false";
            }
        }

        return node.ToJsonString();
    }

    private static int? ReadInt(JsonNode? node)
    {
        long? value = ReadLong(node);
        if (!value.HasValue || value < int.MinValue || value > int.MaxValue)
        {
            return null;
        }

        return (int)value.Value;
    }

    private static long? ReadLong(JsonNode? node)
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
                return (long)asDouble;
            }

            if (scalar.TryGetValue<string>(out string? text) &&
                long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
            {
                return parsed;
            }
        }

        return null;
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

            if (scalar.TryGetValue<float>(out float asFloat))
            {
                return asFloat;
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
                return parsed;
            }
        }

        return null;
    }

    private static string BuildInputLabel(string scope, string sourceName)
    {
        if (string.Equals(scope, "blueprint_json_file", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(sourceName))
            {
                return "Blueprint file";
            }

            try
            {
                string fileName = Path.GetFileName(sourceName.Trim());
                return string.IsNullOrWhiteSpace(fileName) ? "Blueprint file" : fileName;
            }
            catch
            {
                return "Blueprint file";
            }
        }

        if (string.Equals(scope, "construct", StringComparison.OrdinalIgnoreCase))
        {
            return "Construct";
        }

        if (string.Equals(scope, "blueprint", StringComparison.OrdinalIgnoreCase))
        {
            return "Blueprint";
        }

        return "Voxel data";
    }

    private static string BuildVolumeSourceLabel(string source)
    {
        return source switch
        {
            "meta_blob_offline" => "Blueprint meta data (offline)",
            "voxel_metadata_endpoint" => "Server metadata endpoint",
            "decoded_estimate" => "Voxel estimate (fallback)",
            _ => source
        };
    }

    private static bool HasBlobType(JsonObject summary, string blobName)
    {
        if (summary["blob_instances"] is not JsonArray rows)
        {
            return false;
        }

        foreach (JsonNode? node in rows)
        {
            if (node is not JsonObject row)
            {
                continue;
            }

            string? blob = ReadString(row["blob"]);
            if (string.Equals(blob, blobName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeStatus(string blob, string status)
    {
        if (string.Equals(blob, "meta", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(status, "meta_ok", StringComparison.OrdinalIgnoreCase))
        {
            return "meta";
        }

        return status switch
        {
            "ok_diff" => "ok (delta)",
            "decoded_non_voxel" => "non-voxel",
            "consistency_failed" => "consistency issue",
            "decode_failed" => "decode failed",
            _ => status
        };
    }

    private static string BuildMaterialTokenText(MaterialRow material)
    {
        string token = string.IsNullOrWhiteSpace(material.MaterialToken) ? "Unknown" : material.MaterialToken;
        string id = string.IsNullOrWhiteSpace(material.MaterialId) ? "?" : material.MaterialId;
        string amount = material.VoxelBlocks > 0
            ? $" x{material.VoxelBlocks.ToString(CultureInfo.InvariantCulture)}"
            : string.Empty;
        return $"{token}[{id}]{amount}";
    }

    private static string BuildMaterialDisplayText(MaterialRow material)
    {
        string name = string.IsNullOrWhiteSpace(material.MaterialName)
            ? (string.IsNullOrWhiteSpace(material.MaterialToken) ? "Unknown" : material.MaterialToken)
            : material.MaterialName;
        string id = string.IsNullOrWhiteSpace(material.MaterialId) ? "?" : material.MaterialId;
        string amount = material.VoxelBlocks > 0
            ? $" x{material.VoxelBlocks.ToString(CultureInfo.InvariantCulture)}"
            : string.Empty;
        return $"{name}[{id}]{amount}";
    }

    private static List<MaterialRow> ParseMaterials(JsonNode? node)
    {
        var rows = new List<MaterialRow>();
        if (node is not JsonArray materialsArray)
        {
            return rows;
        }

        foreach (JsonNode? materialNode in materialsArray)
        {
            if (materialNode is not JsonObject materialObject)
            {
                continue;
            }

            string materialId = ReadString(materialObject["material_id"]) ?? string.Empty;
            string materialToken = ReadString(materialObject["material_token"]) ?? string.Empty;
            string materialName = ReadString(materialObject["material_name"]) ?? string.Empty;
            long voxelBlocks = ReadLong(materialObject["voxel_blocks"]) ?? 0L;
            rows.Add(new MaterialRow(materialId, materialToken, materialName, voxelBlocks));
        }

        rows.Sort(static (a, b) =>
        {
            int byCount = b.VoxelBlocks.CompareTo(a.VoxelBlocks);
            if (byCount != 0)
            {
                return byCount;
            }

            int byName = string.Compare(a.MaterialName, b.MaterialName, StringComparison.OrdinalIgnoreCase);
            if (byName != 0)
            {
                return byName;
            }

            return string.Compare(a.MaterialId, b.MaterialId, StringComparison.OrdinalIgnoreCase);
        });

        return rows;
    }

    private sealed record VoxelBlobInstanceRow(
        string RowNumber,
        string CellIndex,
        string Coords,
        string H,
        string Blob,
        string Status,
        string VertexSamples,
        string VoxelBlocks,
        string MaterialRuns,
        string VertexRuns,
        string RawBytes,
        string DecodedBytes,
        string MaterialTokens,
        string MaterialNames,
        string Error);

    private sealed record MaterialRow(
        string MaterialId,
        string MaterialToken,
        string MaterialName,
        long VoxelBlocks);
}
