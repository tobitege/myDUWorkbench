using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private sealed record VoxelIngredientGridRow(
        string MaterialId,
        string MaterialName,
        long VoxelBlocks,
        double VolumeLiters);

    private readonly List<VoxelIngredientGridRow> _voxelIngredientRows = new();
    private string _voxelIngredientScopeLabel = string.Empty;

    public async Task<string> ExportSelectedBlueprintVoxelMaterialSummaryJsonAsync(CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            throw new InvalidOperationException("No blueprint selected.");
        }

        DataConnectionOptions options = BuildDbOptions();
        return await _dataService.ExportBlueprintVoxelMaterialSummaryJsonAsync(
            bp.Id,
            EndpointTemplateInput,
            BlueprintImportEndpointInput,
            options,
            cancellationToken);
    }

    public async Task<string> ExportLoadedConstructVoxelMaterialSummaryJsonAsync(CancellationToken cancellationToken)
    {
        if (_lastSnapshot is not { } snapshot || snapshot.ConstructId == 0UL)
        {
            throw new InvalidOperationException("No loaded construct snapshot available.");
        }

        DataConnectionOptions options = BuildDbOptions();
        return await _dataService.ExportConstructVoxelMaterialSummaryJsonAsync(
            snapshot.ConstructId,
            EndpointTemplateInput,
            BlueprintImportEndpointInput,
            options,
            cancellationToken);
    }

    public void ApplyVoxelMaterialSummaryToGrid(string summaryJson)
    {
        if (string.IsNullOrWhiteSpace(summaryJson))
        {
            _voxelIngredientRows.Clear();
            _voxelIngredientScopeLabel = string.Empty;
            ApplyElementPropertyFilter();
            return;
        }

        string scopeLabel = "Voxel materials";
        var rows = new List<VoxelIngredientGridRow>();
        using JsonDocument summaryDocument = JsonDocument.Parse(summaryJson);
        JsonElement root = summaryDocument.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("Voxel summary JSON root must be an object.");
        }

        if (TryGetJsonString(root, out string scope, "scope") &&
            !string.IsNullOrWhiteSpace(scope))
        {
            scopeLabel = scope.Trim();
        }

        if (!TryGetJsonPropertyIgnoreCase(root, out JsonElement materialsElement, "materials") ||
            materialsElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Voxel summary JSON is missing a materials array.");
        }

        foreach (JsonElement materialElement in materialsElement.EnumerateArray())
        {
            if (materialElement.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            string materialId = TryGetJsonString(materialElement, out string id, "materialId")
                ? id
                : "unknown";
            string materialName = TryGetJsonString(materialElement, out string name, "materialName")
                ? name
                : "Unknown";
            long voxelBlocks = TryGetJsonInt64(materialElement, out long count, "voxelBlocks")
                ? count
                : 0L;
            double liters = TryGetJsonDouble(materialElement, out double volume, "volumeLiters")
                ? volume
                : 0d;

            rows.Add(new VoxelIngredientGridRow(materialId, materialName, voxelBlocks, liters));
        }

        _voxelIngredientRows.Clear();
        _voxelIngredientRows.AddRange(
            rows
                .OrderBy(static row => row.MaterialName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(static row => row.MaterialId, StringComparer.OrdinalIgnoreCase));
        _voxelIngredientScopeLabel = scopeLabel;
        ApplyElementPropertyFilter();
        if (AutoCollapseToFirstLevel)
        {
            ElementPropertiesModel.CollapseAll(minDepth: 0);
        }
    }

    private static bool TryGetJsonPropertyIgnoreCase(
        JsonElement jsonObject,
        out JsonElement value,
        params string[] propertyNames)
    {
        if (jsonObject.ValueKind != JsonValueKind.Object || propertyNames is null || propertyNames.Length == 0)
        {
            value = default;
            return false;
        }

        foreach (JsonProperty property in jsonObject.EnumerateObject())
        {
            for (int i = 0; i < propertyNames.Length; i++)
            {
                if (string.Equals(property.Name, propertyNames[i], StringComparison.OrdinalIgnoreCase))
                {
                    value = property.Value;
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static bool TryGetJsonString(JsonElement jsonObject, out string value, params string[] propertyNames)
    {
        value = string.Empty;
        if (!TryGetJsonPropertyIgnoreCase(jsonObject, out JsonElement propertyValue, propertyNames))
        {
            return false;
        }

        if (propertyValue.ValueKind == JsonValueKind.String)
        {
            value = propertyValue.GetString() ?? string.Empty;
            return true;
        }

        if (propertyValue.ValueKind == JsonValueKind.Number ||
            propertyValue.ValueKind == JsonValueKind.True ||
            propertyValue.ValueKind == JsonValueKind.False)
        {
            value = propertyValue.GetRawText();
            return true;
        }

        return false;
    }

    private static bool TryGetJsonInt64(JsonElement jsonObject, out long value, params string[] propertyNames)
    {
        value = 0L;
        if (!TryGetJsonPropertyIgnoreCase(jsonObject, out JsonElement propertyValue, propertyNames))
        {
            return false;
        }

        if (propertyValue.ValueKind == JsonValueKind.Number && propertyValue.TryGetInt64(out long numeric))
        {
            value = numeric;
            return true;
        }

        if (propertyValue.ValueKind == JsonValueKind.String &&
            long.TryParse(propertyValue.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetJsonDouble(JsonElement jsonObject, out double value, params string[] propertyNames)
    {
        value = 0d;
        if (!TryGetJsonPropertyIgnoreCase(jsonObject, out JsonElement propertyValue, propertyNames))
        {
            return false;
        }

        if (propertyValue.ValueKind == JsonValueKind.Number && propertyValue.TryGetDouble(out double numeric))
        {
            value = numeric;
            return true;
        }

        if (propertyValue.ValueKind == JsonValueKind.String &&
            double.TryParse(
                propertyValue.GetString(),
                NumberStyles.Float | NumberStyles.AllowThousands,
                CultureInfo.InvariantCulture,
                out double parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }
}
