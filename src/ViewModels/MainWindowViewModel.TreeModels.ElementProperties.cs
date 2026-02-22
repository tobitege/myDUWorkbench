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
    private void RebuildElementPropertiesTree(
        IReadOnlyList<ElementPropertyRecord> records,
        HashSet<string> activePropertyNames,
        string elementTypeFilter,
        HashSet<ulong>? damagedElementIds)
    {
        List<PropertyTreeRow> typeRoots = BuildElementPropertyTreeRoots(
            records,
            activePropertyNames,
            elementTypeFilter,
            damagedElementIds,
            cancellationToken: default,
            progress: null);
        AppendVoxelIngredientRoot(typeRoots);
        ElementPropertiesModel.SetRoots(typeRoots);
        UpdateConstructBrowserEntryCounts(typeRoots);
    }

    private static List<PropertyTreeRow> BuildElementPropertyTreeRoots(
        IReadOnlyList<ElementPropertyRecord> records,
        HashSet<string> activePropertyNames,
        string elementTypeFilter,
        HashSet<ulong>? damagedElementIds,
        CancellationToken cancellationToken,
        IProgress<TreeBuildProgress>? progress)
    {
        Dictionary<string, ElementPropertyRecord> blueprintMetadataByName = BuildBlueprintTopLevelMetadataByName(records);
        HashSet<string> blueprintMetadataNames = blueprintMetadataByName
            .Keys
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        List<ElementPropertyRecord> nonEnvelopeRecords = records
            .Where(r => !IsBlueprintExportEnvelopeRecord(r))
            .ToList();

        List<IGrouping<string, ElementPropertyRecord>> typeGroups = nonEnvelopeRecords
            .GroupBy(r => DeriveElementTypeName(r.ElementDisplayName), StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();
        int totalElements = nonEnvelopeRecords
            .Select(r => r.ElementId)
            .Distinct()
            .Count();
        int processedElements = 0;
        int reportEvery = totalElements < 200 ? 1 : 20;

        var typeRoots = new List<PropertyTreeRow>();
        foreach (IGrouping<string, ElementPropertyRecord> byType in typeGroups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IGrouping<ulong, ElementPropertyRecord>[] byElementGroups = byType
                .OrderBy(r => r.ElementId)
                .GroupBy(r => r.ElementId)
                .ToArray();

            var elementNodes = new List<PropertyTreeRow>();

            foreach (IGrouping<ulong, ElementPropertyRecord> byElement in byElementGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();
                processedElements++;
                if (damagedElementIds is not null && !damagedElementIds.Contains(byElement.Key))
                {
                    if (processedElements % reportEvery == 0 || processedElements == totalElements)
                    {
                        progress?.Report(new TreeBuildProgress(processedElements, totalElements));
                    }
                    continue;
                }

                ElementPropertyRecord first = byElement.First();
                string customElementName = ResolveElementName(byElement);
                string preferredElementName = ResolvePreferredElementName(customElementName, first.ElementDisplayName);
                if (!MatchesElementTypeOrNameFilter(byType.Key, customElementName, preferredElementName, elementTypeFilter))
                {
                    if (processedElements % reportEvery == 0 || processedElements == totalElements)
                    {
                        progress?.Report(new TreeBuildProgress(processedElements, totalElements));
                    }

                    continue;
                }

                int totalElementProperties = byElement.Count(p =>
                    !IsElementNameProperty(p.Name) &&
                    !blueprintMetadataNames.Contains(NormalizePropertyName(p.Name)));
                List<ElementPropertyRecord> visibleProperties = byElement
                    .Where(p => !IsElementNameProperty(p.Name) &&
                                !blueprintMetadataNames.Contains(NormalizePropertyName(p.Name)) &&
                                activePropertyNames.Contains(NormalizePropertyName(p.Name)))
                    .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var elementNode = new PropertyTreeRow(
                    BuildElementNodeLabel(first.ElementDisplayName, preferredElementName),
                    "Element",
                    first.ElementId,
                    first.ElementDisplayName,
                    string.Empty,
                    null,
                    null,
                    $"{visibleProperties.Count}/{totalElementProperties} properties",
                    string.Empty,
                    preferredElementName);

                foreach (ElementPropertyRecord property in visibleProperties)
                {
                    elementNode.Children.Add(CreatePropertyLeaf(property, property.Name, "Property", preferredElementName));
                }

                elementNodes.Add(elementNode);

                if (processedElements % reportEvery == 0 || processedElements == totalElements)
                {
                    progress?.Report(new TreeBuildProgress(processedElements, totalElements));
                }
            }

            if (elementNodes.Count == 0)
            {
                continue;
            }

            var typeNode = new PropertyTreeRow(
                byType.Key,
                "Element Type",
                null,
                byType.Key,
                string.Empty,
                null,
                null,
                $"{elementNodes.Count} elements",
                string.Empty);

            foreach (PropertyTreeRow elementNode in elementNodes)
            {
                typeNode.Children.Add(elementNode);
            }

            typeRoots.Add(typeNode);
        }

        if (blueprintMetadataByName.Count > 0)
        {
            var metadataRoot = new PropertyTreeRow(
                "Blueprint metadata",
                "Blueprint Metadata",
                null,
                "Blueprint metadata",
                string.Empty,
                null,
                null,
                $"{blueprintMetadataByName.Count} fields",
                string.Empty);

            foreach (string metadataName in BlueprintTopLevelMetadataPropertyNames)
            {
                if (!blueprintMetadataByName.TryGetValue(metadataName, out ElementPropertyRecord? metadataRecord))
                {
                    continue;
                }

                metadataRoot.Children.Add(new PropertyTreeRow(
                    metadataRecord.Name,
                    "Blueprint Metadata Field",
                    null,
                    "Blueprint metadata",
                    metadataRecord.Name,
                    metadataRecord.PropertyType,
                    metadataRecord.ByteLength,
                    BuildPreview(metadataRecord.DecodedValue),
                    metadataRecord.DecodedValue,
                    "Blueprint"));
            }

            if (metadataRoot.Children.Count > 0)
            {
                typeRoots.Insert(0, metadataRoot);
            }
        }

        if (processedElements < totalElements)
        {
            progress?.Report(new TreeBuildProgress(totalElements, totalElements));
        }

        return typeRoots;
    }

    private static HashSet<ulong> BuildDamagedElementIdSet(IReadOnlyList<ElementPropertyRecord> records)
    {
        var damaged = new HashSet<ulong>();
        foreach (IGrouping<ulong, ElementPropertyRecord> byElement in records.GroupBy(r => r.ElementId))
        {
            bool hasDestroyedTrue = byElement.Any(p =>
                IsDestroyedPropertyName(p.Name) &&
                TryReadBooleanTrue(p.DecodedValue));
            if (hasDestroyedTrue)
            {
                damaged.Add(byElement.Key);
                continue;
            }

            bool hasRestoreCountPositive = byElement.Any(p =>
                IsRestoreCountPropertyName(p.Name) &&
                TryReadPositiveNumber(p.DecodedValue));
            if (hasRestoreCountPositive)
            {
                damaged.Add(byElement.Key);
            }
        }

        return damaged;
    }

    private static bool TryReadBooleanTrue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string normalized = value.Trim();
        if (string.Equals(normalized, "1", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(normalized, "0", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return bool.TryParse(normalized, out bool parsed) && parsed;
    }

    private static bool TryReadPositiveNumber(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string normalized = value.Trim();
        if (long.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
        {
            return longValue > 0;
        }

        return double.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue) &&
               doubleValue > 0d;
    }

    private static string ResolveElementName(IEnumerable<ElementPropertyRecord> properties)
    {
        ElementPropertyRecord? nameProperty = properties
            .FirstOrDefault(p => IsElementNameProperty(p.Name) && !string.IsNullOrWhiteSpace(p.DecodedValue));

        return nameProperty?.DecodedValue?.Trim() ?? string.Empty;
    }

    private void ApplyRepairToLoadedSnapshot()
    {
        if (_lastSnapshot is null)
        {
            return;
        }

        _allRegularProperties.RemoveAll(record =>
            IsDestroyedPropertyName(record.Name) ||
            IsRestoreCountPropertyName(record.Name));
        OnPropertyChanged(nameof(CanExportConstructBrowserElementSummary));

        IReadOnlyList<ElementPropertyRecord> snapshotProperties = _lastSnapshot.Properties
            .Where(record =>
                !IsDestroyedPropertyName(record.Name) &&
                !IsRestoreCountPropertyName(record.Name))
            .ToList();

        _lastSnapshot = _lastSnapshot with { Properties = snapshotProperties };
        RebuildPropertyFilterRows(_allRegularProperties);
        ApplyElementPropertyFilter();
    }

    private static bool MatchesElementTypeFilter(string elementTypeName, string wildcardFilter)
    {
        if (string.IsNullOrWhiteSpace(wildcardFilter))
        {
            return true;
        }

        return MatchesWildcardPattern(elementTypeName, wildcardFilter);
    }

    private static bool MatchesElementTypeOrNameFilter(
        string elementTypeName,
        string elementName,
        string preferredDisplayName,
        string wildcardFilter)
    {
        if (string.IsNullOrWhiteSpace(wildcardFilter))
        {
            return true;
        }

        return MatchesWildcardPattern(elementTypeName, wildcardFilter) ||
               MatchesWildcardPattern(elementName, wildcardFilter) ||
               MatchesWildcardPattern(preferredDisplayName, wildcardFilter);
    }

    private static Dictionary<ulong, string> BuildElementNameById(IReadOnlyList<ElementPropertyRecord> records)
    {
        var names = new Dictionary<ulong, string>();
        foreach (IGrouping<ulong, ElementPropertyRecord> byElement in records.GroupBy(r => r.ElementId))
        {
            string elementName = ResolveElementName(byElement);
            if (string.IsNullOrWhiteSpace(elementName))
            {
                continue;
            }

            names[byElement.Key] = elementName;
        }

        return names;
    }

    private static Dictionary<ulong, string> BuildPreferredElementDisplayNameById(IReadOnlyList<ElementPropertyRecord> records)
    {
        var names = new Dictionary<ulong, string>();
        foreach (IGrouping<ulong, ElementPropertyRecord> byElement in records.GroupBy(r => r.ElementId))
        {
            ElementPropertyRecord first = byElement.First();
            string customElementName = ResolveElementName(byElement);
            string preferredElementName = ResolvePreferredElementName(customElementName, first.ElementDisplayName);
            if (string.IsNullOrWhiteSpace(preferredElementName))
            {
                continue;
            }

            names[byElement.Key] = preferredElementName;
        }

        return names;
    }

    private static string ResolveElementName(ulong elementId, IReadOnlyDictionary<ulong, string>? namesByElementId)
    {
        if (namesByElementId is null)
        {
            return string.Empty;
        }

        return namesByElementId.TryGetValue(elementId, out string? value) ? value : string.Empty;
    }

    private static string ResolvePreferredElementName(string customElementName, string elementDisplayName)
    {
        if (!string.IsNullOrWhiteSpace(customElementName))
        {
            return customElementName.Trim();
        }

        string typeName = DeriveElementTypeName(elementDisplayName);
        if (string.Equals(typeName, "BlueprintElement", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(typeName, "Elements", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        return typeName;
    }

    private static string BuildElementNodeLabel(string elementDisplayName, string preferredElementName)
    {
        string suffix = ExtractElementIdSuffix(elementDisplayName);
        if (!string.IsNullOrWhiteSpace(preferredElementName))
        {
            return string.IsNullOrWhiteSpace(suffix)
                ? preferredElementName
                : $"{preferredElementName} {suffix}";
        }

        string typeLabel = DeriveElementTypeName(elementDisplayName);
        if (string.Equals(typeLabel, "BlueprintElement", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(typeLabel, "Elements", StringComparison.OrdinalIgnoreCase))
        {
            return string.IsNullOrWhiteSpace(suffix)
                ? "Element"
                : $"Element {suffix}";
        }

        return string.IsNullOrWhiteSpace(suffix)
            ? typeLabel
            : $"{typeLabel} {suffix}";
    }

    private static string ExtractElementIdSuffix(string elementDisplayName)
    {
        if (string.IsNullOrWhiteSpace(elementDisplayName))
        {
            return string.Empty;
        }

        int bracketStart = elementDisplayName.LastIndexOf(" [", StringComparison.Ordinal);
        if (bracketStart < 0 || !elementDisplayName.EndsWith("]", StringComparison.Ordinal))
        {
            return string.Empty;
        }

        int idStart = bracketStart + 2;
        int idLength = elementDisplayName.Length - idStart - 1;
        if (idLength <= 0)
        {
            return string.Empty;
        }

        ReadOnlySpan<char> idSpan = elementDisplayName.AsSpan(idStart, idLength);
        for (int i = 0; i < idSpan.Length; i++)
        {
            if (!char.IsDigit(idSpan[i]))
            {
                return string.Empty;
            }
        }

        return elementDisplayName[bracketStart..];
    }

    private static string DeriveElementTypeName(string elementDisplayName)
    {
        if (string.IsNullOrWhiteSpace(elementDisplayName))
        {
            return string.Empty;
        }

        int bracketStart = elementDisplayName.LastIndexOf(" [", StringComparison.Ordinal);
        if (bracketStart < 0 || !elementDisplayName.EndsWith("]", StringComparison.Ordinal))
        {
            return elementDisplayName;
        }

        int idStart = bracketStart + 2;
        int idLength = elementDisplayName.Length - idStart - 1;
        if (idLength <= 0)
        {
            return elementDisplayName;
        }

        ReadOnlySpan<char> idSpan = elementDisplayName.AsSpan(idStart, idLength);
        for (int i = 0; i < idSpan.Length; i++)
        {
            if (!char.IsDigit(idSpan[i]))
            {
                return NormalizeElementTypeLabel(elementDisplayName);
            }
        }

        return NormalizeElementTypeLabel(elementDisplayName[..bracketStart]);
    }

    private static string NormalizeElementTypeLabel(string typeLabel)
    {
        if (string.Equals(typeLabel, "BlueprintElement", StringComparison.OrdinalIgnoreCase))
        {
            return "Elements";
        }

        if (string.Equals(typeLabel, "BlueprintRoot", StringComparison.OrdinalIgnoreCase))
        {
            return "Blueprint export envelope";
        }

        return typeLabel;
    }

    private async Task ApplyLoadedPropertyCollectionsAsync(
        IReadOnlyList<ElementPropertyRecord> records,
        CancellationToken cancellationToken = default,
        Action<double, string>? progressUpdate = null,
        bool buildFilteredView = true)
    {
        cancellationToken.ThrowIfCancellationRequested();
        progressUpdate?.Invoke(62d, "Import: categorizing properties");

        List<ElementPropertyRecord> regularProperties = new();
        List<ElementPropertyRecord> dpuyamlProperties = new();
        List<ElementPropertyRecord> content2Properties = new();
        List<ElementPropertyRecord> databankProperties = new();
        int totalRecords = records.Count;
        int categorizeStep = totalRecords < 1000 ? 100 : 1000;
        for (int i = 0; i < totalRecords; i++)
        {
            ElementPropertyRecord record = records[i];
            if (string.Equals(record.Name, "dpuyaml_6", StringComparison.OrdinalIgnoreCase))
            {
                dpuyamlProperties.Add(record);
            }
            else if (string.Equals(record.Name, "content_2", StringComparison.OrdinalIgnoreCase))
            {
                content2Properties.Add(record);
            }
            else if (string.Equals(record.Name, "databank", StringComparison.OrdinalIgnoreCase))
            {
                databankProperties.Add(record);
            }
            else
            {
                regularProperties.Add(record);
            }

            if ((i + 1) % categorizeStep == 0 || i + 1 == totalRecords)
            {
                double ratio = totalRecords == 0 ? 1d : (i + 1) / (double)totalRecords;
                double percent = 62d + (6d * ratio);
                progressUpdate?.Invoke(percent, $"Import: categorizing properties ({i + 1}/{totalRecords})");
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        progressUpdate?.Invoke(68d, "Import: preparing main tree");

        _allRegularProperties.Clear();
        _allRegularProperties.AddRange(regularProperties);
        _voxelIngredientRows.Clear();
        _voxelIngredientScopeLabel = string.Empty;
        OnPropertyChanged(nameof(CanExportConstructBrowserElementSummary));
        RebuildPropertyFilterRows(regularProperties);
        if (buildFilteredView)
        {
            await ApplyElementPropertyFilterAsync(cancellationToken, progressUpdate, 68d, 76d);
        }

        cancellationToken.ThrowIfCancellationRequested();
        progressUpdate?.Invoke(76d, "Import: building LUA/HTML/Databank trees");

        Task<PropertyTreeRow> luaTreeTask = Task.Run(
            () => BuildCodeBlockTreeRoot(dpuyamlProperties, BuildLuaPartRows, "LUA blocks"),
            cancellationToken);
        Task<PropertyTreeRow> htmlTreeTask = Task.Run(
            () => BuildCodeBlockTreeRoot(content2Properties, BuildContentPartRows, "HTML/RS"),
            cancellationToken);
        Task<PropertyTreeRow> databankTreeTask = Task.Run(
            () => BuildCodeBlockTreeRoot(databankProperties, BuildDatabankPartRows, "Databank"),
            cancellationToken);

        PropertyTreeRow luaRoot = await luaTreeTask;
        PropertyTreeRow htmlRoot = await htmlTreeTask;
        PropertyTreeRow databankRoot = await databankTreeTask;

        await ReplaceCollectionAsync(
            Dpuyaml6Properties,
            dpuyamlProperties,
            200,
            cancellationToken,
            (processed, total) =>
            {
                double ratio = total <= 0 ? 1d : Math.Clamp(processed / (double)total, 0d, 1d);
                progressUpdate?.Invoke(76d + (4d * ratio), $"Import: applying LUA rows ({processed}/{total})");
            });
        await ReplaceCollectionAsync(
            Content2Properties,
            content2Properties,
            200,
            cancellationToken,
            (processed, total) =>
            {
                double ratio = total <= 0 ? 1d : Math.Clamp(processed / (double)total, 0d, 1d);
                progressUpdate?.Invoke(80d + (4d * ratio), $"Import: applying HTML/RS rows ({processed}/{total})");
            });
        await ReplaceCollectionAsync(
            DatabankProperties,
            databankProperties,
            200,
            cancellationToken,
            (processed, total) =>
            {
                double ratio = total <= 0 ? 1d : Math.Clamp(processed / (double)total, 0d, 1d);
                progressUpdate?.Invoke(84d + (4d * ratio), $"Import: applying databank rows ({processed}/{total})");
            });

        Dpuyaml6Model.SetRoot(luaRoot);
        Content2Model.SetRoot(htmlRoot);
        DatabankModel.SetRoot(databankRoot);
        RefreshCodeBlockNodeIndexes(luaRoot, htmlRoot, databankRoot);
        progressUpdate?.Invoke(92d, "Import: finalizing view");

        if (AutoCollapseToFirstLevel)
        {
            ElementPropertiesModel.CollapseAll(minDepth: 0);
            Dpuyaml6Model.CollapseAll(minDepth: 1);
            Content2Model.CollapseAll(minDepth: 1);
            DatabankModel.CollapseAll(minDepth: 1);
        }

        SelectedDpuyaml6Node = FindNodeBySelectionKey(Dpuyaml6Model, _selectedDpuyamlNodeKey);
        SelectedContent2Node = FindNodeBySelectionKey(Content2Model, _selectedContent2NodeKey);
        SelectedDatabankNode = FindNodeBySelectionKey(DatabankModel, _selectedDatabankNodeKey);

        if (SelectedDpuyaml6Node is null)
        {
            SelectedDpuyaml6Content = string.Empty;
        }

        if (SelectedContent2Node is null)
        {
            SelectedContent2Content = string.Empty;
        }

        if (SelectedDatabankNode is null)
        {
            SelectedDatabankContent = string.Empty;
        }

        if (buildFilteredView)
        {
            progressUpdate?.Invoke(96d, "Import: applying filters");
        }
        else
        {
            progressUpdate?.Invoke(86d, "Import: preparing filters");
        }
    }

    private void AppendVoxelIngredientRoot(ICollection<PropertyTreeRow> roots)
    {
        if (_voxelIngredientRows.Count == 0)
        {
            return;
        }

        string scopeSuffix = string.IsNullOrWhiteSpace(_voxelIngredientScopeLabel)
            ? string.Empty
            : $" ({_voxelIngredientScopeLabel})";
        var voxelRoot = new PropertyTreeRow(
            "Voxels",
            "Voxels",
            null,
            "Voxels",
            string.Empty,
            null,
            null,
            $"{_voxelIngredientRows.Count.ToString("N0", CultureInfo.CurrentCulture)} ingredients{scopeSuffix}",
            string.Empty,
            "Voxels");

        foreach (VoxelIngredientGridRow ingredient in _voxelIngredientRows
                     .OrderBy(static row => row.MaterialName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(static row => row.MaterialId, StringComparer.OrdinalIgnoreCase))
        {
            string fullContent =
                $"Material: {ingredient.MaterialName}{Environment.NewLine}" +
                $"MaterialId: {ingredient.MaterialId}{Environment.NewLine}" +
                $"Voxel blocks: {ingredient.VoxelBlocks.ToString("N0", CultureInfo.CurrentCulture)}{Environment.NewLine}" +
                $"Volume liters: {ingredient.VolumeLiters.ToString("N2", CultureInfo.CurrentCulture)}";

            voxelRoot.Children.Add(new PropertyTreeRow(
                ingredient.MaterialName,
                "Voxel Ingredient",
                null,
                "Voxels",
                ingredient.MaterialId,
                null,
                null,
                $"{ingredient.VoxelBlocks.ToString("N0", CultureInfo.CurrentCulture)} blocks | {ingredient.VolumeLiters.ToString("N2", CultureInfo.CurrentCulture)} L",
                fullContent,
                ingredient.MaterialName));
        }

        roots.Add(voxelRoot);
    }
}
