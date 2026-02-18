using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorker.Models;
using myDUWorker.Services;
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

namespace myDUWorker.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private static HierarchicalModel<PropertyTreeRow> CreateTreeModel()
    {
        var options = new HierarchicalOptions<PropertyTreeRow>
        {
            ItemsSelector = row => row.Children,
            IsLeafSelector = row => row.Children.Count == 0,
            AutoExpandRoot = true,
            MaxAutoExpandDepth = 1,
            VirtualizeChildren = true
        };

        return new HierarchicalModel<PropertyTreeRow>(options);
    }

    private static PropertyTreeRow CreateRootNode(string label)
    {
        return new PropertyTreeRow(
            label,
            "Root",
            null,
            string.Empty,
            string.Empty,
            null,
            null,
            string.Empty,
            string.Empty);
    }

    private static Dictionary<string, bool> SanitizeElementPropertyActiveStates(IEnumerable<KeyValuePair<string, bool>>? source)
    {
        var sanitized = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        if (source is null)
        {
            return sanitized;
        }

        foreach (KeyValuePair<string, bool> entry in source)
        {
            string propertyName = NormalizePropertyName(entry.Key);
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                continue;
            }

            sanitized[propertyName] = entry.Value;
        }

        return sanitized;
    }

    private static string NormalizePropertyName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var sb = new StringBuilder(value.Length);
        foreach (char ch in value)
        {
            if (char.IsControl(ch))
            {
                continue;
            }

            sb.Append(ch);
        }

        return sb.ToString().Trim();
    }

    private static bool IsElementNameProperty(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "name",
            StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsDestroyedPropertyName(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "destroyed",
            StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsRestoreCountPropertyName(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "restoreCount",
            StringComparison.OrdinalIgnoreCase);
    }

    private void RebuildPropertyFilterRows(IReadOnlyList<ElementPropertyRecord> records)
    {
        _elementPropertyActiveStates = SanitizeElementPropertyActiveStates(_elementPropertyActiveStates);
        foreach (PropertyFilterRecord row in ElementPropertyFilters)
        {
            row.PropertyChanged -= OnElementPropertyFilterChanged;
        }

        ElementPropertyFilters.Clear();

        string[] propertyNames = records
            .Select(r => NormalizePropertyName(r.Name))
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Where(n => !IsElementNameProperty(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var validNames = new HashSet<string>(propertyNames, StringComparer.OrdinalIgnoreCase);
        string[] staleNames = _elementPropertyActiveStates.Keys
            .Where(name => !validNames.Contains(name))
            .ToArray();
        foreach (string staleName in staleNames)
        {
            _elementPropertyActiveStates.Remove(staleName);
        }

        foreach (string propertyName in propertyNames)
        {
            bool isActive = !_elementPropertyActiveStates.TryGetValue(propertyName, out bool savedState) || savedState;
            _elementPropertyActiveStates[propertyName] = isActive;
            var filterRow = new PropertyFilterRecord(propertyName, isActive);
            filterRow.PropertyChanged += OnElementPropertyFilterChanged;
            ElementPropertyFilters.Add(filterRow);
        }
    }

    private void OnElementPropertyFilterChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (!string.Equals(e.PropertyName, nameof(PropertyFilterRecord.IsActive), StringComparison.Ordinal))
        {
            return;
        }

        if (sender is not PropertyFilterRecord row)
        {
            return;
        }

        string propertyName = NormalizePropertyName(row.PropertyName);
        if (string.IsNullOrWhiteSpace(propertyName))
        {
            return;
        }

        _elementPropertyActiveStates[propertyName] = row.IsActive;
        if (_isBulkUpdatingElementPropertyFilters)
        {
            return;
        }

        ApplyElementPropertyFilter();
        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            PersistSettingsNow();
        }
    }

    private void SetAllElementPropertyFilters(bool isActive)
    {
        if (ElementPropertyFilters.Count == 0)
        {
            return;
        }

        bool changed = false;
        _isBulkUpdatingElementPropertyFilters = true;
        try
        {
            foreach (PropertyFilterRecord row in ElementPropertyFilters)
            {
                if (row.IsActive == isActive)
                {
                    continue;
                }

                row.IsActive = isActive;
                changed = true;
            }
        }
        finally
        {
            _isBulkUpdatingElementPropertyFilters = false;
        }

        if (!changed)
        {
            return;
        }

        ApplyElementPropertyFilter();
        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            PersistSettingsNow();
        }
    }

    private void ApplyElementPropertyFilter()
    {
        HashSet<string> activeNames = BuildActivePropertyNameSet();
        string elementTypeFilter = ElementTypeNameFilterInput?.Trim() ?? string.Empty;
        HashSet<ulong>? damagedElementIds = DamagedOnly
            ? BuildDamagedElementIdSet(_allRegularProperties)
            : null;
        IReadOnlyDictionary<ulong, string>? elementNameById = string.IsNullOrWhiteSpace(elementTypeFilter)
            ? null
            : BuildElementNameById(_allRegularProperties);

        List<ElementPropertyRecord> filtered = _allRegularProperties
            .Where(r => !IsElementNameProperty(r.Name) &&
                        activeNames.Contains(NormalizePropertyName(r.Name)) &&
                        (damagedElementIds is null || damagedElementIds.Contains(r.ElementId)) &&
                        MatchesElementTypeOrNameFilter(
                            DeriveElementTypeName(r.ElementDisplayName),
                            ResolveElementName(r.ElementId, elementNameById),
                            elementTypeFilter))
            .ToList();

        ElementProperties.Clear();
        foreach (ElementPropertyRecord record in filtered)
        {
            ElementProperties.Add(record);
        }

        RebuildElementPropertiesTree(_allRegularProperties, activeNames, elementTypeFilter, damagedElementIds);
        SelectedElementPropertyNode = FindNodeBySelectionKey(ElementPropertiesModel, _selectedElementNodeKey);
    }

    private async Task ApplyElementPropertyFilterAsync(
        CancellationToken cancellationToken,
        Action<double, string>? progressUpdate,
        double progressStart,
        double progressEnd)
    {
        HashSet<string> activeNames = BuildActivePropertyNameSet();
        string elementTypeFilter = ElementTypeNameFilterInput?.Trim() ?? string.Empty;
        HashSet<ulong>? damagedElementIds = DamagedOnly
            ? BuildDamagedElementIdSet(_allRegularProperties)
            : null;
        IReadOnlyDictionary<ulong, string>? elementNameById = string.IsNullOrWhiteSpace(elementTypeFilter)
            ? null
            : BuildElementNameById(_allRegularProperties);

        List<ElementPropertyRecord> filtered = _allRegularProperties
            .Where(r => !IsElementNameProperty(r.Name) &&
                        activeNames.Contains(NormalizePropertyName(r.Name)) &&
                        (damagedElementIds is null || damagedElementIds.Contains(r.ElementId)) &&
                        MatchesElementTypeOrNameFilter(
                            DeriveElementTypeName(r.ElementDisplayName),
                            ResolveElementName(r.ElementId, elementNameById),
                            elementTypeFilter))
            .ToList();

        double span = Math.Max(1d, progressEnd - progressStart);
        int totalElements = _allRegularProperties.Select(r => r.ElementId).Distinct().Count();
        progressUpdate?.Invoke(
            progressStart,
            totalElements > 0
                ? $"Import: building element tree (0/{totalElements})"
                : "Import: building element tree");

        var treeProgress = new Progress<TreeBuildProgress>(state =>
        {
            int total = state.TotalElements <= 0 ? totalElements : state.TotalElements;
            double ratio = total <= 0 ? 1d : Math.Clamp(state.ProcessedElements / (double)total, 0d, 1d);
            double percent = progressStart + (span * 0.7d * ratio);
            string text = total > 0
                ? $"Import: building element tree ({Math.Min(state.ProcessedElements, total)}/{total})"
                : "Import: building element tree";
            progressUpdate?.Invoke(percent, text);
        });

        List<PropertyTreeRow> elementTreeRoots = await Task.Run(
            () => BuildElementPropertyTreeRoots(
                _allRegularProperties,
                activeNames,
                elementTypeFilter,
                damagedElementIds,
                cancellationToken,
                treeProgress),
            cancellationToken);

        await ReplaceCollectionAsync(
            ElementProperties,
            filtered,
            500,
            cancellationToken,
            (processed, total) =>
            {
                double ratio = total <= 0 ? 1d : Math.Clamp(processed / (double)total, 0d, 1d);
                double percent = progressStart + (span * (0.7d + 0.3d * ratio));
                string text = total > 0
                    ? $"Import: applying rows ({processed}/{total})"
                    : "Import: applying rows";
                progressUpdate?.Invoke(percent, text);
            });

        ElementPropertiesModel.SetRoots(elementTreeRoots);
        SelectedElementPropertyNode = FindNodeBySelectionKey(ElementPropertiesModel, _selectedElementNodeKey);
        progressUpdate?.Invoke(progressEnd, "Import: filters applied");
    }

    private HashSet<string> BuildActivePropertyNameSet()
    {
        return ElementPropertyFilters
            .Where(f => f.IsActive)
            .Select(f => NormalizePropertyName(f.PropertyName))
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private ElementFilterSnapshot CaptureElementFilterSnapshot()
    {
        var propertyStates = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        foreach (PropertyFilterRecord row in ElementPropertyFilters)
        {
            string propertyName = NormalizePropertyName(row.PropertyName);
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                continue;
            }

            propertyStates[propertyName] = row.IsActive;
        }

        return new ElementFilterSnapshot(
            ElementTypeNameFilterInput ?? string.Empty,
            DamagedOnly,
            propertyStates);
    }

    private void ClearFiltersForBlueprintImport(bool applyFilter = true)
    {
        ElementTypeNameFilterInput = string.Empty;
        SelectedElementTypeFilterHistoryItem = null;
        DamagedOnly = false;

        _isBulkUpdatingElementPropertyFilters = true;
        try
        {
            foreach (PropertyFilterRecord row in ElementPropertyFilters)
            {
                row.IsActive = true;
            }
        }
        finally
        {
            _isBulkUpdatingElementPropertyFilters = false;
        }

        if (applyFilter)
        {
            ApplyElementPropertyFilter();
        }
    }

    private void RestoreElementFilters(ElementFilterSnapshot snapshot, bool applyFilter = true)
    {
        ElementTypeNameFilterInput = snapshot.ElementTypeFilterInput;
        DamagedOnly = snapshot.DamagedOnly;

        _isBulkUpdatingElementPropertyFilters = true;
        try
        {
            foreach (PropertyFilterRecord row in ElementPropertyFilters)
            {
                string propertyName = NormalizePropertyName(row.PropertyName);
                if (string.IsNullOrWhiteSpace(propertyName))
                {
                    continue;
                }

                row.IsActive = !snapshot.PropertyStates.TryGetValue(propertyName, out bool wasActive) || wasActive;
                _elementPropertyActiveStates[propertyName] = row.IsActive;
            }
        }
        finally
        {
            _isBulkUpdatingElementPropertyFilters = false;
        }

        if (applyFilter)
        {
            ApplyElementPropertyFilter();
        }
    }

    private void AddElementTypeFilterHistory(string? filterText)
    {
        string normalized = filterText?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return;
        }

        int existingIndex = -1;
        for (int i = 0; i < ElementTypeFilterHistory.Count; i++)
        {
            if (string.Equals(ElementTypeFilterHistory[i], normalized, StringComparison.OrdinalIgnoreCase))
            {
                existingIndex = i;
                break;
            }
        }

        if (existingIndex >= 0)
        {
            ElementTypeFilterHistory.RemoveAt(existingIndex);
        }

        ElementTypeFilterHistory.Insert(0, normalized);
    }

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
        ElementPropertiesModel.SetRoots(typeRoots);
    }

    private static List<PropertyTreeRow> BuildElementPropertyTreeRoots(
        IReadOnlyList<ElementPropertyRecord> records,
        HashSet<string> activePropertyNames,
        string elementTypeFilter,
        HashSet<ulong>? damagedElementIds,
        CancellationToken cancellationToken,
        IProgress<TreeBuildProgress>? progress)
    {
        List<IGrouping<string, ElementPropertyRecord>> typeGroups = records
            .GroupBy(r => DeriveElementTypeName(r.ElementDisplayName), StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();
        int totalElements = records
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
                string elementName = ResolveElementName(byElement);
                if (!MatchesElementTypeOrNameFilter(byType.Key, elementName, elementTypeFilter))
                {
                    if (processedElements % reportEvery == 0 || processedElements == totalElements)
                    {
                        progress?.Report(new TreeBuildProgress(processedElements, totalElements));
                    }

                    continue;
                }

                int totalElementProperties = byElement.Count(p => !IsElementNameProperty(p.Name));
                List<ElementPropertyRecord> visibleProperties = byElement
                    .Where(p => !IsElementNameProperty(p.Name) &&
                                activePropertyNames.Contains(NormalizePropertyName(p.Name)))
                    .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var elementNode = new PropertyTreeRow(
                    first.ElementDisplayName,
                    "Element",
                    first.ElementId,
                    first.ElementDisplayName,
                    string.Empty,
                    null,
                    null,
                    $"{visibleProperties.Count}/{totalElementProperties} properties",
                    string.Empty,
                    elementName);

                foreach (ElementPropertyRecord property in visibleProperties)
                {
                    elementNode.Children.Add(CreatePropertyLeaf(property, property.Name, "Property", elementName));
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

    private static bool MatchesElementTypeOrNameFilter(string elementTypeName, string elementName, string wildcardFilter)
    {
        if (string.IsNullOrWhiteSpace(wildcardFilter))
        {
            return true;
        }

        return MatchesWildcardPattern(elementTypeName, wildcardFilter) ||
               MatchesWildcardPattern(elementName, wildcardFilter);
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

    private static string ResolveElementName(ulong elementId, IReadOnlyDictionary<ulong, string>? namesByElementId)
    {
        if (namesByElementId is null)
        {
            return string.Empty;
        }

        return namesByElementId.TryGetValue(elementId, out string? value) ? value : string.Empty;
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
                return elementDisplayName;
            }
        }

        return elementDisplayName[..bracketStart];
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

    private static PropertyTreeRow BuildCodeBlockTreeRoot(
        IReadOnlyList<ElementPropertyRecord> records,
        Func<ElementPropertyRecord, IReadOnlyList<PropertyTreeRow>> partBuilder,
        string rootLabel)
    {
        PropertyTreeRow root = CreateRootNode(rootLabel);
        foreach (ElementPropertyRecord record in records
                     .OrderBy(r => r.ElementId)
                     .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase))
        {
            string blockLabel = string.IsNullOrWhiteSpace(record.ElementDisplayName)
                ? $"Element {record.ElementId.ToString(CultureInfo.InvariantCulture)}"
                : record.ElementDisplayName;
            var blockNode = CreatePropertyLeaf(record, blockLabel, "Block");
            IReadOnlyList<PropertyTreeRow> parts = partBuilder(record);
            foreach (PropertyTreeRow part in parts)
            {
                blockNode.Children.Add(part);
            }

            root.Children.Add(blockNode);
        }

        return root;
    }

    private static void RebuildCodeBlockTree(
        HierarchicalModel<PropertyTreeRow> model,
        IReadOnlyList<ElementPropertyRecord> records,
        Func<ElementPropertyRecord, IReadOnlyList<PropertyTreeRow>> partBuilder,
        string rootLabel)
    {
        PropertyTreeRow root = BuildCodeBlockTreeRoot(records, partBuilder, rootLabel);
        model.SetRoot(root);
    }

    private static async Task ReplaceCollectionAsync(
        ObservableCollection<ElementPropertyRecord> target,
        IReadOnlyList<ElementPropertyRecord> source,
        int batchSize,
        CancellationToken cancellationToken,
        Action<int, int>? progress = null)
    {
        target.Clear();
        int total = source.Count;
        progress?.Invoke(0, total);
        if (total == 0)
        {
            return;
        }

        int safeBatchSize = batchSize <= 0 ? 200 : batchSize;
        for (int i = 0; i < total; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            target.Add(source[i]);
            if ((i + 1) % safeBatchSize == 0 || i + 1 == total)
            {
                progress?.Invoke(i + 1, total);
                await Task.Yield();
            }
        }
    }

    private static PropertyTreeRow CreatePropertyLeaf(
        ElementPropertyRecord record,
        string nodeLabel,
        string nodeKind,
        string elementName = "")
    {
        return new PropertyTreeRow(
            nodeLabel,
            nodeKind,
            record.ElementId,
            record.ElementDisplayName,
            record.Name,
            record.PropertyType,
            record.ByteLength,
            BuildPreview(record.DecodedValue),
            record.DecodedValue,
            elementName);
    }

    private static IReadOnlyList<PropertyTreeRow> BuildLuaPartRows(ElementPropertyRecord record)
    {
        IReadOnlyList<(string Title, string Content)> sections = SplitLuaSections(record.DecodedValue);
        var rows = new List<PropertyTreeRow>(sections.Count > 0 ? sections.Count : 1);
        if (sections.Count == 0)
        {
            rows.Add(new PropertyTreeRow(
                "part_001",
                "Part",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                BuildPreview(record.DecodedValue),
                record.DecodedValue));
            return rows;
        }

        var groupedSections = new Dictionary<string, List<(string EventLabel, string Content)>>(StringComparer.Ordinal);
        var componentDisplayByKey = new Dictionary<string, string>(StringComparer.Ordinal);
        int index = 0;
        foreach ((string title, string content) in sections)
        {
            index++;
            (string componentDisplay, string eventLabel) = SplitLuaSectionTitle(title, index);
            string componentKey = NormalizeLuaComponentKey(componentDisplay);
            if (!groupedSections.TryGetValue(componentKey, out List<(string EventLabel, string Content)>? componentRows))
            {
                componentRows = new List<(string EventLabel, string Content)>();
                groupedSections[componentKey] = componentRows;
                componentDisplayByKey[componentKey] = componentDisplay;
            }

            componentRows.Add((eventLabel, content));
        }

        foreach (string componentKey in OrderLuaComponentKeys(groupedSections.Keys))
        {
            string componentDisplay = componentDisplayByKey[componentKey];
            List<(string EventLabel, string Content)> componentSections = groupedSections[componentKey];
            var componentNode = new PropertyTreeRow(
                componentDisplay,
                "Component",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                $"{componentSections.Count.ToString(CultureInfo.InvariantCulture)} handlers",
                string.Empty);

            foreach ((string eventLabel, string content) in componentSections)
            {
                componentNode.Children.Add(new PropertyTreeRow(
                    eventLabel,
                    LuaPartNodeKindPrefix + componentKey,
                    record.ElementId,
                    record.ElementDisplayName,
                    record.Name,
                    record.PropertyType,
                    record.ByteLength,
                    BuildPreview(content),
                    content));
            }

            rows.Add(componentNode);
        }

        return rows;
    }

    private static (string ComponentDisplay, string EventLabel) SplitLuaSectionTitle(string title, int index)
    {
        string normalized = title?.Trim() ?? string.Empty;
        if (normalized.Length == 0)
        {
            return ("misc", $"handler_{index:000}");
        }

        int separatorIndex = normalized.IndexOf(" / ", StringComparison.Ordinal);
        if (separatorIndex > 0 && separatorIndex + 3 < normalized.Length)
        {
            string component = normalized[..separatorIndex].Trim();
            string eventLabel = normalized[(separatorIndex + 3)..].Trim();
            if (component.Length > 0 && eventLabel.Length > 0)
            {
                return (component, eventLabel);
            }
        }

        return ("misc", normalized);
    }

    private static string NormalizeLuaComponentKey(string componentDisplay)
    {
        string normalized = (componentDisplay ?? string.Empty).Trim().ToLowerInvariant();
        normalized = Regex.Replace(normalized, "\\s+", " ");
        return string.IsNullOrWhiteSpace(normalized) ? "misc" : normalized;
    }

    private static IReadOnlyList<string> OrderLuaComponentKeys(IEnumerable<string> componentKeys)
    {
        return componentKeys
            .Distinct(StringComparer.Ordinal)
            .OrderBy(GetLuaComponentSortRank)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static int GetLuaComponentSortRank(string key)
    {
        return key switch
        {
            "library" => 0,
            "system" => 1,
            "player" => 2,
            "construct" => 3,
            "unit" => 4,
            _ => TryGetSlotSortRank(key)
        };
    }

    private static int TryGetSlotSortRank(string key)
    {
        if (!key.StartsWith("slot", StringComparison.OrdinalIgnoreCase))
        {
            return 1000;
        }

        ReadOnlySpan<char> suffix = key.AsSpan(4).Trim();
        if (!int.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out int slotNumber) ||
            slotNumber <= 0)
        {
            return 1000;
        }

        return 100 + slotNumber;
    }

    private static IReadOnlyList<PropertyTreeRow> BuildContentPartRows(ElementPropertyRecord record)
    {
        return Array.Empty<PropertyTreeRow>();
    }

    private static IReadOnlyList<PropertyTreeRow> BuildDatabankPartRows(ElementPropertyRecord record)
    {
        if (!TryParseDatabankJson(record.DecodedValue, out JsonDocument? document) || document is null)
        {
            return new[]
            {
                new PropertyTreeRow(
                    "databank",
                    "Part",
                    record.ElementId,
                    record.ElementDisplayName,
                    record.Name,
                    record.PropertyType,
                    record.ByteLength,
                    BuildPreview(record.DecodedValue),
                    record.DecodedValue)
            };
        }

        using (document)
        {
            JsonElement root = document.RootElement;
            var rows = new List<PropertyTreeRow>();
            if (root.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in root.EnumerateObject())
                {
                    rows.Add(BuildDatabankJsonNode(
                        property.Name,
                        property.Value,
                        record));
                }
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement item in root.EnumerateArray())
                {
                    index++;
                    rows.Add(BuildDatabankJsonNode(
                        $"item_{index:000}",
                        item,
                        record));
                }
            }
            else
            {
                rows.Add(BuildDatabankJsonNode("value", root, record));
            }

            if (rows.Count > 0)
            {
                return rows;
            }
        }

        return new[]
        {
            new PropertyTreeRow(
                "databank",
                "Part",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                BuildPreview(record.DecodedValue),
                record.DecodedValue)
        };
    }

}
