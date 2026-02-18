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
}
