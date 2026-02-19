using myDUWorker.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorker.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private bool HasLoadedConstructBrowserElementData()
    {
        return _allRegularProperties.Any(record =>
            record.ElementId > 0UL &&
            !IsBlueprintExportEnvelopeRecord(record));
    }

    public async Task<string> ExportLoadedElementTypeCountsJsonAsync(
        bool useDisplayNames,
        IReadOnlyCollection<ulong>? selectedElementIds,
        CancellationToken cancellationToken)
    {
        var filteredElementIds = selectedElementIds is not null && selectedElementIds.Count > 0
            ? selectedElementIds.Where(id => id > 0UL).ToHashSet()
            : null;
        var elementTypeByElementId = new Dictionary<ulong, ulong>();
        var fallbackDisplayNameByTypeId = new Dictionary<ulong, string>();
        var allElementIds = new HashSet<ulong>();

        foreach (ElementPropertyRecord record in _allRegularProperties)
        {
            if (record.ElementId == 0UL || IsBlueprintExportEnvelopeRecord(record))
            {
                continue;
            }

            allElementIds.Add(record.ElementId);
            if (filteredElementIds is not null && !filteredElementIds.Contains(record.ElementId))
            {
                continue;
            }

            if (IsElementTypeIdSummaryPropertyName(record.Name) &&
                TryParseElementTypeId(record.DecodedValue, out ulong elementTypeId) &&
                elementTypeId > 0UL)
            {
                elementTypeByElementId[record.ElementId] = elementTypeId;
                if (!fallbackDisplayNameByTypeId.ContainsKey(elementTypeId))
                {
                    string fallbackDisplayName = BuildElementTypeDisplayNameFallback(record.ElementDisplayName);
                    if (!string.IsNullOrWhiteSpace(fallbackDisplayName))
                    {
                        fallbackDisplayNameByTypeId[elementTypeId] = fallbackDisplayName;
                    }
                }
            }
        }

        if (allElementIds.Count == 0)
        {
            throw new InvalidOperationException("No element rows are loaded in Construct Browser.");
        }

        IReadOnlyCollection<ulong> targetElementIds = filteredElementIds is null
            ? allElementIds
            : filteredElementIds.Where(allElementIds.Contains).ToArray();
        if (targetElementIds.Count == 0)
        {
            throw new InvalidOperationException("No matching selected element rows were found.");
        }

        var countsByTypeId = new Dictionary<ulong, long>();
        foreach (ulong elementId in targetElementIds)
        {
            if (!elementTypeByElementId.TryGetValue(elementId, out ulong typeId) || typeId == 0UL)
            {
                continue;
            }

            countsByTypeId[typeId] = countsByTypeId.TryGetValue(typeId, out long current)
                ? current + 1L
                : 1L;
        }

        if (countsByTypeId.Count == 0)
        {
            throw new InvalidOperationException("No element_type_id values were found in the current Construct Browser data.");
        }

        IReadOnlyDictionary<ulong, string> dbDisplayNamesByTypeId = new Dictionary<ulong, string>();
        if (useDisplayNames && IsDatabaseOnline())
        {
            try
            {
                DataConnectionOptions options = BuildDbOptions();
                dbDisplayNamesByTypeId = await _dataService.GetItemDefinitionDisplayNamesAsync(
                    options,
                    countsByTypeId.Keys.ToArray(),
                    cancellationToken);
            }
            catch
            {
            }
        }

        IReadOnlyList<ElementTypeCountRecord> rows = countsByTypeId
            .Select(pair =>
            {
                string displayName =
                    dbDisplayNamesByTypeId.TryGetValue(pair.Key, out string? dbName) && !string.IsNullOrWhiteSpace(dbName)
                        ? dbName.Trim()
                        : fallbackDisplayNameByTypeId.TryGetValue(pair.Key, out string? fallbackName)
                            ? fallbackName
                            : string.Empty;
                return new ElementTypeCountRecord(pair.Key, displayName, pair.Value);
            })
            .ToArray();

        return BuildElementTypeCountsJson(rows, useDisplayNames);
    }

    public async Task<string> ExportBlueprintElementTypeCountsJsonAsync(
        IReadOnlyCollection<ulong> blueprintIds,
        bool useDisplayNames,
        CancellationToken cancellationToken)
    {
        if (blueprintIds is null || blueprintIds.Count == 0)
        {
            throw new InvalidOperationException("No blueprint rows selected for export.");
        }

        DataConnectionOptions options = BuildDbOptions();
        IReadOnlyList<ElementTypeCountRecord> rows = await _dataService.GetBlueprintElementTypeCountsAsync(
            options,
            blueprintIds,
            cancellationToken);
        if (rows.Count == 0)
        {
            throw new InvalidOperationException("No element rows found for the selected blueprint set.");
        }

        return BuildElementTypeCountsJson(rows, useDisplayNames);
    }

    private static string BuildElementTypeCountsJson(
        IReadOnlyList<ElementTypeCountRecord> rows,
        bool useDisplayNames)
    {
        IEnumerable<ElementTypeCountRecord> ordered = useDisplayNames
            ? rows.OrderBy(
                    row => ResolveElementTypeDisplayName(row.DisplayName, row.ElementTypeId),
                    StringComparer.OrdinalIgnoreCase)
                .ThenBy(row => row.ElementTypeId)
            : rows.OrderBy(row => row.ElementTypeId);

        var array = new JsonArray();
        foreach (ElementTypeCountRecord row in ordered)
        {
            var item = new JsonObject();
            if (useDisplayNames)
            {
                item["Name"] = ResolveElementTypeDisplayName(row.DisplayName, row.ElementTypeId);
            }
            else
            {
                item["element_type_id"] = row.ElementTypeId;
            }

            item["Quantity"] = (double)row.Quantity;
            array.Add(item);
        }

        return array.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }

    private static string ResolveElementTypeDisplayName(string? displayName, ulong elementTypeId)
    {
        if (!string.IsNullOrWhiteSpace(displayName))
        {
            return displayName.Trim();
        }

        return $"type_{elementTypeId.ToString(CultureInfo.InvariantCulture)}";
    }

    private static bool IsElementTypeIdSummaryPropertyName(string? propertyName)
    {
        string normalized = NormalizePropertyName(propertyName);
        return string.Equals(normalized, "elementType", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalized, "element_type_id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalized, "elementTypeId", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryParseElementTypeId(string? value, out ulong elementTypeId)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            elementTypeId = 0UL;
            return false;
        }

        return ulong.TryParse(
            value.Trim(),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out elementTypeId);
    }

    private static string BuildElementTypeDisplayNameFallback(string elementDisplayName)
    {
        if (string.IsNullOrWhiteSpace(elementDisplayName))
        {
            return string.Empty;
        }

        string typeName = DeriveElementTypeName(elementDisplayName).Trim();
        if (string.IsNullOrWhiteSpace(typeName))
        {
            return string.Empty;
        }

        if (string.Equals(typeName, "Element", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(typeName, "Elements", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(typeName, "BlueprintElement", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        return typeName;
    }
}
