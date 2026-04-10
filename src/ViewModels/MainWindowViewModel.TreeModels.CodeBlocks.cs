using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorkbench.Helpers;
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
    private static PropertyTreeRow BuildCodeBlockTreeRoot(
        IReadOnlyList<ElementPropertyRecord> records,
        IReadOnlyDictionary<ulong, string>? preferredNamesByElementId,
        Func<ElementPropertyRecord, IReadOnlyList<PropertyTreeRow>> partBuilder,
        string rootLabel)
    {
        PropertyTreeRow root = CreateRootNode(rootLabel);
        foreach (ElementPropertyRecord record in records
                     .OrderBy(r => r.ElementId)
                     .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase))
        {
            string preferredElementName = ResolveElementName(record.ElementId, preferredNamesByElementId);
            string blockLabel = BuildElementNodeLabel(record.ElementDisplayName, preferredElementName);
            if (string.IsNullOrWhiteSpace(blockLabel))
            {
                blockLabel = string.IsNullOrWhiteSpace(record.ElementDisplayName)
                    ? $"Element {record.ElementId.ToString(CultureInfo.InvariantCulture)}"
                    : record.ElementDisplayName;
            }

            var blockNode = CreatePropertyLeaf(record, blockLabel, "Block", preferredElementName);
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
        IReadOnlyDictionary<ulong, string>? preferredNamesByElementId,
        Func<ElementPropertyRecord, IReadOnlyList<PropertyTreeRow>> partBuilder,
        string rootLabel)
    {
        PropertyTreeRow root = BuildCodeBlockTreeRoot(records, preferredNamesByElementId, partBuilder, rootLabel);
        model.SetRoot(root);
    }

    private void RefreshCodeBlockNodeIndexes(
        PropertyTreeRow luaRoot,
        PropertyTreeRow htmlRsRoot,
        PropertyTreeRow databankRoot)
    {
        PopulateBlockNodeIndex(_luaBlockNodeByElementId, luaRoot);
        PopulateBlockNodeIndex(_htmlRsBlockNodeByElementId, htmlRsRoot);
        PopulateBlockNodeIndex(_databankBlockNodeByElementId, databankRoot);
    }

    private static void PopulateBlockNodeIndex(
        IDictionary<ulong, PropertyTreeRow> target,
        PropertyTreeRow root)
    {
        target.Clear();
        if (root.Children.Count == 0)
        {
            return;
        }

        foreach (PropertyTreeRow child in root.Children)
        {
            if (child.ElementId is not ulong elementId ||
                elementId == 0UL ||
                !string.Equals(child.NodeKind, "Block", StringComparison.Ordinal))
            {
                continue;
            }

            if (!target.ContainsKey(elementId))
            {
                target[elementId] = child;
            }
        }
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
            string componentKey = LuaSectionComponentOrder.NormalizeComponentKey(componentDisplay);
            if (!groupedSections.TryGetValue(componentKey, out List<(string EventLabel, string Content)>? componentRows))
            {
                componentRows = new List<(string EventLabel, string Content)>();
                groupedSections[componentKey] = componentRows;
                componentDisplayByKey[componentKey] = componentDisplay;
            }

            componentRows.Add((eventLabel, content));
        }

        foreach (string componentKey in LuaSectionComponentOrder.OrderKeys(groupedSections.Keys))
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
