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
    private static PropertyTreeRow BuildDatabankJsonNode(
        string label,
        JsonElement element,
        ElementPropertyRecord sourceRecord)
    {
        string fullContent;
        string nodeKind = element.ValueKind switch
        {
            JsonValueKind.Object => "Json Object",
            JsonValueKind.Array => "Json Array",
            JsonValueKind.String => "Json String",
            JsonValueKind.Number => "Json Number",
            JsonValueKind.True => "Json Bool",
            JsonValueKind.False => "Json Bool",
            JsonValueKind.Null => "Json Null",
            _ => "Json Value"
        };

        string preview;
        bool hasEmbeddedJsonChildren = false;
        JsonElement embeddedJsonRoot = default;

        if (element.ValueKind == JsonValueKind.String)
        {
            string textValue = element.GetString() ?? string.Empty;
            fullContent = textValue;
            preview = BuildPreview(textValue);

            if (TryParseDatabankJson(textValue, out JsonDocument? embeddedDocument) &&
                embeddedDocument is not null)
            {
                using (embeddedDocument)
                {
                    embeddedJsonRoot = embeddedDocument.RootElement.Clone();
                }

                hasEmbeddedJsonChildren = true;
                nodeKind = "Json String (embedded JSON)";
                fullContent = SerializeJsonElement(embeddedJsonRoot);
                preview = embeddedJsonRoot.ValueKind switch
                {
                    JsonValueKind.Object => $"{embeddedJsonRoot.EnumerateObject().Count()} keys",
                    JsonValueKind.Array => $"{embeddedJsonRoot.GetArrayLength()} items",
                    _ => BuildPreview(embeddedJsonRoot.ToString())
                };
            }
        }
        else
        {
            fullContent = SerializeJsonElement(element);
            preview = element.ValueKind switch
            {
                JsonValueKind.Object => $"{element.EnumerateObject().Count()} keys",
                JsonValueKind.Array => $"{element.GetArrayLength()} items",
                _ => BuildPreview(element.ToString())
            };
        }

        var node = new PropertyTreeRow(
            label,
            nodeKind,
            sourceRecord.ElementId,
            sourceRecord.ElementDisplayName,
            sourceRecord.Name,
            sourceRecord.PropertyType,
            sourceRecord.ByteLength,
            preview,
            fullContent);

        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty childProperty in element.EnumerateObject())
            {
                node.Children.Add(BuildDatabankJsonNode(childProperty.Name, childProperty.Value, sourceRecord));
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            int index = 0;
            foreach (JsonElement childItem in element.EnumerateArray())
            {
                index++;
                node.Children.Add(BuildDatabankJsonNode($"item_{index:000}", childItem, sourceRecord));
            }
        }
        else if (hasEmbeddedJsonChildren)
        {
            if (embeddedJsonRoot.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty childProperty in embeddedJsonRoot.EnumerateObject())
                {
                    node.Children.Add(BuildDatabankJsonNode(childProperty.Name, childProperty.Value, sourceRecord));
                }
            }
            else if (embeddedJsonRoot.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement childItem in embeddedJsonRoot.EnumerateArray())
                {
                    index++;
                    node.Children.Add(BuildDatabankJsonNode($"item_{index:000}", childItem, sourceRecord));
                }
            }
        }

        return node;
    }

    private static string SerializeJsonElement(JsonElement element)
    {
        return JsonSerializer.Serialize(element, new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    private static bool TryParseDatabankJson(string input, out JsonDocument? document)
    {
        document = null;
        if (string.IsNullOrWhiteSpace(input))
        {
            return false;
        }

        if (TryParseJsonDocument(input.Trim(), out document))
        {
            return true;
        }

        string trimmed = input.Trim();
        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[^1] == '"' &&
            TryDeserializeJsonString(trimmed, out string unescaped) &&
            TryParseJsonDocument(unescaped.Trim(), out document))
        {
            return true;
        }

        if (TryNormalizeJsObjectLiteral(trimmed, out string normalizedLiteral) &&
            TryParseJsonDocument(normalizedLiteral, out document))
        {
            return true;
        }

        if (TryExtractJsonSubstring(trimmed, out string extracted))
        {
            if (TryParseJsonDocument(extracted, out document))
            {
                return true;
            }

            if (TryNormalizeJsObjectLiteral(extracted, out string normalizedExtracted) &&
                TryParseJsonDocument(normalizedExtracted, out document))
            {
                return true;
            }
        }

        if (TryWrapAsJsonObject(trimmed, out string wrappedObject))
        {
            if (TryParseJsonDocument(wrappedObject, out document))
            {
                return true;
            }

            if (TryNormalizeJsObjectLiteral(wrappedObject, out string normalizedWrapped) &&
                TryParseJsonDocument(normalizedWrapped, out document))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryParseJsonDocument(string candidate, out JsonDocument? document)
    {
        document = null;
        try
        {
            document = JsonDocument.Parse(candidate);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryDeserializeJsonString(string candidate, out string text)
    {
        text = string.Empty;
        try
        {
            text = JsonSerializer.Deserialize<string>(candidate) ?? string.Empty;
            return !string.IsNullOrWhiteSpace(text);
        }
        catch
        {
            return false;
        }
    }

    private static bool TryExtractJsonSubstring(string input, out string extracted)
    {
        extracted = string.Empty;
        int objectStart = input.IndexOf('{');
        int objectEnd = input.LastIndexOf('}');
        int arrayStart = input.IndexOf('[');
        int arrayEnd = input.LastIndexOf(']');

        string objectCandidate = objectStart >= 0 && objectEnd > objectStart
            ? input[objectStart..(objectEnd + 1)]
            : string.Empty;
        string arrayCandidate = arrayStart >= 0 && arrayEnd > arrayStart
            ? input[arrayStart..(arrayEnd + 1)]
            : string.Empty;

        string candidate = objectCandidate.Length >= arrayCandidate.Length
            ? objectCandidate
            : arrayCandidate;

        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        extracted = candidate;
        return true;
    }

    private static bool TryWrapAsJsonObject(string input, out string wrapped)
    {
        wrapped = string.Empty;
        string candidate = input.Trim().Trim(',');
        if (candidate.Length == 0 || candidate.Contains('{') || candidate.Contains('['))
        {
            return false;
        }

        if (!candidate.Contains(':'))
        {
            return false;
        }

        wrapped = "{" + candidate + "}";
        return true;
    }

    private static bool TryNormalizeJsObjectLiteral(string input, out string normalizedJson)
    {
        normalizedJson = string.Empty;
        if (string.IsNullOrWhiteSpace(input))
        {
            return false;
        }

        string candidate = input.Trim();
        if (!candidate.Contains(':'))
        {
            return false;
        }

        candidate = Regex.Replace(candidate, ",\\s*(?=[}\\]])", string.Empty);
        candidate = Regex.Replace(
            candidate,
            "(?<prefix>[{,]\\s*)(?<key>[A-Za-z_][A-Za-z0-9_]*)(?<suffix>\\s*:)",
            "${prefix}\"${key}\"${suffix}");
        candidate = Regex.Replace(
            candidate,
            "'((?:\\\\.|[^'\\\\])*)'",
            match =>
            {
                string value = match.Groups[1].Value;
                value = value.Replace("\\'", "'");
                value = value.Replace("\"", "\\\"");
                return "\"" + value + "\"";
            });

        normalizedJson = candidate;
        return true;
    }

    private static IReadOnlyList<(string Title, string Content)> SplitLuaSections(string text)
    {
        string normalized = text.Replace("\r\n", "\n");
        string[] lines = normalized.Split('\n');
        var sections = new List<(string Title, string Content)>();
        int currentStart = -1;
        string currentTitle = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i];
            if (!line.StartsWith("-- ===== ", StringComparison.Ordinal))
            {
                continue;
            }

            if (currentStart >= 0)
            {
                sections.Add((currentTitle, JoinLines(lines, currentStart, i - 1)));
            }

            currentTitle = ExtractLuaSectionTitle(line);
            currentStart = i + 1;
        }

        if (currentStart >= 0)
        {
            sections.Add((currentTitle, JoinLines(lines, currentStart, lines.Length - 1)));
        }

        return sections;
    }

    private static string ExtractLuaSectionTitle(string markerLine)
    {
        const string prefix = "-- ===== ";
        const string suffix = " =====";
        string inner = markerLine;
        if (inner.StartsWith(prefix, StringComparison.Ordinal))
        {
            inner = inner[prefix.Length..];
        }

        if (inner.EndsWith(suffix, StringComparison.Ordinal))
        {
            inner = inner[..^suffix.Length];
        }

        return inner.Trim();
    }

    private static string JoinLines(string[] lines, int start, int end)
    {
        if (start > end || start < 0 || end >= lines.Length)
        {
            return string.Empty;
        }

        return string.Join(Environment.NewLine, lines[start..(end + 1)]).Trim();
    }

    private static bool TryBuildJsonParts(ElementPropertyRecord record, out List<PropertyTreeRow> parts)
    {
        parts = new List<PropertyTreeRow>();
        string trimmed = record.DecodedValue.Trim();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '['))
        {
            return false;
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(trimmed);
            JsonElement root = document.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in root.EnumerateObject())
                {
                    string serialized = JsonSerializer.Serialize(property.Value, new JsonSerializerOptions { WriteIndented = true });
                    parts.Add(new PropertyTreeRow(
                        property.Name,
                        "Part",
                        record.ElementId,
                        record.ElementDisplayName,
                        record.Name,
                        record.PropertyType,
                        record.ByteLength,
                        BuildPreview(serialized),
                        serialized));
                }
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement item in root.EnumerateArray())
                {
                    index++;
                    string serialized = JsonSerializer.Serialize(item, new JsonSerializerOptions { WriteIndented = true });
                    parts.Add(new PropertyTreeRow(
                        $"item_{index:000}",
                        "Part",
                        record.ElementId,
                        record.ElementDisplayName,
                        record.Name,
                        record.PropertyType,
                        record.ByteLength,
                        BuildPreview(serialized),
                        serialized));
                }
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildPreview(string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return string.Empty;
        }

        string normalized = content.Replace("\r\n", "\n").Replace('\r', '\n');
        int firstBreak = normalized.IndexOf('\n');
        string firstLine = firstBreak >= 0 ? normalized[..firstBreak] : normalized;
        string collapsed = Regex.Replace(firstLine.Trim(), "\\s+", " ");
        return collapsed.Length > 140 ? $"{collapsed[..140]}..." : collapsed;
    }

    private static PropertyTreeRow? ResolveSelectedTreeRow(object? selected)
    {
        if (selected is HierarchicalNode<PropertyTreeRow> typedNode)
        {
            return typedNode.Item;
        }

        if (selected is HierarchicalNode node && node.Item is PropertyTreeRow row)
        {
            return row;
        }

        return selected as PropertyTreeRow;
    }

    public bool TryGetSelectedLuaBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedDpuyaml6Node, ".lua", IsLuaSaveNode, out request);
    }

    public bool TryGetSelectedLuaEditorSourceContext(out LuaEditorSourceContext? context)
    {
        context = null;
        object? selectedNode = SelectedDpuyaml6Node;
        PropertyTreeRow? row = ResolveSelectedTreeRow(selectedNode);
        if (!IsLuaSaveNode(row))
        {
            return false;
        }

        string nodeLabelForContext = ResolveLuaNodeLabelForContext(selectedNode, row!);
        context = new LuaEditorSourceContext(
            row!.ElementId,
            row.ElementDisplayName ?? string.Empty,
            nodeLabelForContext,
            row.PropertyName ?? string.Empty,
            BuildSuggestedFileName(row, ".lua"));
        return true;
    }

    private static string ResolveLuaNodeLabelForContext(object? selectedNode, PropertyTreeRow row)
    {
        string nodeLabel = row.NodeLabel ?? string.Empty;
        if (!row.NodeKind.StartsWith(LuaPartNodeKindPrefix, StringComparison.Ordinal))
        {
            return nodeLabel;
        }

        string? componentLabel = selectedNode switch
        {
            HierarchicalNode<PropertyTreeRow> typedNode =>
                typedNode.Parent?.Item is PropertyTreeRow parentTypedRow &&
                string.Equals(parentTypedRow.NodeKind, "Component", StringComparison.Ordinal)
                    ? parentTypedRow.NodeLabel
                    : null,
            HierarchicalNode untypedNode =>
                untypedNode.Parent?.Item is PropertyTreeRow parentRow &&
                string.Equals(parentRow.NodeKind, "Component", StringComparison.Ordinal)
                    ? parentRow.NodeLabel
                    : null,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(componentLabel))
        {
            return nodeLabel;
        }

        return $"{componentLabel} / {nodeLabel}";
    }

    public bool TryGetSelectedHtmlRsBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedContent2Node, ".lua", IsMainBlobNode, out request);
    }

    public bool TryGetSelectedDatabankBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedDatabankNode, ".json", IsMainBlobNode, out request);
    }

    public bool TrySelectElementCodeBlockTab(ulong elementId)
    {
        if (elementId == 0UL)
        {
            return false;
        }

        if (_luaBlockNodeByElementId.TryGetValue(elementId, out PropertyTreeRow? luaBlock))
        {
            ConstructDataTabIndex = 2;
            SelectedDpuyaml6Node = ResolveTreeSelectionTarget(Dpuyaml6Model, luaBlock);
            return true;
        }

        if (_htmlRsBlockNodeByElementId.TryGetValue(elementId, out PropertyTreeRow? htmlRsBlock))
        {
            ConstructDataTabIndex = 3;
            SelectedContent2Node = ResolveTreeSelectionTarget(Content2Model, htmlRsBlock);
            return true;
        }

        if (_databankBlockNodeByElementId.TryGetValue(elementId, out PropertyTreeRow? databankBlock))
        {
            ConstructDataTabIndex = 4;
            SelectedDatabankNode = ResolveTreeSelectionTarget(DatabankModel, databankBlock);
            return true;
        }

        return false;
    }

    private static bool TryBuildSelectedBlobSaveRequest(
        object? selectedNode,
        string extension,
        Func<PropertyTreeRow?, bool> saveRule,
        out BlobSaveRequest? request)
    {
        request = null;
        PropertyTreeRow? row = ResolveSelectedTreeRow(selectedNode);
        if (!saveRule(row))
        {
            return false;
        }

        string suggestedName = BuildSuggestedFileName(row!, extension);
        string content = row!.FullContent ?? string.Empty;
        request = new BlobSaveRequest(suggestedName, content, extension);
        return true;
    }

    private static bool IsMainBlobNode(PropertyTreeRow? row)
    {
        return row is not null && string.Equals(row.NodeKind, "Block", StringComparison.Ordinal);
    }

    private static bool IsLuaSaveNode(PropertyTreeRow? row)
    {
        if (row is null)
        {
            return false;
        }

        return string.Equals(row.NodeKind, "Block", StringComparison.Ordinal) ||
               string.Equals(row.NodeKind, "Part", StringComparison.Ordinal) ||
               row.NodeKind.StartsWith(LuaPartNodeKindPrefix, StringComparison.Ordinal);
    }

    private static string BuildSuggestedFileName(PropertyTreeRow row, string extension)
    {
        string elementName = !string.IsNullOrWhiteSpace(row.ElementDisplayName)
            ? row.ElementDisplayName
            : row.ElementTypeName;
        if (string.IsNullOrWhiteSpace(elementName))
        {
            elementName = "element";
        }

        string idPart = row.ElementId.HasValue
            ? row.ElementId.Value.ToString(CultureInfo.InvariantCulture)
            : "unknown";
        string propertyPart = string.IsNullOrWhiteSpace(row.PropertyName) ? "blob" : row.PropertyName;
        string baseName = $"{elementName}_{idPart}_{propertyPart}";
        if (!string.Equals(row.NodeKind, "Block", StringComparison.Ordinal) && !string.IsNullOrWhiteSpace(row.NodeLabel))
        {
            baseName += "_" + row.NodeLabel;
        }

        string sanitized = SanitizeFileName(baseName);
        return sanitized.EndsWith(extension, StringComparison.OrdinalIgnoreCase)
            ? sanitized
            : sanitized + extension;
    }

    private static string SanitizeFileName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "blob";
        }

        string invalid = Regex.Escape(new string(System.IO.Path.GetInvalidFileNameChars()));
        string sanitized = Regex.Replace(value, $"[{invalid}]+", "_");
        sanitized = Regex.Replace(sanitized, "\\s+", "_");
        sanitized = sanitized.Trim('_');
        return string.IsNullOrWhiteSpace(sanitized) ? "blob" : sanitized;
    }

    private static object ResolveTreeSelectionTarget(
        HierarchicalModel<PropertyTreeRow> model,
        PropertyTreeRow row)
    {
        return model.FindNode(row) ?? (object)row;
    }

    private static object? FindNodeBySelectionKey(HierarchicalModel<PropertyTreeRow> model, string selectionKey)
    {
        if (string.IsNullOrWhiteSpace(selectionKey))
        {
            return null;
        }

        foreach (var node in model.Flattened)
        {
            if (node.Item is PropertyTreeRow row &&
                string.Equals(BuildSelectionKey(row), selectionKey, StringComparison.Ordinal))
            {
                return node;
            }
        }

        return null;
    }

    private static string BuildSelectionKey(PropertyTreeRow? row)
    {
        if (row is null || string.Equals(row.NodeKind, "Root", StringComparison.Ordinal))
        {
            return string.Empty;
        }

        string elementPart = row.ElementId.HasValue
            ? row.ElementId.Value.ToString(CultureInfo.InvariantCulture)
            : string.Empty;

        return string.Join(
            "|",
            row.NodeKind ?? string.Empty,
            elementPart,
            row.PropertyName ?? string.Empty,
            row.NodeLabel ?? string.Empty);
    }

}
