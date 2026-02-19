using Avalonia.Controls;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Controls.Primitives;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia;
using Avalonia.Platform;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using AvaloniaEdit.Document;
using AvaloniaEdit.Folding;
using AvaloniaEdit.Rendering;
using AvaloniaEdit.TextMate;
using myDUWorkbench.Controls;
using myDUWorkbench.Helpers;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using myDUWorkbench.ViewModels;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using TextMateSharp.Grammars;

namespace myDUWorkbench.Views;

public partial class MainWindow : Window
{
    private void RebuildLuaEditorSectionTree()
    {
        ResetLuaEditorSectionTreeNodes();
        var groupedRows = new Dictionary<string, LuaEditorSectionTreeRow>(StringComparer.Ordinal);
        foreach (string component in LuaCoreComponentOrder)
        {
            groupedRows[component] = new LuaEditorSectionTreeRow(component);
        }

        int index = 0;
        foreach (LuaEditorSectionState section in _luaEditorSections)
        {
            index++;
            (string component, string eventLabel) = SplitLuaSectionTitleForTree(section.Title, index);
            string componentKey = LuaSectionComponentOrder.NormalizeComponentKey(component);
            if (!groupedRows.TryGetValue(componentKey, out LuaEditorSectionTreeRow? componentNode))
            {
                componentNode = new LuaEditorSectionTreeRow(component);
                groupedRows[componentKey] = componentNode;
            }

            var sectionNode = new LuaEditorSectionTreeRow(section, eventLabel);
            componentNode.Children.Add(sectionNode);
            _luaEditorSectionNodeBySection[section] = sectionNode;
        }

        IReadOnlyList<string> orderedKeys = LuaSectionComponentOrder.OrderKeys(groupedRows.Keys);
        IReadOnlyList<LuaEditorSectionTreeRow> roots = orderedKeys
            .Select(key => groupedRows[key])
            .ToArray();
        _luaEditorSectionModel.SetRoots(roots);
        _luaEditorSectionModel.ExpandAll();
    }

    private void ResetLuaEditorSectionTreeNodes()
    {
        foreach (LuaEditorSectionTreeRow node in _luaEditorSectionNodeBySection.Values)
        {
            node.Detach();
        }

        _luaEditorSectionNodeBySection.Clear();
    }

    private void SelectLuaEditorSectionByIndex(int index)
    {
        _luaSuppressSectionSelectionEvent = true;
        if (index >= 0 &&
            index < _luaEditorSections.Count &&
            _luaEditorSectionNodeBySection.TryGetValue(_luaEditorSections[index], out LuaEditorSectionTreeRow? sectionNode))
        {
            var node = _luaEditorSectionModel.FindNode(sectionNode);
            LuaEditorSectionGrid.SelectedItem = node is null ? sectionNode : (object)node;
        }
        else
        {
            LuaEditorSectionGrid.SelectedItem = null;
        }

        _luaSuppressSectionSelectionEvent = false;
    }

    private LuaEditorSectionState? ResolveSelectedLuaEditorSection()
    {
        object? selected = LuaEditorSectionGrid.SelectedItem;
        if (selected is HierarchicalNode<LuaEditorSectionTreeRow> typedNode)
        {
            return typedNode.Item.Section;
        }

        if (selected is HierarchicalNode node && node.Item is LuaEditorSectionTreeRow row)
        {
            return row.Section;
        }

        return selected is LuaEditorSectionTreeRow direct ? direct.Section : null;
    }

    private static (string Component, string EventLabel) SplitLuaSectionTitleForTree(string title, int index)
    {
        string normalized = title?.Trim() ?? string.Empty;
        if (normalized.Length == 0)
        {
            return ("misc", $"handler_{index.ToString("000", CultureInfo.InvariantCulture)}");
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

    private sealed class LuaEditorSectionTreeRow : INotifyPropertyChanged
    {
        private readonly string _eventLabel;

        public LuaEditorSectionTreeRow(string componentLabel)
        {
            ComponentLabel = componentLabel ?? string.Empty;
            _eventLabel = string.Empty;
        }

        public LuaEditorSectionTreeRow(LuaEditorSectionState section, string eventLabel)
        {
            Section = section ?? throw new ArgumentNullException(nameof(section));
            _eventLabel = eventLabel ?? string.Empty;
            ComponentLabel = string.Empty;
            Section.PropertyChanged += OnSectionPropertyChanged;
        }

        public LuaEditorSectionState? Section { get; }
        public string ComponentLabel { get; }
        public ObservableCollection<LuaEditorSectionTreeRow> Children { get; } = new();
        public FontWeight DisplayFontWeight => Section is null ? FontWeight.Bold : FontWeight.Normal;
        public string DisplayLabel => Section is null
            ? ComponentLabel
            : $"{Section.Index:000} {_eventLabel}{(Section.IsDirty ? " *" : string.Empty)}";

        public event PropertyChangedEventHandler? PropertyChanged;

        public void Detach()
        {
            if (Section is not null)
            {
                Section.PropertyChanged -= OnSectionPropertyChanged;
            }
        }

        private void OnSectionPropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (string.Equals(e.PropertyName, nameof(LuaEditorSectionState.IsDirty), StringComparison.Ordinal) ||
                string.Equals(e.PropertyName, nameof(LuaEditorSectionState.DisplayLabel), StringComparison.Ordinal))
            {
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(DisplayLabel)));
            }
        }
    }

    private sealed class LuaEditorSectionState : INotifyPropertyChanged
    {
        public LuaEditorSectionState(int index, string title, string originalContent)
        {
            Index = index;
            Title = title;
            OriginalContent = originalContent ?? string.Empty;
            _currentContent = OriginalContent;
        }

        public int Index { get; }
        public string Title { get; }
        public string OriginalContent { get; }
        public int LastTopVisibleLine { get; set; } = 1;

        private string _currentContent;

        public string CurrentContent
        {
            get => _currentContent;
            set
            {
                string normalized = value ?? string.Empty;
                if (string.Equals(_currentContent, normalized, StringComparison.Ordinal))
                {
                    return;
                }

                _currentContent = normalized;
                OnPropertyChanged();
                OnPropertyChanged(nameof(IsDirty));
                OnPropertyChanged(nameof(DisplayLabel));
            }
        }

        public bool IsDirty => !string.Equals(OriginalContent, CurrentContent, StringComparison.Ordinal);
        public string DisplayLabel => $"{Index:000} {Title}{(IsDirty ? " *" : string.Empty)}";

        public event PropertyChangedEventHandler? PropertyChanged;

        private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    private sealed record ParsedLuaSection(int Index, string Title, string Body);

    private static List<ParsedLuaSection> ParseStructuredLuaSections(string text)
    {
        string normalized = (text ?? string.Empty).Replace("\r\n", "\n");
        string[] lines = normalized.Split('\n');
        var sections = new List<ParsedLuaSection>();
        int currentHeaderLine = -1;
        int currentIndex = 0;
        string currentTitle = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            Match match = LuaSectionHeaderRegex.Match(lines[i]);
            if (!match.Success)
            {
                continue;
            }

            if (currentHeaderLine >= 0)
            {
                sections.Add(new ParsedLuaSection(
                    currentIndex,
                    currentTitle,
                    JoinStructuredSectionBody(lines, currentHeaderLine + 1, i - 1)));
            }

            _ = int.TryParse(match.Groups["index"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out currentIndex);
            currentTitle = match.Groups["title"].Value.Trim();
            currentHeaderLine = i;
        }

        if (currentHeaderLine >= 0)
        {
            sections.Add(new ParsedLuaSection(
                currentIndex,
                currentTitle,
                JoinStructuredSectionBody(lines, currentHeaderLine + 1, lines.Length - 1)));
        }

        return sections;
    }

    private static string JoinStructuredSectionBody(string[] lines, int start, int end)
    {
        if (start > end || start < 0 || end >= lines.Length)
        {
            return string.Empty;
        }

        return string.Join(Environment.NewLine, lines[start..(end + 1)]).TrimEnd();
    }
}
