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
using myDUWorker.Controls;
using myDUWorker.Helpers;
using myDUWorker.Models;
using myDUWorker.Services;
using myDUWorker.ViewModels;
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

namespace myDUWorker.Views;

public partial class MainWindow : Window
{
    private void EnsureLuaEditorInitialized()
    {
        if (_luaEditorInitialized)
        {
            return;
        }

        if (LuaSourceEditor.Document is null)
        {
            LuaSourceEditor.Document = new TextDocument(string.Empty);
        }

        _luaBreakpointMargin = new BreakpointMargin();
        _luaBreakpointMargin.MarginClicked += LuaBreakpointMargin_Clicked;
        LuaSourceEditor.TextArea.LeftMargins.Insert(0, _luaBreakpointMargin);

        _luaExecutionHighlighter = new ExecutionLineHighlighter(LuaSourceEditor);
        LuaSourceEditor.TextArea.TextView.BackgroundRenderers.Add(_luaExecutionHighlighter);

        _luaFoldingManager = FoldingManager.Install(LuaSourceEditor.TextArea);
        _luaFoldRefreshTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(LuaFoldRefreshDebounceMs) };
        _luaFoldRefreshTimer.Tick += (_, _) =>
        {
            _luaFoldRefreshTimer.Stop();
            RefreshLuaFoldings();
        };

        SetupLuaTextMate();
        EnableLuaOverstrikeToggle();
        LuaSourceEditor.Options.HighlightCurrentLine = true;
        LuaSourceEditor.Options.ShowColumnRulers = true;
        LuaSourceEditor.Document.TextChanged += LuaDocument_TextChanged;
        LuaSourceEditor.TextArea.Caret.PositionChanged += LuaCaret_PositionChanged;
        LuaSourceEditor.TextArea.KeyDown += LuaTextArea_KeyDown;
        LuaSourceEditor.TextArea.KeyUp += LuaTextArea_KeyUp;
        LuaSourceEditor.TextArea.PointerPressed += LuaTextArea_PointerPressed;
        LuaSourceEditor.TextArea.PointerReleased += LuaTextArea_PointerReleased;
        if (LuaHoverTooltipsEnabled)
        {
            LuaSourceEditor.TextArea.PointerMoved += LuaTextArea_PointerMoved;
            LuaSourceEditor.TextArea.PointerExited += LuaTextArea_PointerExited;
        }

        RefreshLuaFoldings();
        UpdateLuaMarkerVisuals();
        LuaEditorSectionGrid.HierarchicalModel = _luaEditorSectionModel;
        RebuildLuaEditorSectionTree();
        UpdateLuaSectionPaneState();
        UpdateLuaEditorStatus();
        UpdateLuaEditorCommandStates();
        _luaEditorInitialized = true;
    }

    private void CleanupLuaEditor()
    {
        _luaFoldRefreshTimer?.Stop();
        if (_luaFoldRefreshTimer is not null)
        {
            _luaFoldRefreshTimer = null;
        }

        if (_luaEditorInitialized)
        {
            LuaSourceEditor.Document.TextChanged -= LuaDocument_TextChanged;
            LuaSourceEditor.TextArea.Caret.PositionChanged -= LuaCaret_PositionChanged;
            LuaSourceEditor.TextArea.KeyDown -= LuaTextArea_KeyDown;
            LuaSourceEditor.TextArea.KeyUp -= LuaTextArea_KeyUp;
            LuaSourceEditor.TextArea.PointerPressed -= LuaTextArea_PointerPressed;
            LuaSourceEditor.TextArea.PointerReleased -= LuaTextArea_PointerReleased;
            if (LuaHoverTooltipsEnabled)
            {
                LuaSourceEditor.TextArea.PointerMoved -= LuaTextArea_PointerMoved;
                LuaSourceEditor.TextArea.PointerExited -= LuaTextArea_PointerExited;
            }
        }

        if (_luaBreakpointMargin is not null)
        {
            _luaBreakpointMargin.MarginClicked -= LuaBreakpointMargin_Clicked;
        }

        if (_luaFoldingManager is not null)
        {
            FoldingManager.Uninstall(_luaFoldingManager);
            _luaFoldingManager = null;
        }

        _luaTextMateInstallation?.Dispose();
        _luaTextMateInstallation = null;

        HideLuaHoverTooltip();
    }

    private void SetupLuaTextMate()
    {
        try
        {
            var registryOptions = new RegistryOptions(ThemeName.DarkPlus);
            _luaTextMateInstallation = LuaSourceEditor.InstallTextMate(registryOptions);

            if (TryResolveLuaGrammarFilePath(out string grammarPath))
            {
                _luaTextMateInstallation.SetGrammarFile(grammarPath);
                return;
            }

            try
            {
                _luaTextMateInstallation.SetGrammar(registryOptions.GetScopeByLanguageId("lua"));
            }
            catch
            {
                _luaTextMateInstallation.SetGrammar(registryOptions.GetScopeByLanguageId("sql"));
            }
        }
        catch
        {
        }
    }

    private static bool TryResolveLuaGrammarFilePath(out string grammarPath)
    {
        grammarPath = string.Empty;
        var assembly = typeof(MainWindow).Assembly;
        string? resourceName = assembly.GetManifestResourceNames()
            .FirstOrDefault(name => name.EndsWith(LuaGrammarResourceSuffix, StringComparison.OrdinalIgnoreCase));
        if (string.IsNullOrWhiteSpace(resourceName))
        {
            return false;
        }

        using Stream? grammarStream = assembly.GetManifestResourceStream(resourceName);
        if (grammarStream is null)
        {
            return false;
        }

        string cacheDirectory = Path.Combine(Path.GetTempPath(), "myDUWorker", "Grammars");
        Directory.CreateDirectory(cacheDirectory);
        grammarPath = Path.Combine(cacheDirectory, LuaGrammarCacheFileName);

        using Stream fileStream = File.Create(grammarPath);
        grammarStream.CopyTo(fileStream);
        return true;
    }

    private void LuaDocument_TextChanged(object? sender, EventArgs e)
    {
        _luaFoldRefreshTimer?.Stop();
        _luaFoldRefreshTimer?.Start();
        SyncCurrentStructuredSectionFromEditor();
        UpdateLuaEditorStatus();
        UpdateLuaEditorCommandStates();
        UpdateLuaStructuredSummaryText();
    }

    private void LuaCaret_PositionChanged(object? sender, EventArgs e)
    {
        UpdateLuaEditorStatus();
    }

    private void LuaTextArea_KeyDown(object? sender, KeyEventArgs e)
    {
        _ = sender;
        _ = e;
    }

    private void LuaTextArea_KeyUp(object? sender, KeyEventArgs e)
    {
        _ = sender;
        if (e.Key == Key.Insert)
        {
            UpdateLuaEditorStatus();
        }
    }

    private void EnableLuaOverstrikeToggle()
    {
        object? options = LuaSourceEditor.TextArea?
            .GetType()
            .GetProperty("Options", BindingFlags.Instance | BindingFlags.Public)?
            .GetValue(LuaSourceEditor.TextArea);
        if (options is not null)
        {
            TryWriteBoolProperty(options, "AllowToggleOverstrikeMode", true);
        }
    }

    private void RefreshLuaFoldings()
    {
        if (_luaFoldingManager is null || LuaSourceEditor.Document is null)
        {
            return;
        }

        IReadOnlyList<NewFolding> foldings = LuaCodeFoldingBuilder.BuildRegions(LuaSourceEditor.Document)
            .Select(region => new NewFolding(region.StartOffset, region.EndOffset)
            {
                Name = region.Title
            })
            .ToList();
        _luaFoldingManager.UpdateFoldings(foldings, firstErrorOffset: -1);
    }

    private void LuaBreakpointMargin_Clicked(object? sender, int line)
    {
        if (_luaBreakpoints.Contains(line))
        {
            _luaBreakpoints.Remove(line);
        }
        else
        {
            _luaBreakpoints.Add(line);
        }

        if (line > 0 && line <= LuaSourceEditor.Document.LineCount)
        {
            DocumentLine targetLine = LuaSourceEditor.Document.GetLineByNumber(line);
            LuaSourceEditor.CaretOffset = targetLine.Offset;
            LuaSourceEditor.TextArea.Caret.BringCaretToView();
        }

        UpdateLuaMarkerVisuals();
        UpdateLuaEditorStatus();
    }

    private void UpdateLuaMarkerVisuals()
    {
        if (_luaBreakpointMargin is not null)
        {
            _luaBreakpointMargin.Breakpoints = _luaBreakpoints.OrderBy(value => value).ToArray();
            _luaBreakpointMargin.ExecutionLine = _luaExecutionLine;
            _luaBreakpointMargin.ExecutionLineIsCurrentFile = _luaExecutionLineIsCurrentFile;
            _luaBreakpointMargin.InvalidateVisual();
        }

        if (_luaExecutionHighlighter is not null)
        {
            _luaExecutionHighlighter.ExecutionLine = _luaExecutionLine;
            _luaExecutionHighlighter.IsCurrentFile = _luaExecutionLineIsCurrentFile;
        }

        LuaSourceEditor.TextArea.TextView.InvalidateLayer(KnownLayer.Background);
    }

    private void LuaTextArea_PointerPressed(object? sender, PointerPressedEventArgs e)
    {
        _luaPendingDoubleClickSelection = e.ClickCount == 2;
    }

    private void LuaTextArea_PointerReleased(object? sender, PointerReleasedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (!_luaPendingDoubleClickSelection)
        {
            return;
        }

        _luaPendingDoubleClickSelection = false;
        ExtendLuaSelectionWithPrefix();
    }

    private void LuaTextArea_PointerMoved(object? sender, PointerEventArgs e)
    {
        _ = sender;
        if (!LuaHoverTooltipsEnabled)
        {
            HideLuaHoverTooltip();
            return;
        }

        if (!TryShowLuaHoverTooltip(e))
        {
            HideLuaHoverTooltip();
        }
    }

    private void LuaTextArea_PointerExited(object? sender, PointerEventArgs e)
    {
        _ = sender;
        _ = e;
        if (!LuaHoverTooltipsEnabled)
        {
            return;
        }

        HideLuaHoverTooltip();
    }

    private bool TryShowLuaHoverTooltip(PointerEventArgs e)
    {
        if (!LuaHoverTooltipsEnabled)
        {
            return false;
        }

        if (!TryGetLuaIdentifierAtPointer(e, out string identifier))
        {
            return false;
        }

        _luaHoverToolTip ??= new ToolTip();
        _luaHoverToolTip.Content = identifier;
        ToolTip.SetTip(LuaSourceEditor, _luaHoverToolTip);
        ToolTip.SetIsOpen(LuaSourceEditor, true);
        return true;
    }

    private void HideLuaHoverTooltip()
    {
        ToolTip.SetIsOpen(LuaSourceEditor, false);
    }

    private bool TryGetLuaIdentifierAtPointer(PointerEventArgs e, out string identifier)
    {
        identifier = string.Empty;
        if (!TryGetLuaEditorOffsetFromPointer(e, out int offset))
        {
            return false;
        }

        return TryGetLuaIdentifierAtOffset(offset, out identifier);
    }

    private bool TryGetLuaEditorOffsetFromPointer(PointerEventArgs e, out int offset)
    {
        offset = -1;
        TextDocument document = LuaSourceEditor.Document;
        TextView? textView = LuaSourceEditor.TextArea?.TextView;
        if (textView is null || document.TextLength <= 0)
        {
            return false;
        }

        Point pointerPoint = e.GetPosition(textView);
        var position = textView.GetPosition(pointerPoint + textView.ScrollOffset);
        if (!position.HasValue)
        {
            return false;
        }

        int lineNumber = position.Value.Line;
        int column = position.Value.Column;
        if (lineNumber <= 0 || lineNumber > document.LineCount)
        {
            return false;
        }

        DocumentLine line = document.GetLineByNumber(lineNumber);
        int columnOffset = Math.Max(0, column - 1);
        offset = Math.Clamp(line.Offset + columnOffset, line.Offset, Math.Max(line.Offset, line.EndOffset - 1));
        return true;
    }

    private bool TryGetLuaIdentifierAtOffset(int offset, out string identifier)
    {
        identifier = string.Empty;
        string text = LuaSourceEditor.Text ?? string.Empty;
        if (text.Length == 0 || offset < 0 || offset >= text.Length)
        {
            return false;
        }

        if (!IsLuaIdentifierCharacter(text[offset]))
        {
            return false;
        }

        int start = offset;
        while (start > 0 && IsLuaIdentifierCharacter(text[start - 1]))
        {
            start--;
        }

        int end = offset;
        while (end + 1 < text.Length && IsLuaIdentifierCharacter(text[end + 1]))
        {
            end++;
        }

        identifier = text.Substring(start, end - start + 1);
        return identifier.Length > 0;
    }

    private static bool IsLuaIdentifierCharacter(char value)
    {
        return char.IsLetterOrDigit(value) || value == '_' || value == '.' || value == ':';
    }

    private void ExtendLuaSelectionWithPrefix()
    {
        int selectionStart = LuaSourceEditor.SelectionStart;
        int selectionLength = LuaSourceEditor.SelectionLength;
        if (selectionLength <= 0)
        {
            int caret = Math.Clamp(LuaSourceEditor.CaretOffset, 0, Math.Max(0, LuaSourceEditor.Document.TextLength - 1));
            string text = LuaSourceEditor.Text ?? string.Empty;
            if (text.Length == 0 || !IsLuaIdentifierCharacter(text[caret]))
            {
                return;
            }

            int start = caret;
            while (start > 0 && IsLuaIdentifierCharacter(text[start - 1]))
            {
                start--;
            }

            int end = caret;
            while (end + 1 < text.Length && IsLuaIdentifierCharacter(text[end + 1]))
            {
                end++;
            }

            LuaSourceEditor.Select(start, end - start + 1);
            return;
        }

        string selected = LuaSourceEditor.SelectedText ?? string.Empty;
        if (selected.Length == 0)
        {
            return;
        }

        string textContent = LuaSourceEditor.Text ?? string.Empty;
        int left = selectionStart;
        while (left > 0 && IsLuaIdentifierCharacter(textContent[left - 1]))
        {
            left--;
        }

        int right = selectionStart + selectionLength;
        while (right < textContent.Length && IsLuaIdentifierCharacter(textContent[right]))
        {
            right++;
        }

        LuaSourceEditor.Select(left, right - left);
    }

    private static HierarchicalModel<LuaEditorSectionTreeRow> CreateLuaEditorSectionModel()
    {
        var options = new HierarchicalOptions<LuaEditorSectionTreeRow>
        {
            ItemsSelector = row => row.Children,
            IsLeafSelector = row => row.Children.Count == 0,
            AutoExpandRoot = true,
            MaxAutoExpandDepth = 2,
            VirtualizeChildren = true
        };

        return new HierarchicalModel<LuaEditorSectionTreeRow>(options);
    }

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
            string componentKey = NormalizeLuaComponentKey(component);
            if (!groupedRows.TryGetValue(componentKey, out LuaEditorSectionTreeRow? componentNode))
            {
                componentNode = new LuaEditorSectionTreeRow(component);
                groupedRows[componentKey] = componentNode;
            }

            var sectionNode = new LuaEditorSectionTreeRow(section, eventLabel);
            componentNode.Children.Add(sectionNode);
            _luaEditorSectionNodeBySection[section] = sectionNode;
        }

        IReadOnlyList<string> orderedKeys = OrderLuaComponentKeys(groupedRows.Keys);
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

    private static string NormalizeLuaComponentKey(string component)
    {
        string normalized = (component ?? string.Empty).Trim().ToLowerInvariant();
        normalized = Regex.Replace(normalized, "\\s+", " ");
        return string.IsNullOrWhiteSpace(normalized) ? "misc" : normalized;
    }

    private static IReadOnlyList<string> OrderLuaComponentKeys(IEnumerable<string> componentKeys)
    {
        return componentKeys
            .Distinct(StringComparer.Ordinal)
            .OrderBy(GetLuaComponentSortRank)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
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
            _ => GetLuaSlotSortRank(key)
        };
    }

    private static int GetLuaSlotSortRank(string key)
    {
        if (!key.StartsWith("slot", StringComparison.OrdinalIgnoreCase))
        {
            return 1000;
        }

        if (!int.TryParse(key.AsSpan(4), NumberStyles.Integer, CultureInfo.InvariantCulture, out int slotNumber) ||
            slotNumber <= 0)
        {
            return 1000;
        }

        return 100 + slotNumber;
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

    private void DisableStructuredLuaEditor()
    {
        _luaStructuredModeActive = false;
        _luaEditorStructuredTitleBaseline = Array.Empty<string>();
        _luaSelectedStructuredSectionIndex = -1;
        SelectLuaEditorSectionByIndex(-1);
        ResetLuaEditorSectionTreeNodes();
        _luaEditorSections.Clear();
        RebuildLuaEditorSectionTree();
        UpdateLuaSectionPaneState();
    }

    private bool TryEnableStructuredLuaEditor(string combinedLua, string? preferredSectionLabel)
    {
        List<ParsedLuaSection> parsed = ParseStructuredLuaSections(combinedLua);
        if (parsed.Count == 0)
        {
            DisableStructuredLuaEditor();
            return false;
        }

        _luaEditorSections.Clear();
        foreach (ParsedLuaSection section in parsed)
        {
            _luaEditorSections.Add(new LuaEditorSectionState(
                section.Index <= 0 ? _luaEditorSections.Count + 1 : section.Index,
                string.IsNullOrWhiteSpace(section.Title) ? $"part_{_luaEditorSections.Count + 1:000}" : section.Title,
                section.Body));
        }

        _luaStructuredModeActive = true;
        _luaEditorStructuredTitleBaseline = _luaEditorSections.Select(section => section.Title).ToArray();
        RebuildLuaEditorSectionTree();
        UpdateLuaSectionPaneState();

        int selectedIndex = 0;
        if (!string.IsNullOrWhiteSpace(preferredSectionLabel))
        {
            int preferred = _luaEditorSections
                .Select((section, index) => (section, index))
                .Where(tuple => string.Equals(tuple.section.Title, preferredSectionLabel, StringComparison.Ordinal))
                .Select(tuple => tuple.index)
                .DefaultIfEmpty(-1)
                .First();
            if (preferred < 0 &&
                preferredSectionLabel.StartsWith("part_", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(preferredSectionLabel.AsSpan(5), NumberStyles.Integer, CultureInfo.InvariantCulture, out int ordinal))
            {
                preferred = ordinal - 1;
            }
            if (preferred >= 0 && preferred < _luaEditorSections.Count)
            {
                selectedIndex = preferred;
            }
        }

        SelectLuaEditorSectionByIndex(selectedIndex);
        _luaSelectedStructuredSectionIndex = selectedIndex;
        _luaSuppressSectionTextSync = true;
        SetLuaEditorText(_luaEditorSections[selectedIndex].CurrentContent);
        _luaSuppressSectionTextSync = false;
        return true;
    }

    private void UpdateLuaSectionPaneState()
    {
        bool paneVisible = _luaStructuredModeActive && _luaEditorSections.Count > 1;
        LuaEditorSectionPane.IsVisible = paneVisible;
        UpdateLuaStructuredSummaryText();
    }

    private void UpdateLuaStructuredSummaryText()
    {
        if (!_luaStructuredModeActive)
        {
            LuaEditorSectionSummaryText.Text = "Single text mode";
            return;
        }

        if (ValidateStructuredLuaSession(out string error))
        {
            LuaEditorSectionSummaryText.Text =
                $"Structured mode: {_luaEditorSections.Count.ToString(CultureInfo.InvariantCulture)} sections | preflight ok";
        }
        else
        {
            LuaEditorSectionSummaryText.Text =
                $"Structured mode: {_luaEditorSections.Count.ToString(CultureInfo.InvariantCulture)} sections | blocked: {error}";
        }
    }

    private void SyncCurrentStructuredSectionFromEditor()
    {
        if (!_luaStructuredModeActive ||
            _luaSuppressSectionTextSync ||
            _luaSelectedStructuredSectionIndex < 0 ||
            _luaSelectedStructuredSectionIndex >= _luaEditorSections.Count)
        {
            return;
        }

        string current = LuaSourceEditor.Text ?? string.Empty;
        _luaEditorSections[_luaSelectedStructuredSectionIndex].CurrentContent = current;
    }

    private int GetLuaEditorTopVisibleLine()
    {
        TextView? textView = LuaSourceEditor.TextArea?.TextView;
        TextDocument? document = LuaSourceEditor.Document;
        if (textView is null || document is null || document.LineCount <= 0)
        {
            return 1;
        }

        textView.EnsureVisualLines();
        VisualLine? visualTop = textView.GetVisualLineFromVisualTop(0);
        if (visualTop?.FirstDocumentLine is DocumentLine line)
        {
            return Math.Clamp(line.LineNumber, 1, document.LineCount);
        }

        if (textView.VisualLines.Count > 0)
        {
            return Math.Clamp(textView.VisualLines[0].FirstDocumentLine.LineNumber, 1, document.LineCount);
        }

        return 1;
    }

    private void CaptureCurrentStructuredSectionViewport()
    {
        if (!_luaStructuredModeActive ||
            _luaSelectedStructuredSectionIndex < 0 ||
            _luaSelectedStructuredSectionIndex >= _luaEditorSections.Count)
        {
            return;
        }

        _luaEditorSections[_luaSelectedStructuredSectionIndex].LastTopVisibleLine = GetLuaEditorTopVisibleLine();
    }

    private void RestoreStructuredSectionViewport(LuaEditorSectionState section)
    {
        if (LuaSourceEditor.Document is null || LuaSourceEditor.Document.LineCount <= 0)
        {
            return;
        }

        int desiredTop = Math.Clamp(section.LastTopVisibleLine, 1, LuaSourceEditor.Document.LineCount);
        int requestLine = desiredTop;
        int lineCount = LuaSourceEditor.Document.LineCount;

        for (int attempt = 0; attempt < 3; attempt++)
        {
            LuaSourceEditor.ScrollTo(requestLine, 1);
            LuaSourceEditor.TextArea?.TextView?.EnsureVisualLines();

            int currentTop = GetLuaEditorTopVisibleLine();
            if (currentTop == desiredTop)
            {
                return;
            }

            int correction = desiredTop - currentTop;
            if (correction == 0)
            {
                return;
            }

            requestLine = Math.Clamp(requestLine + correction, 1, lineCount);
        }
    }

    private void RestoreStructuredSectionViewportDeferred(LuaEditorSectionState section)
    {
        Dispatcher.UIThread.Post(() =>
        {
            if (!_luaStructuredModeActive ||
                _luaSelectedStructuredSectionIndex < 0 ||
                _luaSelectedStructuredSectionIndex >= _luaEditorSections.Count ||
                !ReferenceEquals(_luaEditorSections[_luaSelectedStructuredSectionIndex], section))
            {
                return;
            }

            RestoreStructuredSectionViewport(section);
        }, DispatcherPriority.Background);
    }

    private bool ValidateStructuredLuaSession(out string error)
    {
        error = string.Empty;
        if (!_luaStructuredModeActive)
        {
            return true;
        }

        SyncCurrentStructuredSectionFromEditor();
        if (_luaEditorSections.Count == 0)
        {
            error = "Structured session has no sections.";
            return false;
        }

        if (_luaEditorStructuredTitleBaseline.Count != _luaEditorSections.Count)
        {
            error = "Section structure changed unexpectedly. Reload LUA blocks.";
            return false;
        }

        for (int i = 0; i < _luaEditorSections.Count; i++)
        {
            if (!string.Equals(_luaEditorStructuredTitleBaseline[i], _luaEditorSections[i].Title, StringComparison.Ordinal))
            {
                error = "Section order/title changed unexpectedly. Reload LUA blocks.";
                return false;
            }
        }

        return true;
    }

    private string BuildStructuredCombinedLuaText()
    {
        SyncCurrentStructuredSectionFromEditor();
        var sb = new StringBuilder();
        for (int i = 0; i < _luaEditorSections.Count; i++)
        {
            LuaEditorSectionState section = _luaEditorSections[i];
            sb.Append("-- ===== ");
            sb.Append(section.Index.ToString("000", CultureInfo.InvariantCulture));
            sb.Append(' ');
            sb.Append(section.Title);
            sb.AppendLine(" =====");
            sb.AppendLine((section.CurrentContent ?? string.Empty).TrimEnd());
            sb.AppendLine();
        }

        return sb.ToString().TrimEnd();
    }

    private string GetLuaEditorPersistedText()
    {
        return _luaStructuredModeActive
            ? BuildStructuredCombinedLuaText()
            : (LuaSourceEditor.Text ?? string.Empty);
    }

    private bool HasLuaEditorUnsavedChanges()
    {
        if (!LuaEditorPageRoot.IsVisible || !_luaEditorInitialized)
        {
            return false;
        }

        string currentContent = GetLuaEditorPersistedText();
        return !string.Equals(currentContent, _luaEditorLastPersistedContent, StringComparison.Ordinal);
    }

    private void MarkLuaEditorCleanFromCurrentContent()
    {
        _luaEditorLastPersistedContent = GetLuaEditorPersistedText();
    }

    private sealed record LuaPersistenceIssueSummary(int NullCount, int UnpairedSurrogateCount)
    {
        public int TotalInvalidCount => NullCount + UnpairedSurrogateCount;
        public bool HasIssues => TotalInvalidCount > 0;
    }

    private static LuaPersistenceIssueSummary AnalyzeLuaPersistenceIssues(string content)
    {
        string text = content ?? string.Empty;
        int nullCount = 0;
        int unpairedSurrogateCount = 0;

        for (int i = 0; i < text.Length; i++)
        {
            char ch = text[i];
            if (ch == '\0')
            {
                nullCount++;
                continue;
            }

            if (!char.IsSurrogate(ch))
            {
                continue;
            }

            if (char.IsHighSurrogate(ch) &&
                i + 1 < text.Length &&
                char.IsLowSurrogate(text[i + 1]))
            {
                i++;
                continue;
            }

            unpairedSurrogateCount++;
        }

        return new LuaPersistenceIssueSummary(nullCount, unpairedSurrogateCount);
    }

    private static string SanitizeLuaPersistenceIssues(
        string content,
        bool replaceInvalidWithBlanks,
        out int replacedOrRemovedCount)
    {
        string text = content ?? string.Empty;
        replacedOrRemovedCount = 0;
        var sb = new StringBuilder(text.Length);

        for (int i = 0; i < text.Length; i++)
        {
            char ch = text[i];
            if (ch == '\0')
            {
                replacedOrRemovedCount++;
                if (replaceInvalidWithBlanks)
                {
                    sb.Append(' ');
                }

                continue;
            }

            if (!char.IsSurrogate(ch))
            {
                sb.Append(ch);
                continue;
            }

            if (char.IsHighSurrogate(ch) &&
                i + 1 < text.Length &&
                char.IsLowSurrogate(text[i + 1]))
            {
                sb.Append(ch);
                sb.Append(text[i + 1]);
                i++;
                continue;
            }

            replacedOrRemovedCount++;
            if (replaceInvalidWithBlanks)
            {
                sb.Append(' ');
            }
        }

        return sb.ToString();
    }

    private static bool TryValidateLuaContentForPersistence(string content, out string error)
    {
        error = string.Empty;
        string text = content ?? string.Empty;

        if (text.IndexOf('\0') >= 0)
        {
            error = "Save blocked: content contains NUL (\\0) characters.";
            return false;
        }

        try
        {
            // Strict UTF-8 validation catches malformed surrogate pairs before disk/DB writes.
            var strictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
            _ = strictUtf8.GetByteCount(text);
            return true;
        }
        catch (EncoderFallbackException)
        {
            error = "Save blocked: content contains invalid Unicode sequences that cannot be encoded as UTF-8.";
            return false;
        }
    }

    private void ApplyPersistableLuaContentToEditor(string content)
    {
        string persisted = content ?? string.Empty;
        if (_luaStructuredModeActive)
        {
            string? preferredSectionTitle =
                _luaSelectedStructuredSectionIndex >= 0 && _luaSelectedStructuredSectionIndex < _luaEditorSections.Count
                    ? _luaEditorSections[_luaSelectedStructuredSectionIndex].Title
                    : null;
            if (!TryEnableStructuredLuaEditor(persisted, preferredSectionTitle))
            {
                DisableStructuredLuaEditor();
                SetLuaEditorText(persisted);
            }
        }
        else
        {
            SetLuaEditorText(persisted);
        }

        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
    }

    private async Task<(bool Proceed, string Content, string StatusMessage)> ResolveLuaContentForPersistenceAsync(
        string content,
        string actionDescription)
    {
        string original = content ?? string.Empty;
        LuaPersistenceIssueSummary issues = AnalyzeLuaPersistenceIssues(original);
        if (!issues.HasIssues)
        {
            if (!TryValidateLuaContentForPersistence(original, out string validationError))
            {
                return (false, original, validationError);
            }

            return (true, original, string.Empty);
        }

        var dialog = new LuaPersistenceCleanupDialog(
            actionDescription,
            issues.NullCount,
            issues.UnpairedSurrogateCount);
        LuaPersistenceCleanupAction action = await dialog.ShowDialog<LuaPersistenceCleanupAction>(this);
        if (action == LuaPersistenceCleanupAction.Cancel)
        {
            return (false, original, "Save cancelled.");
        }

        bool replaceWithBlanks = action == LuaPersistenceCleanupAction.ReplaceInvalidCharactersWithBlanks;
        string cleaned = SanitizeLuaPersistenceIssues(original, replaceWithBlanks, out int changedCount);
        if (!TryValidateLuaContentForPersistence(cleaned, out string cleanedValidationError))
        {
            return (false, original, cleanedValidationError);
        }

        string status = replaceWithBlanks
            ? $"Replaced {changedCount} invalid character(s) with blanks."
            : $"Removed {changedCount} invalid character(s).";
        return (true, cleaned, status);
    }

    private async Task<bool> ConfirmDiscardLuaEditorChangesAsync(string actionDescription)
    {
        if (!HasLuaEditorUnsavedChanges())
        {
            return true;
        }

        var dialog = new UnsavedChangesDialog(actionDescription);
        bool discard = await dialog.ShowDialog<bool>(this);
        return discard;
    }

    private void OnLuaEditorSectionSelectionChanged(object? sender, SelectionChangedEventArgs e)
    {
        _ = sender;
        _ = e;
        if (_luaSuppressSectionSelectionEvent || !_luaStructuredModeActive)
        {
            return;
        }

        LuaEditorSectionState? selected = ResolveSelectedLuaEditorSection();
        if (selected is null)
        {
            return;
        }

        int selectedIndex = _luaEditorSections.IndexOf(selected);
        if (selectedIndex < 0 || selectedIndex >= _luaEditorSections.Count)
        {
            return;
        }

        CaptureCurrentStructuredSectionViewport();
        SyncCurrentStructuredSectionFromEditor();
        _luaSelectedStructuredSectionIndex = selectedIndex;
        _luaSuppressSectionTextSync = true;
        SetLuaEditorText(selected.CurrentContent);
        _luaSuppressSectionTextSync = false;
        RestoreStructuredSectionViewportDeferred(selected);
        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
        UpdateLuaStructuredSummaryText();
    }

    private void SetLuaEditorPageVisible(bool isVisible)
    {
        MainWorkbenchRoot.IsVisible = !isVisible;
        LuaEditorPageRoot.IsVisible = isVisible;
        if (isVisible)
        {
            LuaSourceEditor.Focus();
            LuaSourceEditor.TextArea.Caret.BringCaretToView();
        }
    }

    private void UpdateLuaEditorHeader()
    {
        string currentLabel = !string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath)
            ? Path.GetFileName(_luaEditorCurrentFilePath)
            : _luaEditorSuggestedFileName;
        string contextLabel = ResolveLuaEditorContextLabel();
        if (_luaStructuredModeActive &&
            _luaSelectedStructuredSectionIndex >= 0 &&
            _luaSelectedStructuredSectionIndex < _luaEditorSections.Count)
        {
            LuaEditorSectionState section = _luaEditorSections[_luaSelectedStructuredSectionIndex];
            LuaEditorHeaderText.Text =
                $"{contextLabel} - {currentLabel} | section {_luaSelectedStructuredSectionIndex + 1}/{_luaEditorSections.Count}: {section.Title}";
            return;
        }

        LuaEditorHeaderText.Text = $"{contextLabel} - {currentLabel}";
    }

    private string ResolveLuaEditorContextLabel()
    {
        if (DataContext is MainWindowViewModel vm)
        {
            if (!string.IsNullOrWhiteSpace(vm.ActiveConstructName))
            {
                return vm.ActiveConstructName;
            }

            if (vm.SelectedConstructNameSuggestion is not null &&
                !string.IsNullOrWhiteSpace(vm.SelectedConstructNameSuggestion.ConstructName))
            {
                return vm.SelectedConstructNameSuggestion.ConstructName;
            }
        }

        return "LUA Editor";
    }

    private void UpdateLuaEditorStatus()
    {
        int line = Math.Max(1, LuaSourceEditor.TextArea.Caret.Line);
        int column = Math.Max(1, LuaSourceEditor.TextArea.Caret.Column);
        string sourceLabel = !string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath)
            ? Path.GetFileName(_luaEditorCurrentFilePath)
            : _luaEditorSuggestedFileName;
        string modeLabel = _luaEditorSections.Count > 1 ? "structured" : (_luaStructuredModeActive ? "single-section" : "plain");
        string sectionLabel = "-";
        if (_luaStructuredModeActive &&
            _luaSelectedStructuredSectionIndex >= 0 &&
            _luaSelectedStructuredSectionIndex < _luaEditorSections.Count)
        {
            sectionLabel = _luaEditorSections[_luaSelectedStructuredSectionIndex].DisplayLabel;
        }

        string insertModeLabel = GetLuaInsertOverwriteLabel();
        string eolLabel = DetectLineEndingLabel(LuaSourceEditor.Text ?? string.Empty);
        string encodingLabel = string.IsNullOrWhiteSpace(_luaEditorEncodingLabel) ? "UTF-8" : _luaEditorEncodingLabel;
        string selectedText = LuaSourceEditor.SelectedText ?? string.Empty;
        int selectedBytes = Encoding.UTF8.GetByteCount(selectedText);

        LuaEditorStatusSourceText.Text = sourceLabel;
        LuaEditorStatusModeText.Text = modeLabel;
        LuaEditorStatusSectionText.Text = sectionLabel;
        LuaEditorStatusCaretText.Text = $"Ln {line}, Col {column}";
        LuaEditorStatusSelectionBytesText.Text = $"Sel {selectedBytes.ToString(CultureInfo.InvariantCulture)} B";
        LuaEditorStatusInsertModeText.Text = insertModeLabel;
        LuaEditorStatusEolText.Text = eolLabel;
        LuaEditorStatusEncodingText.Text = encodingLabel;

        if (_luaStructuredModeActive &&
            _luaSelectedStructuredSectionIndex >= 0 &&
            _luaSelectedStructuredSectionIndex < _luaEditorSections.Count)
        {
            return;
        }
    }

    private static string DetectLineEndingLabel(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return "LF";
        }

        bool hasCrLf = text.Contains("\r\n", StringComparison.Ordinal);
        bool hasAnyLf = text.Contains('\n');
        if (!hasAnyLf)
        {
            return "LF";
        }

        if (!hasCrLf)
        {
            return "LF";
        }

        bool hasLoneLf = text.Replace("\r\n", string.Empty, StringComparison.Ordinal).Contains('\n');
        return hasLoneLf ? "Mixed EOL" : "CRLF";
    }

    private string GetLuaInsertOverwriteLabel()
    {
        bool? overstrike = TryReadLuaCaretBoolProperty("OverstrikeMode") ??
                           TryReadLuaTextAreaBoolProperty("OverstrikeMode");
        if (overstrike is null)
        {
            return "INS";
        }

        return overstrike.Value ? "OVR" : "INS";
    }

    private bool? TryReadLuaTextAreaBoolProperty(string propertyName)
    {
        object? textArea = LuaSourceEditor.TextArea;
        return TryReadBoolProperty(textArea, propertyName);
    }

    private bool? TryReadLuaCaretBoolProperty(string propertyName)
    {
        object? caret = LuaSourceEditor.TextArea?.Caret;
        return TryReadBoolProperty(caret, propertyName);
    }

    private bool TryWriteLuaTextAreaBoolProperty(string propertyName, bool value)
    {
        object? textArea = LuaSourceEditor.TextArea;
        return TryWriteBoolProperty(textArea, propertyName, value);
    }

    private bool TryWriteLuaCaretBoolProperty(string propertyName, bool value)
    {
        object? caret = LuaSourceEditor.TextArea?.Caret;
        return TryWriteBoolProperty(caret, propertyName, value);
    }

    private static bool? TryReadBoolProperty(object? target, string propertyName)
    {
        if (target is null || string.IsNullOrWhiteSpace(propertyName))
        {
            return null;
        }

        PropertyInfo? property = target
            .GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        if (property is null || property.PropertyType != typeof(bool))
        {
            return null;
        }

        object? value = property.GetValue(target);
        return value is bool boolValue ? boolValue : null;
    }

    private static bool TryWriteBoolProperty(object? target, string propertyName, bool value)
    {
        if (target is null || string.IsNullOrWhiteSpace(propertyName))
        {
            return false;
        }

        PropertyInfo? property = target
            .GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        if (property is null || property.PropertyType != typeof(bool) || !property.CanWrite)
        {
            return false;
        }

        property.SetValue(target, value);
        return true;
    }

    private void UpdateLuaEditorCommandStates()
    {
        LuaEditorSaveButton.IsEnabled = !string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath);
        bool isDbAvailable = DataContext is MainWindowViewModel vm &&
                             string.Equals(vm.DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase);
        bool structuredValid = ValidateStructuredLuaSession(out _);
        LuaEditorSaveToDbButton.IsEnabled = isDbAvailable &&
                                            structuredValid &&
                                            _luaEditorOriginalDbRecord is not null &&
                                            _luaEditorSourceContext?.ElementId.HasValue == true &&
                                            string.Equals(_luaEditorSourceContext.PropertyName, "dpuyaml_6", StringComparison.OrdinalIgnoreCase);
        UpdateLuaStructuredSummaryText();
    }

    private static bool TryBuildDbOptions(MainWindowViewModel vm, out DataConnectionOptions options, out string error)
    {
        options = default!;
        error = string.Empty;

        if (!int.TryParse(vm.DbPortInput, NumberStyles.Integer, CultureInfo.InvariantCulture, out int port) || port <= 0)
        {
            error = "DB port is invalid.";
            return false;
        }

        options = new DataConnectionOptions(
            vm.ServerRootPathInput ?? string.Empty,
            vm.DbHostInput ?? string.Empty,
            port,
            vm.DbNameInput ?? string.Empty,
            vm.DbUserInput ?? string.Empty,
            vm.DbPasswordInput ?? string.Empty);
        return true;
    }

    private async Task CreateLuaVersionBackupAsync(string content, string sourceFilePath)
    {
        if (DataContext is not MainWindowViewModel vm || !vm.LuaVersioningEnabled)
        {
            return;
        }

        string fallbackSuggestedName = string.IsNullOrWhiteSpace(_luaEditorSuggestedFileName)
            ? "script.lua"
            : _luaEditorSuggestedFileName;
        string fallbackPropertyName = _luaEditorSourceContext?.PropertyName ?? "dpuyaml_6";
        string fallbackNodeLabel = _luaEditorSourceContext?.NodeLabel ?? "lua";

        var request = new LuaBackupCreateRequest(
            content,
            fallbackSuggestedName,
            sourceFilePath,
            _luaEditorSourceContext?.ElementId,
            _luaEditorSourceContext?.ElementDisplayName ?? string.Empty,
            fallbackNodeLabel,
            fallbackPropertyName);
        await _luaBackupService.CreateBackupAsync(request, default);
    }

    private void SetLuaEditorText(string text)
    {
        EnsureLuaEditorInitialized();
        LuaSourceEditor.Text = text ?? string.Empty;
        LuaSourceEditor.CaretOffset = 0;
        RefreshLuaFoldings();
        UpdateLuaEditorStatus();
        UpdateLuaEditorCommandStates();
    }

    private async Task<bool> SaveLuaEditorToPathAsync(string path)
    {
        string content = GetLuaEditorPersistedText();
        (bool proceed, string persistableContent, string resolveStatus) =
            await ResolveLuaContentForPersistenceAsync(content, "save to file");
        if (!proceed)
        {
            if (DataContext is MainWindowViewModel vmCancelled)
            {
                vmCancelled.StatusMessage = resolveStatus;
            }

            return false;
        }

        if (!string.Equals(content, persistableContent, StringComparison.Ordinal))
        {
            ApplyPersistableLuaContentToEditor(persistableContent);
            content = persistableContent;
        }

        await File.WriteAllTextAsync(path, content, new UTF8Encoding(false));
        _luaEditorEncodingLabel = "UTF-8";
        await CreateLuaVersionBackupAsync(content, path);
        _luaEditorCurrentFilePath = path;
        _luaEditorSuggestedFileName = Path.GetFileName(path);
        if (DataContext is MainWindowViewModel vm)
        {
            UpdateLastUsedFolder(vm, path);

            vm.StatusMessage = string.IsNullOrWhiteSpace(resolveStatus)
                ? $"Saved LUA editor file to {path}."
                : $"{resolveStatus} Saved LUA editor file to {path}.";
        }

        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
        UpdateLuaEditorCommandStates();
        MarkLuaEditorCleanFromCurrentContent();
        return true;
    }

    private async Task SaveLuaEditorAsAsync()
    {
        var options = new FilePickerSaveOptions
        {
            Title = "Save LUA File",
            SuggestedFileName = _luaEditorSuggestedFileName,
            DefaultExtension = ".lua",
            FileTypeChoices = new[]
            {
                new FilePickerFileType("LUA files")
                {
                    Patterns = new[] {"*.lua"}
                },
                new FilePickerFileType("All files")
                {
                    Patterns = new[] {"*.*"}
                }
            }
        };

        if (DataContext is MainWindowViewModel vm && !string.IsNullOrWhiteSpace(vm.LastSavedFolder))
        {
            Uri? folderUri = TryBuildFolderUri(vm.LastSavedFolder);
            if (folderUri is not null)
            {
                IStorageFolder? folder = await StorageProvider.TryGetFolderFromPathAsync(folderUri);
                if (folder is not null)
                {
                    options.SuggestedStartLocation = folder;
                }
            }
        }

        IStorageFile? file = await StorageProvider.SaveFilePickerAsync(options);
        if (file?.Path is not Uri filePathUri || !filePathUri.IsFile)
        {
            return;
        }

        _ = await SaveLuaEditorToPathAsync(filePathUri.LocalPath);
    }

    private async Task OpenLuaEditorFileAsync()
    {
        var options = new FilePickerOpenOptions
        {
            Title = "Open LUA/JSON/CONF File",
            AllowMultiple = false,
            FileTypeFilter = new[]
            {
                new FilePickerFileType("LUA files")
                {
                    Patterns = new[] {"*.lua"}
                },
                new FilePickerFileType("JSON files")
                {
                    Patterns = new[] {"*.json"}
                },
                new FilePickerFileType("CONF files")
                {
                    Patterns = new[] {"*.conf"}
                },
                new FilePickerFileType("All files")
                {
                    Patterns = new[] {"*.*"}
                }
            }
        };

        if (DataContext is MainWindowViewModel vm && !string.IsNullOrWhiteSpace(vm.LastSavedFolder))
        {
            Uri? folderUri = TryBuildFolderUri(vm.LastSavedFolder);
            if (folderUri is not null)
            {
                IStorageFolder? folder = await StorageProvider.TryGetFolderFromPathAsync(folderUri);
                if (folder is not null)
                {
                    options.SuggestedStartLocation = folder;
                }
            }
        }

        IReadOnlyList<IStorageFile> files = await StorageProvider.OpenFilePickerAsync(options);
        if (files.Count == 0)
        {
            return;
        }

        IStorageFile selected = files[0];
        await using Stream readStream = await selected.OpenReadAsync();
        using var reader = new StreamReader(readStream, Encoding.UTF8, true);
        string text = await reader.ReadToEndAsync();
        _luaEditorEncodingLabel = FormatEncodingLabel(reader.CurrentEncoding);

        string filePath = ResolveStorageFilePath(selected);

        _luaEditorCurrentFilePath = filePath;
        _luaEditorSuggestedFileName = Path.GetFileName(filePath);
        _luaEditorSourceContext = null;
        _luaEditorOriginalDbRecord = null;
        DisableStructuredLuaEditor();

        string extension = Path.GetExtension(filePath)?.ToLowerInvariant() ?? string.Empty;
        bool openedStructured = false;
        string openStatus = $"Opened file {filePath}.";
        if (string.Equals(extension, ".json", StringComparison.Ordinal))
        {
            if (DpuLuaDecoder.TryBuildCombinedLuaFromJsonText(
                    text,
                    out string combinedLua,
                    out int jsonSectionCount,
                    out string? jsonError))
            {
                openedStructured = TryEnableStructuredLuaEditor(combinedLua, preferredSectionLabel: null);
                if (!openedStructured)
                {
                    SetLuaEditorText(combinedLua);
                }

                openStatus = $"Opened JSON file {filePath} ({jsonSectionCount.ToString(CultureInfo.InvariantCulture)} sections).";
            }
            else
            {
                SetLuaEditorText(text);
                openStatus = $"Opened JSON file {filePath} (section parsing unavailable: {jsonError ?? "unknown error"}).";
            }
        }
        else if (string.Equals(extension, ".lua", StringComparison.Ordinal) ||
                 string.Equals(extension, ".conf", StringComparison.Ordinal))
        {
            SetLuaEditorText(text);
        }
        else if (!TryEnableStructuredLuaEditor(text, preferredSectionLabel: null))
        {
            SetLuaEditorText(text);
        }

        MarkLuaEditorCleanFromCurrentContent();
        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
        if (DataContext is MainWindowViewModel vm2)
        {
            UpdateLastUsedFolder(vm2, filePath);
            vm2.StatusMessage = openStatus;
        }
    }

    private async void OnEditLuaClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (LuaEditorPageRoot.IsVisible &&
            !await ConfirmDiscardLuaEditorChangesAsync("open another LUA block"))
        {
            return;
        }

        if (DataContext is not MainWindowViewModel vm ||
            !vm.TryGetSelectedLuaBlobSaveRequest(out MainWindowViewModel.BlobSaveRequest? request) ||
            request is null)
        {
            if (DataContext is MainWindowViewModel vmNoSelection)
            {
                vmNoSelection.StatusMessage = "Select a LUA block or part first.";
            }

            return;
        }

        _luaEditorCurrentFilePath = string.Empty;
        _luaEditorSuggestedFileName = request.SuggestedFileName;
        _luaEditorEncodingLabel = "UTF-8";
        _luaEditorSourceContext = vm.TryGetSelectedLuaEditorSourceContext(out MainWindowViewModel.LuaEditorSourceContext? sourceContext)
            ? sourceContext
            : null;
        _luaEditorOriginalDbRecord = null;
        DisableStructuredLuaEditor();

        if (_luaEditorSourceContext?.ElementId.HasValue == true &&
            !string.IsNullOrWhiteSpace(_luaEditorSourceContext.PropertyName) &&
            TryBuildDbOptions(vm, out DataConnectionOptions options, out _))
        {
            try
            {
                _luaEditorOriginalDbRecord = await _dataService.GetLuaPropertyRawAsync(
                    options,
                    _luaEditorSourceContext.ElementId.Value,
                    _luaEditorSourceContext.PropertyName,
                    default);
            }
            catch
            {
                _luaEditorOriginalDbRecord = null;
            }
        }

        bool openedStructured = false;
        if (_luaEditorOriginalDbRecord is not null &&
            string.Equals(_luaEditorSourceContext?.PropertyName, "dpuyaml_6", StringComparison.OrdinalIgnoreCase) &&
            DpuLuaEditorCodec.TryBuildCombinedLuaFromDbValue(
                _luaEditorOriginalDbRecord.RawValue,
                vm.ServerRootPathInput ?? string.Empty,
                out string combinedLua,
                out _,
                out _))
        {
            openedStructured = TryEnableStructuredLuaEditor(combinedLua, _luaEditorSourceContext?.NodeLabel);
        }

        if (!openedStructured)
        {
            openedStructured = TryEnableStructuredLuaEditor(request.Content, _luaEditorSourceContext?.NodeLabel);
        }

        if (!openedStructured)
        {
            SetLuaEditorText(request.Content);
        }

        MarkLuaEditorCleanFromCurrentContent();
        UpdateLuaEditorHeader();
        SetLuaEditorPageVisible(true);
        vm.StatusMessage = $"Opened {request.SuggestedFileName} in LUA editor.";
    }

    private async void OnLuaEditorBackClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        if (!await ConfirmDiscardLuaEditorChangesAsync("go back to the workbench"))
        {
            return;
        }

        HideLuaHoverTooltip();
        SetLuaEditorPageVisible(false);
    }

    private async void OnLuaEditorNewClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        if (!await ConfirmDiscardLuaEditorChangesAsync("create a new LUA file"))
        {
            return;
        }

        _luaEditorCurrentFilePath = string.Empty;
        _luaEditorSuggestedFileName = "script.lua";
        _luaEditorEncodingLabel = "UTF-8";
        _luaEditorSourceContext = null;
        _luaEditorOriginalDbRecord = null;
        DisableStructuredLuaEditor();
        SetLuaEditorText(string.Empty);
        MarkLuaEditorCleanFromCurrentContent();
        UpdateLuaEditorHeader();
    }

    private async void OnLuaEditorOpenClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        try
        {
            if (!await ConfirmDiscardLuaEditorChangesAsync("open another LUA file"))
            {
                return;
            }

            await OpenLuaEditorFileAsync();
        }
        catch (Exception ex)
        {
            if (DataContext is MainWindowViewModel vm)
            {
                vm.StatusMessage = $"Open failed: {ex.Message}";
            }
        }
    }

    private async void OnLuaEditorSaveClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        try
        {
            if (string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath))
            {
                if (DataContext is MainWindowViewModel vmNoPath)
                {
                    vmNoPath.StatusMessage = "Save is only available for files already on disk. Use Save as... first.";
                }

                return;
            }

            await SaveLuaEditorToPathAsync(_luaEditorCurrentFilePath);
        }
        catch (Exception ex)
        {
            if (DataContext is MainWindowViewModel vm)
            {
                vm.StatusMessage = $"Save failed: {ex.Message}";
            }
        }
    }

    private async void OnLuaEditorSaveAsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        try
        {
            await SaveLuaEditorAsAsync();
        }
        catch (Exception ex)
        {
            if (DataContext is MainWindowViewModel vm)
            {
                vm.StatusMessage = $"Save as failed: {ex.Message}";
            }
        }
    }

    private async void OnLuaEditorSaveToDbClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (_luaEditorOriginalDbRecord is null ||
            _luaEditorSourceContext?.ElementId.HasValue != true ||
            string.IsNullOrWhiteSpace(_luaEditorSourceContext.PropertyName))
        {
            vm.StatusMessage = "Save to DB requires LUA content opened from the DB.";
            return;
        }

        MainWindowViewModel.LuaEditorSourceContext sourceContext = _luaEditorSourceContext;

        if (!string.Equals(vm.DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase))
        {
            vm.StatusMessage = "DB is offline. Save to DB is unavailable.";
            return;
        }

        if (!TryBuildDbOptions(vm, out DataConnectionOptions options, out string optionsError))
        {
            vm.StatusMessage = $"Save to DB failed: {optionsError}";
            return;
        }

        try
        {
            if (!ValidateStructuredLuaSession(out string preflightError))
            {
                vm.StatusMessage = $"Save to DB blocked: {preflightError}";
                return;
            }

            ulong elementId = sourceContext.ElementId!.Value;
            string propertyName = sourceContext.PropertyName;
            string content = GetLuaEditorPersistedText();
            (bool proceed, string persistableContent, string resolveStatus) =
                await ResolveLuaContentForPersistenceAsync(content, "save to DB");
            if (!proceed)
            {
                vm.StatusMessage = string.IsNullOrWhiteSpace(resolveStatus) ? "Save to DB cancelled." : resolveStatus;
                return;
            }

            if (!string.Equals(content, persistableContent, StringComparison.Ordinal))
            {
                ApplyPersistableLuaContentToEditor(persistableContent);
                content = persistableContent;
            }

            string dbSource = $"db://element/{elementId}/{propertyName}";
            try
            {
                await CreateLuaVersionBackupAsync(content, dbSource);
            }
            catch (Exception backupEx)
            {
                vm.StatusMessage = $"Backup creation failed before DB save: {backupEx.Message}";
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            LuaDbSaveResult result = await _dataService.SaveLuaPropertyAsync(
                options,
                elementId,
                propertyName,
                content,
                _luaEditorOriginalDbRecord.RawValue,
                vm.ServerRootPathInput ?? string.Empty,
                cts.Token);

            _luaEditorOriginalDbRecord = _luaEditorOriginalDbRecord with
            {
                PropertyType = result.PropertyType,
                RawValue = result.NewDbValue
            };
            if (_luaStructuredModeActive)
            {
                string? preferredSectionTitle =
                    _luaSelectedStructuredSectionIndex >= 0 && _luaSelectedStructuredSectionIndex < _luaEditorSections.Count
                        ? _luaEditorSections[_luaSelectedStructuredSectionIndex].Title
                        : null;
                TryEnableStructuredLuaEditor(content, preferredSectionTitle);
            }
            _luaEditorEncodingLabel = "UTF-8";
            MarkLuaEditorCleanFromCurrentContent();
            UpdateLuaEditorCommandStates();
            string saveMessage = result.UsesHashReference
                ? $"Saved to DB (hash {result.HashReference}, sections={result.SectionCount})."
                : $"Saved to DB (inline payload, sections={result.SectionCount}).";
            vm.StatusMessage = string.IsNullOrWhiteSpace(resolveStatus)
                ? saveMessage
                : $"{resolveStatus} {saveMessage}";
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"Save to DB failed: {ex.Message}";
        }
    }

    private async void OnLuaEditorBackupsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        try
        {
            string currentContent = GetLuaEditorPersistedText();
            var dialog = new LuaBackupManagerDialog(_luaBackupService, currentContent);
            LuaBackupManagerDialogResult? result = await dialog.ShowDialog<LuaBackupManagerDialogResult?>(this);
            if (result is null)
            {
                return;
            }

            if (!await ConfirmDiscardLuaEditorChangesAsync("load backup content"))
            {
                return;
            }

            _luaEditorCurrentFilePath = string.Empty;
            _luaEditorSuggestedFileName = string.IsNullOrWhiteSpace(result.SuggestedFileName)
                ? "restored.lua"
                : result.SuggestedFileName;
            _luaEditorEncodingLabel = "UTF-8";
            _luaEditorSourceContext = new MainWindowViewModel.LuaEditorSourceContext(
                result.ElementId,
                result.ElementDisplayName ?? string.Empty,
                result.NodeLabel ?? string.Empty,
                result.PropertyName ?? string.Empty,
                _luaEditorSuggestedFileName);
            _luaEditorOriginalDbRecord = null;
            DisableStructuredLuaEditor();
            if (!TryEnableStructuredLuaEditor(result.ScriptContent, result.NodeLabel))
            {
                SetLuaEditorText(result.ScriptContent);
            }
            MarkLuaEditorCleanFromCurrentContent();
            UpdateLuaEditorHeader();
            if (DataContext is MainWindowViewModel vm)
            {
                vm.StatusMessage = "Loaded backup content into LUA editor.";
            }
        }
        catch (Exception ex)
        {
            if (DataContext is MainWindowViewModel vm)
            {
                vm.StatusMessage = $"Backup manager failed: {ex.Message}";
            }
        }
    }

    private void OnLuaEditorUndoClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        LuaSourceEditor.Undo();
    }

    private void OnLuaEditorRedoClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        LuaSourceEditor.Redo();
    }

    private void OnLuaEditorCutClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        LuaSourceEditor.Cut();
    }

    private void OnLuaEditorCopyClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        LuaSourceEditor.Copy();
    }

    private void OnLuaEditorPasteClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        LuaSourceEditor.Paste();
    }

    private void OnLuaEditorWordWrapChanged(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        bool wrap = LuaEditorWordWrapToggle.IsChecked == true;
        LuaSourceEditor.WordWrap = wrap;
        LuaSourceEditor.HorizontalScrollBarVisibility = wrap ? ScrollBarVisibility.Disabled : ScrollBarVisibility.Auto;
    }
}
