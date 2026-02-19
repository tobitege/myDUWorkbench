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
        ClearLuaEditorInfoStatusMessage(clearCurrentMessage: false);
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

        string cacheDirectory = Path.Combine(Path.GetTempPath(), "myDUWorkbench", "Grammars");
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
}
