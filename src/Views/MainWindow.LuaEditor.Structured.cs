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
            return;
        }

        ClearLuaEditorInfoStatusMessage(clearCurrentMessage: true);
    }

    private void ShowLuaEditorInfoStatusMessage(string message)
    {
        if (DataContext is not MainWindowViewModel vm || string.IsNullOrWhiteSpace(message))
        {
            return;
        }

        vm.StatusMessage = message;
        _luaInfoStatusMessage = message;
        _luaInfoStatusClearCts?.Cancel();
        _luaInfoStatusClearCts?.Dispose();

        var cts = new CancellationTokenSource();
        _luaInfoStatusClearCts = cts;
        _ = ClearLuaEditorInfoStatusMessageAfterDelayAsync(
            vm,
            message,
            TimeSpan.FromSeconds(LuaEditorInfoStatusClearDelaySeconds),
            cts.Token);
    }

    private void ClearLuaEditorInfoStatusMessage(bool clearCurrentMessage)
    {
        _luaInfoStatusClearCts?.Cancel();
        _luaInfoStatusClearCts?.Dispose();
        _luaInfoStatusClearCts = null;

        if (clearCurrentMessage &&
            DataContext is MainWindowViewModel vm &&
            !string.IsNullOrWhiteSpace(_luaInfoStatusMessage) &&
            string.Equals(vm.StatusMessage, _luaInfoStatusMessage, StringComparison.Ordinal))
        {
            vm.StatusMessage = string.Empty;
        }

        _luaInfoStatusMessage = string.Empty;
    }

    private async Task ClearLuaEditorInfoStatusMessageAfterDelayAsync(
        MainWindowViewModel vm,
        string message,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        await Dispatcher.UIThread.InvokeAsync(() =>
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (!string.Equals(_luaInfoStatusMessage, message, StringComparison.Ordinal))
            {
                return;
            }

            if (string.Equals(vm.StatusMessage, message, StringComparison.Ordinal))
            {
                vm.StatusMessage = string.Empty;
            }

            _luaInfoStatusMessage = string.Empty;
            _luaInfoStatusClearCts?.Dispose();
            _luaInfoStatusClearCts = null;
        });
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
}
