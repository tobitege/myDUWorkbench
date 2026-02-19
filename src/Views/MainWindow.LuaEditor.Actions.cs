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
        ShowLuaEditorInfoStatusMessage($"Opened {request.SuggestedFileName} in LUA editor.");
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
                ShowLuaEditorInfoStatusMessage("Loaded backup content into LUA editor.");
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
