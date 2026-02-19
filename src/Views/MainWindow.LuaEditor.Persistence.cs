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
            ShowLuaEditorInfoStatusMessage(openStatus);
        }
    }
}
