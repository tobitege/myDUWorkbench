// Helper Index:
// - SaveBlobAsync: Saves selected LUA/HTML/databank content through Avalonia storage APIs.
// - CaptureWindowPlacement: Captures current size, position, screen, and maximized state.
// - ApplyWindowPlacement: Restores saved window geometry while keeping it on a visible screen.
// - ApplyColumnWidths: Restores persisted DataGrid column widths for all tracked grids.
using Avalonia.Controls;
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
using MyDu.Controls;
using MyDu.Helpers;
using MyDu.Models;
using MyDu.Services;
using MyDu.ViewModels;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TextMateSharp.Grammars;

namespace MyDu.Views;

public partial class MainWindow : Window
{
    private const string LuaGrammarResourceSuffix = "Grammars.lua.tmLanguage.json";
    private const string LuaGrammarCacheFileName = "lua.tmLanguage.json";
    private const int LuaFoldRefreshDebounceMs = 250;

    private BreakpointMargin? _luaBreakpointMargin;
    private ExecutionLineHighlighter? _luaExecutionHighlighter;
    private TextMate.Installation? _luaTextMateInstallation;
    private FoldingManager? _luaFoldingManager;
    private DispatcherTimer? _luaFoldRefreshTimer;
    private ToolTip? _luaHoverToolTip;
    private readonly HashSet<int> _luaBreakpoints = new();
    private readonly MyDuDataService _dataService = new();
    private readonly LuaBackupService _luaBackupService = new();
    private string _luaEditorCurrentFilePath = string.Empty;
    private string _luaEditorSuggestedFileName = "script.lua";
    private MainWindowViewModel.LuaEditorSourceContext? _luaEditorSourceContext;
    private LuaPropertyRawRecord? _luaEditorOriginalDbRecord;
    private int _luaExecutionLine = -1;
    private bool _luaExecutionLineIsCurrentFile = true;
    private bool _luaEditorInitialized;
    private bool _luaPendingDoubleClickSelection;

    public MainWindow()
    {
        InitializeComponent();
        Loaded += OnLoaded;
        Opened += OnOpened;
        Closing += OnClosing;
    }

    private async void OnExportGetConstructDataClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        try
        {
            string json = vm.BuildGetConstructDataExportJson();
            var dialog = new ExportJsonDialog(json);
            await dialog.ShowDialog(this);
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"Export failed: {ex.Message}";
        }
    }

    private async void OnSaveLuaAsClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm ||
            !vm.TryGetSelectedLuaBlobSaveRequest(out MainWindowViewModel.BlobSaveRequest? request) ||
            request is null)
        {
            return;
        }

        await SaveBlobAsync(vm, request, "Save LUA Blob");
    }

    private async void OnSaveHtmlRsAsClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm ||
            !vm.TryGetSelectedHtmlRsBlobSaveRequest(out MainWindowViewModel.BlobSaveRequest? request) ||
            request is null)
        {
            return;
        }

        await SaveBlobAsync(vm, request, "Save HTML/RS Blob");
    }

    private async void OnSaveDatabankAsClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm ||
            !vm.TryGetSelectedDatabankBlobSaveRequest(out MainWindowViewModel.BlobSaveRequest? request) ||
            request is null)
        {
            return;
        }

        await SaveBlobAsync(vm, request, "Save Databank Blob");
    }

    private async Task SaveBlobAsync(
        MainWindowViewModel vm,
        MainWindowViewModel.BlobSaveRequest request,
        string dialogTitle)
    {
        try
        {
            var options = new FilePickerSaveOptions
            {
                Title = dialogTitle,
                SuggestedFileName = request.SuggestedFileName,
                DefaultExtension = request.DefaultExtension,
                FileTypeChoices = new[]
                {
                    new FilePickerFileType($"{request.DefaultExtension.TrimStart('.').ToUpperInvariant()} files")
                    {
                        Patterns = new[] {$"*{request.DefaultExtension}"}
                    },
                    new FilePickerFileType("All files")
                    {
                        Patterns = new[] {"*.*"}
                    }
                }
            };

            if (!string.IsNullOrWhiteSpace(vm.LastSavedFolder))
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
            if (file is null)
            {
                return;
            }

            await using Stream stream = await file.OpenWriteAsync();
            await using var writer = new StreamWriter(stream, new UTF8Encoding(false));
            await writer.WriteAsync(request.Content);
            await writer.FlushAsync();

            if (file.Path is Uri savedUri && savedUri.IsFile)
            {
                string? folderPath = Path.GetDirectoryName(savedUri.LocalPath);
                if (!string.IsNullOrWhiteSpace(folderPath))
                {
                    vm.LastSavedFolder = folderPath;
                }
            }

            vm.StatusMessage = $"Saved blob to {request.SuggestedFileName}.";
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"Save failed: {ex.Message}";
        }
    }

    private static Uri? TryBuildFolderUri(string folderPath)
    {
        if (string.IsNullOrWhiteSpace(folderPath))
        {
            return null;
        }

        try
        {
            return new Uri(Path.GetFullPath(folderPath));
        }
        catch
        {
            return null;
        }
    }

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
        LuaSourceEditor.Document.TextChanged += LuaDocument_TextChanged;
        LuaSourceEditor.TextArea.Caret.PositionChanged += LuaCaret_PositionChanged;
        LuaSourceEditor.TextArea.PointerPressed += LuaTextArea_PointerPressed;
        LuaSourceEditor.TextArea.PointerReleased += LuaTextArea_PointerReleased;
        LuaSourceEditor.TextArea.PointerMoved += LuaTextArea_PointerMoved;
        LuaSourceEditor.TextArea.PointerExited += LuaTextArea_PointerExited;

        RefreshLuaFoldings();
        UpdateLuaMarkerVisuals();
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
            LuaSourceEditor.TextArea.PointerPressed -= LuaTextArea_PointerPressed;
            LuaSourceEditor.TextArea.PointerReleased -= LuaTextArea_PointerReleased;
            LuaSourceEditor.TextArea.PointerMoved -= LuaTextArea_PointerMoved;
            LuaSourceEditor.TextArea.PointerExited -= LuaTextArea_PointerExited;
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

        string cacheDirectory = Path.Combine(Path.GetTempPath(), "MyDu", "Grammars");
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
        UpdateLuaEditorStatus();
    }

    private void LuaCaret_PositionChanged(object? sender, EventArgs e)
    {
        UpdateLuaEditorStatus();
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
        if (!TryShowLuaHoverTooltip(e))
        {
            HideLuaHoverTooltip();
        }
    }

    private void LuaTextArea_PointerExited(object? sender, PointerEventArgs e)
    {
        _ = sender;
        _ = e;
        HideLuaHoverTooltip();
    }

    private bool TryShowLuaHoverTooltip(PointerEventArgs e)
    {
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
        LuaEditorHeaderText.Text = $"LUA Editor - {currentLabel}";
    }

    private void UpdateLuaEditorStatus()
    {
        int line = Math.Max(1, LuaSourceEditor.TextArea.Caret.Line);
        int column = Math.Max(1, LuaSourceEditor.TextArea.Caret.Column);
        string sourceLabel = !string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath)
            ? _luaEditorCurrentFilePath
            : _luaEditorSuggestedFileName;
        LuaEditorStatusText.Text = $"{sourceLabel} | Ln {line}, Col {column}";
    }

    private void UpdateLuaEditorCommandStates()
    {
        LuaEditorSaveButton.IsEnabled = !string.IsNullOrWhiteSpace(_luaEditorCurrentFilePath);
        bool isDbAvailable = DataContext is MainWindowViewModel vm &&
                             string.Equals(vm.DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase);
        LuaEditorSaveToDbButton.IsEnabled = isDbAvailable &&
                                            _luaEditorOriginalDbRecord is not null &&
                                            _luaEditorSourceContext?.ElementId.HasValue == true &&
                                            string.Equals(_luaEditorSourceContext.PropertyName, "dpuyaml_6", StringComparison.OrdinalIgnoreCase);
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

    private async Task SaveLuaEditorToPathAsync(string path)
    {
        string content = LuaSourceEditor.Text ?? string.Empty;
        await File.WriteAllTextAsync(path, content, new UTF8Encoding(false));
        await CreateLuaVersionBackupAsync(content, path);
        _luaEditorCurrentFilePath = path;
        _luaEditorSuggestedFileName = Path.GetFileName(path);
        if (DataContext is MainWindowViewModel vm)
        {
            string? folder = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(folder))
            {
                vm.LastSavedFolder = folder;
            }

            vm.StatusMessage = $"Saved LUA editor file to {path}.";
        }

        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
        UpdateLuaEditorCommandStates();
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

        await SaveLuaEditorToPathAsync(filePathUri.LocalPath);
    }

    private async Task OpenLuaEditorFileAsync()
    {
        var options = new FilePickerOpenOptions
        {
            Title = "Open LUA File",
            AllowMultiple = false,
            FileTypeFilter = new[]
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

        IReadOnlyList<IStorageFile> files = await StorageProvider.OpenFilePickerAsync(options);
        if (files.Count == 0)
        {
            return;
        }

        IStorageFile selected = files[0];
        await using Stream readStream = await selected.OpenReadAsync();
        using var reader = new StreamReader(readStream, Encoding.UTF8, true);
        string text = await reader.ReadToEndAsync();

        string filePath = selected.Name;
        if (selected.Path is Uri pathUri && pathUri.IsFile)
        {
            filePath = pathUri.LocalPath;
        }

        _luaEditorCurrentFilePath = filePath;
        _luaEditorSuggestedFileName = Path.GetFileName(filePath);
        _luaEditorSourceContext = null;
        _luaEditorOriginalDbRecord = null;
        SetLuaEditorText(text);
        UpdateLuaEditorHeader();
        UpdateLuaEditorStatus();
        if (DataContext is MainWindowViewModel vm2)
        {
            string? folder = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrWhiteSpace(folder))
            {
                vm2.LastSavedFolder = folder;
            }

            vm2.StatusMessage = $"Opened LUA editor file {filePath}.";
        }
    }

    private async void OnEditLuaClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

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
        _luaEditorSourceContext = vm.TryGetSelectedLuaEditorSourceContext(out MainWindowViewModel.LuaEditorSourceContext? sourceContext)
            ? sourceContext
            : null;
        _luaEditorOriginalDbRecord = null;

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

        SetLuaEditorText(request.Content);
        UpdateLuaEditorHeader();
        SetLuaEditorPageVisible(true);
        vm.StatusMessage = $"Opened {request.SuggestedFileName} in LUA editor.";
    }

    private void OnLuaEditorBackClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        HideLuaHoverTooltip();
        SetLuaEditorPageVisible(false);
    }

    private void OnLuaEditorNewClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        _luaEditorCurrentFilePath = string.Empty;
        _luaEditorSuggestedFileName = "script.lua";
        _luaEditorSourceContext = null;
        _luaEditorOriginalDbRecord = null;
        SetLuaEditorText(string.Empty);
        UpdateLuaEditorHeader();
    }

    private async void OnLuaEditorOpenClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        try
        {
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
            ulong elementId = sourceContext.ElementId!.Value;
            string propertyName = sourceContext.PropertyName;
            string content = LuaSourceEditor.Text ?? string.Empty;
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
            UpdateLuaEditorCommandStates();
            vm.StatusMessage = result.UsesHashReference
                ? $"Saved to DB (hash {result.HashReference}, sections={result.SectionCount})."
                : $"Saved to DB (inline payload, sections={result.SectionCount}).";
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
            string currentContent = LuaSourceEditor.Text ?? string.Empty;
            var dialog = new LuaBackupManagerDialog(_luaBackupService, currentContent);
            LuaBackupManagerDialogResult? result = await dialog.ShowDialog<LuaBackupManagerDialogResult?>(this);
            if (result is null)
            {
                return;
            }

            _luaEditorCurrentFilePath = string.Empty;
            _luaEditorSuggestedFileName = string.IsNullOrWhiteSpace(result.SuggestedFileName)
                ? "restored.lua"
                : result.SuggestedFileName;
            _luaEditorSourceContext = new MainWindowViewModel.LuaEditorSourceContext(
                result.ElementId,
                result.ElementDisplayName ?? string.Empty,
                result.NodeLabel ?? string.Empty,
                result.PropertyName ?? string.Empty,
                _luaEditorSuggestedFileName);
            _luaEditorOriginalDbRecord = null;
            SetLuaEditorText(result.ScriptContent);
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

    private void OnOpened(object? sender, EventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        ApplyWindowPlacement(vm.GetSavedWindowPlacement());
        ApplyColumnWidths(vm.GetSavedGridColumnWidths());
    }

    private void OnLoaded(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        EnsureLuaEditorInitialized();
        UpdateLuaEditorHeader();
    }

    private void OnClosing(object? sender, WindowClosingEventArgs e)
    {
        CleanupLuaEditor();

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        WindowPlacementSettings placement = CaptureWindowPlacement();
        vm.UpdateWindowPlacement(placement);
        Dictionary<string, string> widths = CaptureColumnWidths();
        vm.UpdateGridColumnWidths(widths);
        vm.PersistSettingsNow();
    }

    private WindowPlacementSettings CaptureWindowPlacement()
    {
        var placement = new WindowPlacementSettings
        {
            StartMaximized = WindowState == WindowState.Maximized
        };

        if (WindowState == WindowState.Normal)
        {
            if (Bounds.Width > 0)
            {
                placement.Width = Bounds.Width;
            }

            if (Bounds.Height > 0)
            {
                placement.Height = Bounds.Height;
            }

            placement.PositionX = Position.X;
            placement.PositionY = Position.Y;
        }

        Screen? currentScreen = Screens?.ScreenFromWindow(this);
        if (currentScreen is not null)
        {
            placement.ScreenKey = BuildScreenKey(currentScreen);
        }

        return placement;
    }

    private void ApplyWindowPlacement(WindowPlacementSettings placement)
    {
        if (placement.Width.HasValue && placement.Width.Value >= MinWidth)
        {
            Width = placement.Width.Value;
        }

        if (placement.Height.HasValue && placement.Height.Value >= MinHeight)
        {
            Height = placement.Height.Value;
        }

        Screen? targetScreen = ResolveTargetScreen(placement.ScreenKey, placement.PositionX, placement.PositionY);
        if (placement.PositionX.HasValue && placement.PositionY.HasValue)
        {
            var requested = new PixelPoint(placement.PositionX.Value, placement.PositionY.Value);
            Position = CoerceToVisiblePosition(requested, targetScreen);
        }
        else if (targetScreen is not null)
        {
            Position = CenterInWorkingArea(targetScreen);
        }

        if (placement.StartMaximized)
        {
            WindowState = WindowState.Maximized;
        }
    }

    private Screen? ResolveTargetScreen(string screenKey, int? x, int? y)
    {
        if (Screens is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(screenKey))
        {
            Screen? byKey = Screens.All.FirstOrDefault(s => string.Equals(BuildScreenKey(s), screenKey, StringComparison.Ordinal));
            if (byKey is not null)
            {
                return byKey;
            }
        }

        if (x.HasValue && y.HasValue)
        {
            Screen? byPoint = Screens.ScreenFromPoint(new PixelPoint(x.Value, y.Value));
            if (byPoint is not null)
            {
                return byPoint;
            }
        }

        return Screens.ScreenFromWindow(this) ?? Screens.Primary;
    }

    private PixelPoint CoerceToVisiblePosition(PixelPoint requested, Screen? screen)
    {
        if (screen is null)
        {
            return requested;
        }

        PixelRect wa = screen.WorkingArea;
        int safeWidth = (int)Math.Round(Math.Max(MinWidth, Width > 0 ? Width : MinWidth));
        int safeHeight = (int)Math.Round(Math.Max(MinHeight, Height > 0 ? Height : MinHeight));

        int maxX = Math.Max(wa.X, wa.X + wa.Width - Math.Min(safeWidth, wa.Width));
        int maxY = Math.Max(wa.Y, wa.Y + wa.Height - Math.Min(safeHeight, wa.Height));

        int x = Math.Clamp(requested.X, wa.X, maxX);
        int y = Math.Clamp(requested.Y, wa.Y, maxY);
        return new PixelPoint(x, y);
    }

    private PixelPoint CenterInWorkingArea(Screen screen)
    {
        PixelRect wa = screen.WorkingArea;
        int safeWidth = (int)Math.Round(Math.Max(MinWidth, Width > 0 ? Width : MinWidth));
        int safeHeight = (int)Math.Round(Math.Max(MinHeight, Height > 0 ? Height : MinHeight));
        int x = wa.X + Math.Max(0, (wa.Width - safeWidth) / 2);
        int y = wa.Y + Math.Max(0, (wa.Height - safeHeight) / 2);
        return new PixelPoint(x, y);
    }

    private static string BuildScreenKey(Screen screen)
    {
        PixelRect b = screen.Bounds;
        return $"{b.X},{b.Y},{b.Width},{b.Height}";
    }

    private Dictionary<string, string> CaptureColumnWidths()
    {
        var widths = new Dictionary<string, string>(StringComparer.Ordinal);
        CaptureGridColumnWidths(ConfigPropertiesGrid, "config-grid", widths);
        CaptureGridColumnWidths(ElementPropertiesGrid, "element-grid", widths);
        CaptureGridColumnWidths(LuaBlocksGrid, "lua-grid", widths);
        CaptureGridColumnWidths(HtmlRsGrid, "content2-grid", widths);
        CaptureGridColumnWidths(DatabankGrid, "databank-grid", widths);
        return widths;
    }

    private static void CaptureGridColumnWidths(DataGrid grid, string gridKey, Dictionary<string, string> widths)
    {
        for (int i = 0; i < grid.Columns.Count; i++)
        {
            DataGridColumn column = grid.Columns[i];
            string key = BuildColumnWidthKey(gridKey, i, column);
            widths[key] = SerializeColumnWidth(column.Width);
        }
    }

    private void ApplyColumnWidths(IReadOnlyDictionary<string, string> persisted)
    {
        ApplyGridColumnWidths(ConfigPropertiesGrid, "config-grid", persisted);
        ApplyGridColumnWidths(ElementPropertiesGrid, "element-grid", persisted);
        ApplyGridColumnWidths(LuaBlocksGrid, "lua-grid", persisted);
        ApplyGridColumnWidths(HtmlRsGrid, "content2-grid", persisted);
        ApplyGridColumnWidths(DatabankGrid, "databank-grid", persisted);
    }

    private static void ApplyGridColumnWidths(DataGrid grid, string gridKey, IReadOnlyDictionary<string, string> persisted)
    {
        for (int i = 0; i < grid.Columns.Count; i++)
        {
            DataGridColumn column = grid.Columns[i];
            string key = BuildColumnWidthKey(gridKey, i, column);
            if (!persisted.TryGetValue(key, out string? serialized) || string.IsNullOrWhiteSpace(serialized))
            {
                continue;
            }

            if (TryParseColumnWidth(serialized, out DataGridLength width))
            {
                column.Width = width;
            }
        }
    }

    private static string BuildColumnWidthKey(string gridKey, int index, DataGridColumn column)
    {
        string header = column.Header?.ToString() ?? string.Empty;
        return $"{gridKey}|{index}|{header}";
    }

    private static string SerializeColumnWidth(DataGridLength width)
    {
        if (width.IsAuto)
        {
            return "Auto";
        }

        if (width.IsSizeToCells)
        {
            return "SizeToCells";
        }

        if (width.IsSizeToHeader)
        {
            return "SizeToHeader";
        }

        if (width.IsStar)
        {
            return $"Star:{width.Value.ToString("R", CultureInfo.InvariantCulture)}";
        }

        return $"Pixel:{width.Value.ToString("R", CultureInfo.InvariantCulture)}";
    }

    private static bool TryParseColumnWidth(string serialized, out DataGridLength width)
    {
        width = default;
        if (string.Equals(serialized, "Auto", StringComparison.Ordinal))
        {
            width = DataGridLength.Auto;
            return true;
        }

        if (string.Equals(serialized, "SizeToCells", StringComparison.Ordinal))
        {
            width = DataGridLength.SizeToCells;
            return true;
        }

        if (string.Equals(serialized, "SizeToHeader", StringComparison.Ordinal))
        {
            width = DataGridLength.SizeToHeader;
            return true;
        }

        if (serialized.StartsWith("Star:", StringComparison.Ordinal) &&
            double.TryParse(serialized[5..], NumberStyles.Float, CultureInfo.InvariantCulture, out double star) &&
            star > 0)
        {
            width = new DataGridLength(star, DataGridLengthUnitType.Star);
            return true;
        }

        if (serialized.StartsWith("Pixel:", StringComparison.Ordinal) &&
            double.TryParse(serialized[6..], NumberStyles.Float, CultureInfo.InvariantCulture, out double px) &&
            px > 0)
        {
            width = new DataGridLength(px, DataGridLengthUnitType.Pixel);
            return true;
        }

        return false;
    }
}