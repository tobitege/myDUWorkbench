// Helper Index:
// - RefreshBackupsAsync: Loads backup list from local backup store and binds grid rows.
// - OnLoadIntoEditorClick: Returns selected backup content to caller for restore/load flows.
using Avalonia.Controls;
using Avalonia.Interactivity;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.Views;

public sealed record BackupManagerDialogOptions(
    BackupContentKind ContentKind,
    string WindowTitle,
    string CurrentContentHeader,
    string SelectedBackupHeader,
    string ComparisonHeader,
    string LoadButtonText,
    string EmptyComparisonText);

public partial class LuaBackupManagerDialog : Window
{
    private const int DiffContextLines = 2;
    private const int MaxLcsCells = 2_000_000;

    private readonly LuaBackupService _backupService;
    private readonly ObservableCollection<LuaBackupEntry> _backups = new();
    private readonly string _currentContent;
    private readonly BackupManagerDialogOptions _options;

    private LuaBackupDocument? _selectedBackupDocument;
    private CancellationTokenSource? _selectionLoadCts;

    public LuaBackupManagerDialog()
        : this(new LuaBackupService(), string.Empty, CreateLuaOptions())
    {
    }

    public LuaBackupManagerDialog(LuaBackupService backupService, string currentContent)
        : this(backupService, currentContent, CreateLuaOptions())
    {
    }

    public LuaBackupManagerDialog(
        LuaBackupService backupService,
        string currentContent,
        BackupManagerDialogOptions options)
    {
        InitializeComponent();
        _backupService = backupService ?? throw new ArgumentNullException(nameof(backupService));
        _currentContent = currentContent ?? string.Empty;
        _options = options ?? throw new ArgumentNullException(nameof(options));

        WireUi();
        ApplyOptions();
        CurrentContentTextBox.Text = _currentContent;
        ClearSelectedBackupPresentation();
    }

    public static BackupManagerDialogOptions CreateLuaOptions()
    {
        return new BackupManagerDialogOptions(
            BackupContentKind.Lua,
            "LUA Backups",
            "Current editor content",
            "Selected backup content",
            "Line comparison",
            "Load into editor",
            "Select a backup to compare.");
    }

    public static BackupManagerDialogOptions CreateDatabankOptions()
    {
        return new BackupManagerDialogOptions(
            BackupContentKind.Databank,
            "Databank Backups",
            "Current databank content",
            "Selected databank backup",
            "Line comparison",
            "Restore to DB",
            "Select a databank backup to compare.");
    }

    private void WireUi()
    {
        BackupsGrid.ItemsSource = _backups;
        Opened += OnOpened;
        Closed += OnClosed;
    }

    private void ApplyOptions()
    {
        Title = _options.WindowTitle;
        CurrentContentHeaderText.Text = _options.CurrentContentHeader;
        SelectedBackupHeaderText.Text = _options.SelectedBackupHeader;
        ComparisonHeaderText.Text = _options.ComparisonHeader;
        LoadIntoEditorButton.Content = _options.LoadButtonText;
        BackupLocationText.Text = BuildBackupLocationLabel(_backupService, _options.ContentKind);
    }

    private async void OnOpened(object? sender, EventArgs e)
    {
        _ = sender;
        _ = e;
        await RefreshBackupsAsync();
    }

    private void OnClosed(object? sender, EventArgs e)
    {
        _ = sender;
        _ = e;
        CancelSelectionLoad();
    }

    private async Task RefreshBackupsAsync()
    {
        string? selectedFilePath = (BackupsGrid.SelectedItem as LuaBackupEntry)?.FilePath;
        CancelSelectionLoad();

        IReadOnlyList<LuaBackupEntry> backups = await _backupService.GetBackupsAsync(_options.ContentKind, CancellationToken.None);
        _backups.Clear();
        foreach (LuaBackupEntry backup in backups)
        {
            _backups.Add(backup);
        }

        LuaBackupEntry? restoredSelection = string.IsNullOrWhiteSpace(selectedFilePath)
            ? null
            : _backups.FirstOrDefault(entry => string.Equals(entry.FilePath, selectedFilePath, StringComparison.OrdinalIgnoreCase));
        BackupsGrid.SelectedItem = restoredSelection;

        if (restoredSelection is null)
        {
            ClearSelectedBackupPresentation();
        }
    }

    private async void OnRefreshClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await RefreshBackupsAsync();
    }

    private async void OnBackupsSelectionChanged(object? sender, SelectionChangedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (BackupsGrid.SelectedItem is not LuaBackupEntry selected)
        {
            CancelSelectionLoad();
            ClearSelectedBackupPresentation();
            return;
        }

        CancellationTokenSource selectionLoadCts = BeginSelectionLoad();
        LuaBackupDocument? document;
        try
        {
            document = await _backupService.ReadBackupAsync(selected.FilePath, selectionLoadCts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (!ReferenceEquals(_selectionLoadCts, selectionLoadCts) ||
            selectionLoadCts.IsCancellationRequested ||
            BackupsGrid.SelectedItem is not LuaBackupEntry currentSelected ||
            !string.Equals(currentSelected.FilePath, selected.FilePath, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        bool isUnsafeLegacyDatabankBackup =
            _options.ContentKind == BackupContentKind.Databank &&
            document is not null &&
            LuaBackupService.LooksLikeCorruptedDatabankBackupText(document.ScriptContent);

        _selectedBackupDocument = document;
        SelectedBackupContentTextBox.Text = document?.ScriptContent ?? string.Empty;
        LoadIntoEditorButton.IsEnabled = document is not null && !isUnsafeLegacyDatabankBackup;
        UpdateComparisonPresentation(
            document,
            isUnsafeLegacyDatabankBackup
                ? "Selected backup looks corrupted by the old databank backup bug and is blocked from restore."
                : null);
    }

    private async void OnDeleteSelectedClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (BackupsGrid.SelectedItem is not LuaBackupEntry selected)
        {
            return;
        }

        CancelSelectionLoad();
        await _backupService.DeleteBackupAsync(selected.FilePath, CancellationToken.None);
        await RefreshBackupsAsync();
    }

    private async void OnDeleteAllClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        CancelSelectionLoad();
        await _backupService.DeleteAllBackupsAsync(_options.ContentKind, CancellationToken.None);
        await RefreshBackupsAsync();
    }

    private void OnLoadIntoEditorClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        if (_selectedBackupDocument is null)
        {
            return;
        }

        LuaBackupEntry entry = _selectedBackupDocument.Entry;
        var result = new BackupManagerDialogResult(
            _selectedBackupDocument.ScriptContent,
            entry.ContentKind,
            entry.ElementId,
            entry.ElementDisplayName,
            entry.NodeLabel,
            entry.PropertyName,
            entry.SuggestedFileName);
        Close(result);
    }

    private void OnCloseClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(null);
    }

    private void ClearSelectedBackupPresentation()
    {
        SelectedBackupContentTextBox.Text = string.Empty;
        ComparisonSummaryText.Text = _options.EmptyComparisonText;
        ComparisonDiffTextBox.Text = string.Empty;
        LoadIntoEditorButton.IsEnabled = false;
        _selectedBackupDocument = null;
    }

    private void UpdateComparisonPresentation(LuaBackupDocument? document, string? warningText = null)
    {
        if (document is null)
        {
            ClearSelectedBackupPresentation();
            return;
        }

        ComparisonResult result = BuildComparisonResult(_currentContent, document.ScriptContent);
        ComparisonSummaryText.Text = string.IsNullOrWhiteSpace(warningText)
            ? result.Summary
            : $"{warningText} {result.Summary}";
        ComparisonDiffTextBox.Text = result.DiffText;
    }

    private CancellationTokenSource BeginSelectionLoad()
    {
        CancelSelectionLoad();
        _selectionLoadCts = new CancellationTokenSource();
        return _selectionLoadCts;
    }

    private void CancelSelectionLoad()
    {
        if (_selectionLoadCts is null)
        {
            return;
        }

        try
        {
            _selectionLoadCts.Cancel();
        }
        catch
        {
        }

        _selectionLoadCts.Dispose();
        _selectionLoadCts = null;
    }

    private static string BuildBackupLocationLabel(LuaBackupService backupService, BackupContentKind contentKind)
    {
        string contentLabel = contentKind == BackupContentKind.Databank ? "Databank" : "Lua";
        return $"Backup folder: {backupService.BackupDirectoryPath} (showing {contentLabel} backups)";
    }

    private static ComparisonResult BuildComparisonResult(string currentContent, string backupContent)
    {
        string[] currentLines = SplitLines(currentContent);
        string[] backupLines = SplitLines(backupContent);
        List<RawDiffLine> rawDiff = BuildRawDiff(currentLines, backupLines);

        int unchangedCount = 0;
        int currentOnlyCount = 0;
        int backupOnlyCount = 0;
        foreach (RawDiffLine line in rawDiff)
        {
            switch (line.Kind)
            {
                case DiffKind.Unchanged:
                    unchangedCount++;
                    break;
                case DiffKind.CurrentOnly:
                    currentOnlyCount++;
                    break;
                case DiffKind.BackupOnly:
                    backupOnlyCount++;
                    break;
            }
        }

        bool identical = currentOnlyCount == 0 && backupOnlyCount == 0;
        string summary = identical
            ? $"Current content matches the selected backup exactly ({currentLines.Length.ToString(CultureInfo.InvariantCulture)} lines)."
            : $"Current lines: {currentLines.Length.ToString(CultureInfo.InvariantCulture)} | Backup lines: {backupLines.Length.ToString(CultureInfo.InvariantCulture)} | Same: {unchangedCount.ToString(CultureInfo.InvariantCulture)} | Current only: {currentOnlyCount.ToString(CultureInfo.InvariantCulture)} | Backup only: {backupOnlyCount.ToString(CultureInfo.InvariantCulture)}";
        string diffText = identical
            ? "No line differences."
            : BuildDiffText(rawDiff);

        return new ComparisonResult(summary, diffText);
    }

    private static List<RawDiffLine> BuildRawDiff(string[] currentLines, string[] backupLines)
    {
        if ((long)currentLines.Length * backupLines.Length > MaxLcsCells)
        {
            return BuildIndexedFallbackDiff(currentLines, backupLines);
        }

        int[,] lcs = BuildLongestCommonSubsequenceTable(currentLines, backupLines);
        var diff = new List<RawDiffLine>();
        int currentIndex = currentLines.Length;
        int backupIndex = backupLines.Length;

        while (currentIndex > 0 || backupIndex > 0)
        {
            if (currentIndex > 0 &&
                backupIndex > 0 &&
                string.Equals(currentLines[currentIndex - 1], backupLines[backupIndex - 1], StringComparison.Ordinal))
            {
                diff.Add(new RawDiffLine(
                    DiffKind.Unchanged,
                    currentIndex,
                    backupIndex,
                    currentLines[currentIndex - 1]));
                currentIndex--;
                backupIndex--;
                continue;
            }

            if (backupIndex > 0 &&
                (currentIndex == 0 || lcs[currentIndex, backupIndex - 1] >= lcs[currentIndex - 1, backupIndex]))
            {
                diff.Add(new RawDiffLine(
                    DiffKind.BackupOnly,
                    null,
                    backupIndex,
                    backupLines[backupIndex - 1]));
                backupIndex--;
                continue;
            }

            diff.Add(new RawDiffLine(
                DiffKind.CurrentOnly,
                currentIndex,
                null,
                currentLines[currentIndex - 1]));
            currentIndex--;
        }

        diff.Reverse();
        return diff;
    }

    private static List<RawDiffLine> BuildIndexedFallbackDiff(string[] currentLines, string[] backupLines)
    {
        var diff = new List<RawDiffLine>(Math.Max(currentLines.Length, backupLines.Length) * 2);
        int sharedLength = Math.Min(currentLines.Length, backupLines.Length);

        for (int index = 0; index < sharedLength; index++)
        {
            if (string.Equals(currentLines[index], backupLines[index], StringComparison.Ordinal))
            {
                diff.Add(new RawDiffLine(DiffKind.Unchanged, index + 1, index + 1, currentLines[index]));
                continue;
            }

            diff.Add(new RawDiffLine(DiffKind.CurrentOnly, index + 1, null, currentLines[index]));
            diff.Add(new RawDiffLine(DiffKind.BackupOnly, null, index + 1, backupLines[index]));
        }

        for (int index = sharedLength; index < currentLines.Length; index++)
        {
            diff.Add(new RawDiffLine(DiffKind.CurrentOnly, index + 1, null, currentLines[index]));
        }

        for (int index = sharedLength; index < backupLines.Length; index++)
        {
            diff.Add(new RawDiffLine(DiffKind.BackupOnly, null, index + 1, backupLines[index]));
        }

        return diff;
    }

    private static int[,] BuildLongestCommonSubsequenceTable(string[] currentLines, string[] backupLines)
    {
        int[,] table = new int[currentLines.Length + 1, backupLines.Length + 1];
        for (int currentIndex = 1; currentIndex <= currentLines.Length; currentIndex++)
        {
            for (int backupIndex = 1; backupIndex <= backupLines.Length; backupIndex++)
            {
                if (string.Equals(currentLines[currentIndex - 1], backupLines[backupIndex - 1], StringComparison.Ordinal))
                {
                    table[currentIndex, backupIndex] = table[currentIndex - 1, backupIndex - 1] + 1;
                }
                else
                {
                    table[currentIndex, backupIndex] = Math.Max(
                        table[currentIndex - 1, backupIndex],
                        table[currentIndex, backupIndex - 1]);
                }
            }
        }

        return table;
    }

    private static string BuildDiffText(IReadOnlyList<RawDiffLine> rawDiff)
    {
        var builder = new StringBuilder();
        int unchangedRunStart = -1;

        void FlushUnchangedRun(int runEndExclusive)
        {
            if (unchangedRunStart < 0)
            {
                return;
            }

            int runLength = runEndExclusive - unchangedRunStart;
            if (runLength <= DiffContextLines * 2)
            {
                for (int index = unchangedRunStart; index < runEndExclusive; index++)
                {
                    AppendDiffLine(builder, rawDiff[index]);
                }
            }
            else
            {
                for (int index = unchangedRunStart; index < unchangedRunStart + DiffContextLines; index++)
                {
                    AppendDiffLine(builder, rawDiff[index]);
                }

                builder.AppendLine($"... {runLength - (DiffContextLines * 2)} unchanged line(s) omitted ...");

                for (int index = runEndExclusive - DiffContextLines; index < runEndExclusive; index++)
                {
                    AppendDiffLine(builder, rawDiff[index]);
                }
            }

            unchangedRunStart = -1;
        }

        for (int index = 0; index < rawDiff.Count; index++)
        {
            if (rawDiff[index].Kind == DiffKind.Unchanged)
            {
                if (unchangedRunStart < 0)
                {
                    unchangedRunStart = index;
                }

                continue;
            }

            FlushUnchangedRun(index);
            AppendDiffLine(builder, rawDiff[index]);
        }

        FlushUnchangedRun(rawDiff.Count);
        return builder.ToString().TrimEnd();
    }

    private static void AppendDiffLine(StringBuilder builder, RawDiffLine line)
    {
        char prefix = line.Kind switch
        {
            DiffKind.Unchanged => '=',
            DiffKind.CurrentOnly => '-',
            DiffKind.BackupOnly => '+',
            _ => '?'
        };

        string currentLine = line.CurrentLineNumber?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
        string backupLine = line.BackupLineNumber?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
        builder.Append(prefix);
        builder.Append(' ');
        builder.Append(currentLine.PadLeft(5));
        builder.Append(" | ");
        builder.Append(backupLine.PadLeft(5));
        builder.Append(" | ");
        builder.AppendLine(line.Text);
    }

    private static string[] SplitLines(string content)
    {
        string normalized = (content ?? string.Empty).Replace("\r\n", "\n").Replace('\r', '\n');
        if (normalized.Length == 0)
        {
            return Array.Empty<string>();
        }

        return normalized.Split('\n');
    }

    private enum DiffKind
    {
        Unchanged = 0,
        CurrentOnly = 1,
        BackupOnly = 2
    }

    private sealed record RawDiffLine(
        DiffKind Kind,
        int? CurrentLineNumber,
        int? BackupLineNumber,
        string Text);

    private sealed record ComparisonResult(
        string Summary,
        string DiffText);
}
