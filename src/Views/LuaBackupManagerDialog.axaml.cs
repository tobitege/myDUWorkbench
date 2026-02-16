// Helper Index:
// - RefreshBackupsAsync: Loads backup list from local backup store and binds grid rows.
// - OnLoadIntoEditorClick: Returns selected backup content to caller for editor restore.
using Avalonia.Controls;
using Avalonia.Interactivity;
using MyDu.Models;
using MyDu.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;

namespace MyDu.Views;

public sealed record LuaBackupManagerDialogResult(
    string ScriptContent,
    ulong? ElementId,
    string ElementDisplayName,
    string NodeLabel,
    string PropertyName,
    string SuggestedFileName);

public partial class LuaBackupManagerDialog : Window
{
    private readonly LuaBackupService _backupService;
    private readonly ObservableCollection<LuaBackupEntry> _backups = new();
    private readonly string _currentScriptContent;
    private LuaBackupDocument? _selectedBackupDocument;

    public LuaBackupManagerDialog()
    {
        InitializeComponent();
        _backupService = new LuaBackupService();
        _currentScriptContent = string.Empty;
        WireUi();
        BackupLocationText.Text = $"Backup folder: {_backupService.BackupDirectoryPath}";
    }

    public LuaBackupManagerDialog(LuaBackupService backupService, string currentScriptContent)
        : this()
    {
        _backupService = backupService ?? throw new ArgumentNullException(nameof(backupService));
        _currentScriptContent = currentScriptContent ?? string.Empty;
        CurrentContentTextBox.Text = _currentScriptContent;
        BackupLocationText.Text = $"Backup folder: {_backupService.BackupDirectoryPath}";
    }

    private void WireUi()
    {
        BackupsGrid.ItemsSource = _backups;
        Opened += OnOpened;
    }

    private async void OnOpened(object? sender, EventArgs e)
    {
        _ = sender;
        _ = e;
        await RefreshBackupsAsync();
    }

    private async Task RefreshBackupsAsync()
    {
        IReadOnlyList<LuaBackupEntry> backups = await _backupService.GetBackupsAsync(CancellationToken.None);
        _backups.Clear();
        foreach (LuaBackupEntry backup in backups)
        {
            _backups.Add(backup);
        }

        if (_backups.Count == 0)
        {
            SelectedBackupContentTextBox.Text = string.Empty;
            LoadIntoEditorButton.IsEnabled = false;
            _selectedBackupDocument = null;
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
            SelectedBackupContentTextBox.Text = string.Empty;
            LoadIntoEditorButton.IsEnabled = false;
            _selectedBackupDocument = null;
            return;
        }

        LuaBackupDocument? document = await _backupService.ReadBackupAsync(selected.FilePath, CancellationToken.None);
        _selectedBackupDocument = document;
        SelectedBackupContentTextBox.Text = document?.ScriptContent ?? string.Empty;
        LoadIntoEditorButton.IsEnabled = document is not null;
    }

    private async void OnDeleteSelectedClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        if (BackupsGrid.SelectedItem is not LuaBackupEntry selected)
        {
            return;
        }

        await _backupService.DeleteBackupAsync(selected.FilePath, CancellationToken.None);
        await RefreshBackupsAsync();
    }

    private async void OnDeleteAllClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await _backupService.DeleteAllBackupsAsync(CancellationToken.None);
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
        var result = new LuaBackupManagerDialogResult(
            _selectedBackupDocument.ScriptContent,
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
}
