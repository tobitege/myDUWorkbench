using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace myDUWorker.Views;

public partial class BlueprintImportDialog : Window
{
    private string _lastFolderPath = string.Empty;
    private bool _isPickingFile;

    public BlueprintImportDialog()
    {
        InitializeComponent();
    }

    public BlueprintImportDialog(
        bool dryRunMode,
        bool importIntoApp,
        bool importIntoGameDatabase,
        bool appendDateIfExists,
        string? initialFolderPath)
        : this()
    {
        Title = "Import Blueprint";
        DryRunCheckBox.IsChecked = dryRunMode;
        ImportIntoAppCheckBox.IsChecked = importIntoApp;
        ImportIntoGameDatabaseCheckBox.IsChecked = importIntoGameDatabase;
        AppendDateIfExistsCheckBox.IsChecked = appendDateIfExists;
        _lastFolderPath = initialFolderPath?.Trim() ?? string.Empty;
    }

    public IStorageFile? SelectedFile { get; private set; }
    public string SelectedSourcePath => (FilePathTextBox.Text ?? string.Empty).Trim();
    public bool DryRunMode => DryRunCheckBox.IsChecked == true;
    public bool ImportIntoApp => ImportIntoAppCheckBox.IsChecked == true;
    public bool ImportIntoGameDatabase => ImportIntoGameDatabaseCheckBox.IsChecked == true;
    public bool AppendDateIfExists => AppendDateIfExistsCheckBox.IsChecked == true;

    protected override void OnOpened(EventArgs e)
    {
        base.OnOpened(e);
        _ = PickBlueprintFileAsync();
    }

    private async void OnBrowseClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await PickBlueprintFileAsync();
    }

    private async void OnFilePathPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        _ = sender;
        if (!e.GetCurrentPoint(this).Properties.IsLeftButtonPressed)
        {
            return;
        }

        e.Handled = true;
        await PickBlueprintFileAsync();
    }

    private async Task PickBlueprintFileAsync()
    {
        if (_isPickingFile)
        {
            return;
        }

        _isPickingFile = true;
        try
        {
            var options = new FilePickerOpenOptions
            {
                Title = "Select Blueprint JSON",
                AllowMultiple = false,
                FileTypeFilter = new[]
                {
                    new FilePickerFileType("JSON files")
                    {
                        Patterns = new[] {"*.json"}
                    },
                    new FilePickerFileType("All files")
                    {
                        Patterns = new[] {"*.*"}
                    }
                }
            };

            if (!string.IsNullOrWhiteSpace(_lastFolderPath))
            {
                Uri? folderUri = TryBuildFolderUri(_lastFolderPath);
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
            SelectedFile = selected;

            string resolvedPath = ResolveStorageFilePath(selected);
            FilePathTextBox.Text = resolvedPath;
            ValidationText.Text = string.Empty;
            ValidationText.IsVisible = false;

            string? folderPath = Path.GetDirectoryName(resolvedPath);
            if (!string.IsNullOrWhiteSpace(folderPath))
            {
                _lastFolderPath = folderPath;
            }
        }
        finally
        {
            _isPickingFile = false;
        }
    }

    private void OnImportClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        string sourcePath = SelectedSourcePath;
        if (string.IsNullOrWhiteSpace(sourcePath))
        {
            ValidationText.Text = "Select a blueprint file first.";
            ValidationText.IsVisible = true;
            return;
        }

        if (Path.IsPathRooted(sourcePath) && !File.Exists(sourcePath))
        {
            ValidationText.Text = "Selected file path does not exist anymore.";
            ValidationText.IsVisible = true;
            return;
        }

        if (!Path.IsPathRooted(sourcePath) && SelectedFile is null)
        {
            ValidationText.Text = "Please select a file with the picker.";
            ValidationText.IsVisible = true;
            return;
        }

        ValidationText.Text = string.Empty;
        ValidationText.IsVisible = false;
        Close(true);
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(false);
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

    private static string ResolveStorageFilePath(IStorageFile file)
    {
        if (file.Path is Uri pathUri && pathUri.IsFile)
        {
            return pathUri.LocalPath;
        }

        return file.Name;
    }
}
