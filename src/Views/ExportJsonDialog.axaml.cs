// Helper Index:
// - ExportJsonDialog(string json): Initializes dialog with prefilled export payload text.
// - OnSaveToClick: Saves JSON to a user-selected folder with auto-generated .json filename.
// - OnCopyClick: Copies JSON content to clipboard.
// - OnCloseClick: Closes the modal dialog from the UI button handler.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input.Platform;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using myDUWorkbench.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace myDUWorkbench.Views;

public partial class ExportJsonDialog : Window
{
    public ExportJsonDialog()
    {
        InitializeComponent();
    }

    public ExportJsonDialog(string json)
        : this()
    {
        JsonTextBox.Text = json;
    }

    private async void OnSaveToClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        try
        {
            TopLevel? topLevel = TopLevel.GetTopLevel(this);
            IStorageProvider? storageProvider = topLevel?.StorageProvider;
            if (storageProvider is null || !storageProvider.CanPickFolder)
            {
                ActionResultText.Text = "Folder picker unavailable.";
                return;
            }

            IReadOnlyList<IStorageFolder> folders = await storageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions
            {
                Title = "Select export folder",
                AllowMultiple = false
            });
            if (folders.Count == 0)
            {
                return;
            }

            string? folderPath = TryResolveStorageFolderPath(folders[0]);
            if (string.IsNullOrWhiteSpace(folderPath))
            {
                ActionResultText.Text = "Selected folder path is unavailable.";
                return;
            }

            string jsonContent = JsonTextBox.Text ?? string.Empty;
            string fileName = BuildDefaultExportFileName();
            string outputPath = EnsureUniqueFilePath(folderPath, fileName);

            await File.WriteAllTextAsync(outputPath, jsonContent, new UTF8Encoding(false));
            ActionResultText.Text = $"Saved: {Path.GetFileName(outputPath)}";
        }
        catch (Exception ex)
        {
            ActionResultText.Text = $"Save failed: {ex.Message}";
        }
    }

    private async void OnCopyClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        try
        {
            TopLevel? topLevel = TopLevel.GetTopLevel(this);
            IClipboard? clipboard = topLevel?.Clipboard;
            if (clipboard is null)
            {
                ActionResultText.Text = "Clipboard unavailable.";
                return;
            }

            await ClipboardExtensions.SetTextAsync(clipboard, JsonTextBox.Text ?? string.Empty);
            ActionResultText.Text = "Copied.";
        }
        catch (Exception ex)
        {
            ActionResultText.Text = $"Copy failed: {ex.Message}";
        }
    }

    private void OnCloseClick(object? sender, RoutedEventArgs e)
    {
        Close();
    }

    private string BuildDefaultExportFileName()
    {
        string baseName = FileNameHelper.SanitizeGeneratedFileName(Title, "export_json");
        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        return $"{baseName}_{timestamp}.json";
    }

    private static string EnsureUniqueFilePath(string directoryPath, string fileName)
    {
        string baseName = Path.GetFileNameWithoutExtension(fileName);
        string extension = Path.GetExtension(fileName);
        string candidate = Path.Combine(directoryPath, fileName);
        if (!File.Exists(candidate))
        {
            return candidate;
        }

        int suffix = 2;
        while (true)
        {
            string nextName = $"{baseName}_{suffix}{extension}";
            candidate = Path.Combine(directoryPath, nextName);
            if (!File.Exists(candidate))
            {
                return candidate;
            }

            suffix++;
        }
    }

    private static string? TryResolveStorageFolderPath(IStorageFolder folder)
    {
        if (folder.Path is Uri uri && uri.IsFile)
        {
            return uri.LocalPath;
        }

        return null;
    }
}
