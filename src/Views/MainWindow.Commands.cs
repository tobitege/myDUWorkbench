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
    private void AttachHierarchicalGridLeftNavigation()
    {
        foreach (DataGrid grid in _hierarchicalTreeGrids)
        {
            HierarchicalGridLeftNavigationHelper.Attach(grid);
        }
    }

    private void DetachHierarchicalGridLeftNavigation()
    {
        foreach (DataGrid grid in _hierarchicalTreeGrids)
        {
            HierarchicalGridLeftNavigationHelper.Detach(grid);
        }
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

    private async void OnRepairDestroyedElementsClick(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanRepairDestroyedElements)
        {
            vm.StatusMessage = "Repair is unavailable. Load a DB snapshot first.";
            return;
        }

        var dialog = new ConfirmationDialog(
            "Repair element flags",
            "Delete destroyed and restoreCount properties for all elements in the loaded construct?",
            "Repair",
            "Cancel");
        bool confirmed = await dialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            await vm.RepairDestroyedElementsAsync(cts.Token);
        }
        catch (Exception ex)
        {
            vm.RepairStatusText = "Repair failed.";
            vm.StatusMessage = $"Repair failed: {ex.Message}";
        }
    }

    private async void OnDeleteBlueprintClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanDeleteBlueprint || vm.SelectedBlueprint is not { } bp)
        {
            vm.BlueprintsStatus = "No blueprint selected or database offline.";
            return;
        }

        var dialog = new ConfirmationDialog(
            "Delete Blueprint",
            $"Delete blueprint '{bp.Name}' (ID {bp.Id}, {bp.ElementCount} element(s))?",
            "Delete",
            "Cancel");
        bool confirmed = await dialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await vm.DeleteBlueprintAsync(cts.Token);
        }
        catch (Exception ex)
        {
            vm.BlueprintsStatus = $"Delete failed: {ex.Message}";
        }
    }

    private async void OnCopyBlueprintClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanCopyBlueprint || vm.SelectedBlueprint is not { } bp)
        {
            vm.BlueprintsStatus = "No blueprint selected or database offline.";
            return;
        }

        string initialName = string.IsNullOrWhiteSpace(bp.Name) ? "BlueprintCopy" : $"{bp.Name} Copy";
        var dialog = new TextInputDialog(
            "Copy Blueprint",
            "New name",
            initialName,
            "Copy",
            "Cancel",
            vm.ValidateBlueprintNameInput);
        string? newName = await dialog.ShowDialog<string?>(this);
        if (string.IsNullOrWhiteSpace(newName))
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await vm.CopyBlueprintAsync(newName, cts.Token);
        }
        catch (Exception ex)
        {
            vm.BlueprintsStatus = $"Copy failed: {ex.Message}";
        }
    }

    private async void OnExportBlueprintJsonClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanEditBlueprint || vm.SelectedBlueprint is null)
        {
            vm.BlueprintsStatus = "No blueprint selected or database offline.";
            return;
        }

        var optionsDialog = new BlueprintExportOptionsDialog();
        bool confirmed = await optionsDialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            string json = await vm.ExportSelectedBlueprintJsonAsync(
                optionsDialog.ExcludeVoxels,
                optionsDialog.ExcludeElementsLinks,
                cts.Token);

            vm.BlueprintsStatus = "Blueprint JSON export ready.";
            var dialog = new ExportJsonDialog(json)
            {
                Title = "Blueprint Export JSON"
            };
            await dialog.ShowDialog(this);
        }
        catch (Exception ex)
        {
            vm.BlueprintsStatus = $"Export failed: {ex.Message}";
        }
    }

    private void OnClearConstructSelectionClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        vm.SelectedConstructNameSuggestion = null;
        vm.ConstructIdInput = string.Empty;
    }

    private void OnClearPlayerSelectionClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        vm.SelectedPlayerNameSuggestion = null;
        vm.PlayerIdInput = string.Empty;
        vm.PersistSettingsNow();
    }

    private async void OnImportBlueprintClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (vm.SelectedPlayerNameSuggestion?.PlayerId is not ulong selectedPlayerId || selectedPlayerId == 0UL)
        {
            const string message = "Select a player first (Player ID > 0) before importing a blueprint.";
            vm.StatusMessage = message;
            vm.BlueprintsStatus = message;
            return;
        }

        try
        {
            var importDialog = new BlueprintImportDialog(
                vm.BlueprintImportDryRunMode,
                vm.BlueprintImportIntoApp,
                vm.BlueprintImportIntoGameDatabase,
                vm.BlueprintImportAppendDateIfExists,
                vm.LastSavedFolder);
            bool confirmed = await importDialog.ShowDialog<bool>(this);
            if (!confirmed)
            {
                vm.StatusMessage = "Blueprint import cancelled.";
                return;
            }

            vm.BlueprintImportDryRunMode = importDialog.DryRunMode;
            vm.BlueprintImportIntoApp = importDialog.ImportIntoApp;
            vm.BlueprintImportIntoGameDatabase = importDialog.ImportIntoGameDatabase;
            vm.BlueprintImportAppendDateIfExists = importDialog.AppendDateIfExists;

            IStorageFile? selectedFile = importDialog.SelectedFile;
            string sourcePath = importDialog.SelectedSourcePath;
            if (string.IsNullOrWhiteSpace(sourcePath))
            {
                vm.StatusMessage = "Blueprint import cancelled.";
                return;
            }

            UpdateLastUsedFolder(vm, sourcePath);

            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
            if (Path.IsPathRooted(sourcePath) && File.Exists(sourcePath))
            {
                await vm.ImportBlueprintFileAsync(
                    sourcePath,
                    vm.BlueprintImportDryRunMode,
                    vm.BlueprintImportIntoApp,
                    vm.BlueprintImportIntoGameDatabase,
                    vm.BlueprintImportAppendDateIfExists,
                    cts.Token);
            }
            else if (selectedFile is not null)
            {
                await using Stream readStream = await selectedFile.OpenReadAsync();
                using var reader = new StreamReader(readStream, Encoding.UTF8, true);
                string json = await reader.ReadToEndAsync();
                await vm.ImportBlueprintJsonAsync(
                    json,
                    sourcePath,
                    vm.BlueprintImportDryRunMode,
                    vm.BlueprintImportIntoApp,
                    vm.BlueprintImportIntoGameDatabase,
                    vm.BlueprintImportAppendDateIfExists,
                    cts.Token);
            }
            else
            {
                throw new InvalidOperationException("Selected blueprint file is unavailable.");
            }
        }
        catch (OperationCanceledException)
        {
            vm.LastBlueprintImportErrorDetails = string.Empty;
            vm.StatusMessage = "Blueprint import cancelled.";
        }
        catch (Exception ex)
        {
            vm.SetBlueprintImportError(ex);
        }
    }

    private async void OnStatusMessagePointerPressed(object? sender, PointerPressedEventArgs e)
    {
        _ = sender;
        if (!e.GetCurrentPoint(this).Properties.IsLeftButtonPressed)
        {
            return;
        }

        if (DataContext is not MainWindowViewModel vm ||
            !vm.TryGetBlueprintImportErrorDetails(out string title, out string details))
        {
            return;
        }

        var dialog = new StatusDetailsDialog(title, details);
        await dialog.ShowDialog(this);
        e.Handled = true;
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

            UpdateLastUsedFolder(vm, ResolveStorageFilePath(file));

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

    private static string ResolveStorageFilePath(IStorageFile file)
    {
        if (file.Path is Uri pathUri && pathUri.IsFile)
        {
            return pathUri.LocalPath;
        }

        return file.Name;
    }

    private static void UpdateLastUsedFolder(MainWindowViewModel vm, string? filePath)
    {
        if (vm is null || string.IsNullOrWhiteSpace(filePath))
        {
            return;
        }

        string? folderPath = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(folderPath))
        {
            vm.LastSavedFolder = folderPath;
        }
    }
}
