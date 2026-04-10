using Avalonia.Controls;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Controls.Primitives;
using Avalonia.Input;
using Avalonia.Input.Platform;
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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using TextMateSharp.Grammars;

namespace myDUWorkbench.Views;

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
            "Remove 'destroyed' and 'restoreCount' properties for all elements in the loaded construct?",
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

        IReadOnlyList<BlueprintDbRecord> selectedBlueprints = GetSelectedBlueprintRows(vm.SelectedBlueprint);
        if (!vm.CanDeleteBlueprint || selectedBlueprints.Count == 0)
        {
            vm.BlueprintsStatus = "No blueprint selected or database offline.";
            return;
        }

        BlueprintDbRecord primaryBlueprint = selectedBlueprints[0];
        string prompt = selectedBlueprints.Count == 1
            ? $"Delete blueprint '{primaryBlueprint.Name}' (ID {primaryBlueprint.Id}, {primaryBlueprint.ElementCount} element(s))?"
            : $"Delete {selectedBlueprints.Count.ToString(CultureInfo.InvariantCulture)} selected blueprints?";
        string? details = selectedBlueprints.Count <= 1
            ? null
            : BuildBlueprintDeleteDetails(selectedBlueprints);

        var dialog = new ConfirmationDialog(
            selectedBlueprints.Count == 1 ? "Delete Blueprint" : "Delete Blueprints",
            prompt,
            "Delete",
            "Cancel",
            detailsText: details);
        bool confirmed = await dialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        using var cts = new CancellationTokenSource();
        var progressDialog = new BlueprintDeleteProgressDialog(selectedBlueprints.Count);
        progressDialog.CancelRequested += (_, _) => cts.Cancel();
        Task progressDialogTask = progressDialog.ShowDialog(this);

        try
        {
            var progress = new Progress<BlueprintDeleteProgress>(progressDialog.UpdateProgress);
            await vm.DeleteBlueprintsAsync(selectedBlueprints, progress, cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            if (string.IsNullOrWhiteSpace(vm.BlueprintsStatus))
            {
                vm.BlueprintsStatus = $"Delete failed: {ex.Message}";
            }
        }
        finally
        {
            progressDialog.CloseDialog();
            await progressDialogTask;
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

    private async void OnGiveBlueprintToPlayerCoreClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await GiveSelectedBlueprintToPlayerAsync(vm, singleUse: false);
    }

    private async void OnGiveBlueprintToPlayerSingleUseClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await GiveSelectedBlueprintToPlayerAsync(vm, singleUse: true);
    }

    private async Task GiveSelectedBlueprintToPlayerAsync(MainWindowViewModel vm, bool singleUse)
    {
        if (!vm.CanGiveBlueprintToPlayer || vm.SelectedBlueprint is not { } bp)
        {
            vm.BlueprintsStatus = "Select a blueprint and a valid player first.";
            return;
        }

        string playerIdText = vm.SelectedPlayerNameSuggestion?.PlayerId?.ToString(CultureInfo.InvariantCulture)
                              ?? vm.PlayerIdInput?.Trim()
                              ?? string.Empty;
        string modeLabel = singleUse ? "Single-use blueprint" : "Core blueprint";

        var dialog = new ConfirmationDialog(
            "Give Blueprint",
            $"Give blueprint '{bp.Name}' (ID {bp.Id}) to player {playerIdText} as {modeLabel}?",
            "Give",
            "Cancel");
        bool confirmed = await dialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await vm.GiveSelectedBlueprintToPlayerAsync(singleUse, cts.Token);
        }
        catch (Exception ex)
        {
            vm.BlueprintsStatus = $"Grant failed: {ex.Message}";
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
            BeginExportProgress(vm, "Export: preparing blueprint JSON", 10d);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            SetExportProgress(vm, "Export: reading blueprint data", 45d);
            string json = await vm.ExportSelectedBlueprintJsonAsync(
                optionsDialog.ExcludeVoxels,
                optionsDialog.ExcludeElementsLinks,
                cts.Token);
            SetExportProgress(vm, "Export: formatting result", 85d);

            vm.BlueprintsStatus = "Blueprint JSON export ready.";
            SetExportProgress(vm, "Export: ready", 100d);
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
        finally
        {
            EndExportProgress(vm);
        }
    }

    private async void OnExportConstructBrowserElementSummaryClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanExportConstructBrowserElementSummary)
        {
            vm.StatusMessage = "No Construct Browser element data loaded.";
            return;
        }

        IReadOnlyList<ulong> selectedElementIds = GetSelectedElementIdsFromConstructBrowserGrid();
        var optionsDialog = new ElementTypeSummaryExportDialog("Construct Browser Elements JSON", selectedElementIds.Count);
        bool confirmed = await optionsDialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        IReadOnlyCollection<ulong>? exportElementIds = optionsDialog.UseSelectedRowsOnly
            ? selectedElementIds
            : null;

        await RunElementSummaryExportAsync(
            vm,
            "Export: preparing construct browser summary",
            "Construct Browser element summary export ready.",
            static (targetVm, message) => targetVm.StatusMessage = message,
            "Construct Browser elements export failed",
            "Construct Browser Elements JSON",
            ct => vm.ExportLoadedElementTypeCountsJsonAsync(
                optionsDialog.UseDisplayNameField,
                exportElementIds,
                ct));
    }

    private async void OnExportConstructVoxelMaterialsJsonClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await RunConstructVoxelDataAsync(vm, showDialog: true, applyToGrid: false);
    }

    private async void OnExportConstructVoxelAnalysisJsonClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanRepairDestroyedElements)
        {
            vm.StatusMessage = "No loaded DB construct snapshot available for voxel analysis.";
            return;
        }

        await RunElementSummaryExportAsync(
            vm,
            "Export: analyzing construct voxel blobs",
            "Construct voxel analysis export ready.",
            static (targetVm, message) => targetVm.StatusMessage = message,
            "Construct voxel analysis export failed",
            "Construct Voxel Analysis JSON",
            ct => vm.ExportLoadedConstructVoxelAnalysisJsonAsync(ct),
            timeoutSeconds: 120,
            exportProgressText: "Export: analyzing voxel blobs",
            showDialogHandler: json => ShowVoxelAnalysisDialogAsync(json, "Construct Voxel Analysis"));
    }

    private async void OnShowConstructVoxelDataClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await RunConstructVoxelDataAsync(vm, showDialog: false, applyToGrid: true);
    }

    private async Task RunConstructVoxelDataAsync(MainWindowViewModel vm, bool showDialog, bool applyToGrid)
    {
        if (!vm.CanRepairDestroyedElements)
        {
            vm.StatusMessage = "No loaded DB construct snapshot available for voxel material summary.";
            return;
        }

        await RunElementSummaryExportAsync(
            vm,
            "Export: analyzing construct voxel materials",
            showDialog ? "Construct voxel material summary ready." : "Construct voxel data loaded.",
            static (targetVm, message) => targetVm.StatusMessage = message,
            "Construct voxel material summary export failed",
            "Construct Voxel Materials JSON",
            ct => vm.ExportLoadedConstructVoxelMaterialSummaryJsonAsync(ct),
            timeoutSeconds: 90,
            onExportJsonReady: applyToGrid
                ? static (targetVm, json) => targetVm.ApplyVoxelMaterialSummaryToGrid(json)
                : null,
            showDialog: showDialog,
            refreshConstructBrowserGrid: applyToGrid);
    }

    private async void OnExportConstructBrowserBothJsonClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanExportConstructBrowserElementSummary)
        {
            vm.StatusMessage = "No Construct Browser element data loaded.";
            return;
        }

        if (!vm.CanRepairDestroyedElements)
        {
            vm.StatusMessage = "No loaded DB construct snapshot available for voxel material summary.";
            return;
        }

        IReadOnlyList<ulong> selectedElementIds = GetSelectedElementIdsFromConstructBrowserGrid();
        var optionsDialog = new ElementTypeSummaryExportDialog("Construct Browser Elements JSON", selectedElementIds.Count);
        bool confirmed = await optionsDialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        IReadOnlyCollection<ulong>? exportElementIds = optionsDialog.UseSelectedRowsOnly
            ? selectedElementIds
            : null;

        string voxelJson = string.Empty;
        await RunElementSummaryExportAsync(
            vm,
            "Export: preparing construct elements + voxels",
            "Construct elements + voxel summary export ready.",
            static (targetVm, message) => targetVm.StatusMessage = message,
            "Construct elements + voxel summary export failed",
            "Construct Elements + Voxels JSON",
            async ct =>
            {
                string elementsJson = await vm.ExportLoadedElementTypeCountsJsonAsync(
                    optionsDialog.UseDisplayNameField,
                    exportElementIds,
                    ct);
                voxelJson = await vm.ExportLoadedConstructVoxelMaterialSummaryJsonAsync(ct);
                return BuildCombinedExportJson(elementsJson, voxelJson);
            },
            timeoutSeconds: 120,
            onExportJsonReady: null,
            showDialog: true,
            refreshConstructBrowserGrid: false);
    }

    private async void OnExportBlueprintElementSummaryClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        if (!vm.CanExportBlueprintElementSummary)
        {
            vm.BlueprintsStatus = "No loaded blueprint data available for export.";
            return;
        }

        ulong[] allBlueprintIds = GetAllLoadedBlueprintIds(vm);
        if (allBlueprintIds.Length == 0)
        {
            vm.BlueprintsStatus = "No blueprint rows available for export.";
            return;
        }

        IReadOnlyList<ulong> selectedBlueprintIds = GetSelectedBlueprintIds(vm.SelectedBlueprint);
        var optionsDialog = new ElementTypeSummaryExportDialog("Blueprint Elements JSON", selectedBlueprintIds.Count);
        bool confirmed = await optionsDialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        IReadOnlyCollection<ulong> exportBlueprintIds = optionsDialog.UseSelectedRowsOnly
            ? selectedBlueprintIds
            : allBlueprintIds;

        await RunElementSummaryExportAsync(
            vm,
            "Export: preparing blueprint element summary",
            "Blueprint element summary export ready.",
            static (targetVm, message) => targetVm.BlueprintsStatus = message,
            "Blueprint elements export failed",
            "Blueprint Elements JSON",
            ct => vm.ExportBlueprintElementTypeCountsJsonAsync(
                exportBlueprintIds,
                optionsDialog.UseDisplayNameField,
                ct));
    }

    private async void OnExportBlueprintVoxelMaterialsJsonClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await RunBlueprintVoxelDataAsync(vm, showDialog: true, applyToGrid: false);
    }

    private async void OnShowBlueprintVoxelDataClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await RunBlueprintVoxelDataAsync(vm, showDialog: false, applyToGrid: true);
    }

    private async void OnAnalyzeBlueprintVoxelJsonFileClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        IStorageFile? selectedFile = await PickBlueprintJsonFileAsync(vm);
        if (selectedFile is null)
        {
            return;
        }

        await RunElementSummaryExportAsync(
            vm,
            "Export: reading blueprint JSON file",
            "Blueprint voxel analysis from file ready.",
            static (targetVm, message) => targetVm.BlueprintsStatus = message,
            "Blueprint voxel analysis failed",
            "Blueprint Voxel Analysis JSON",
            ct => BuildBlueprintVoxelAnalysisFromStorageFileAsync(vm, selectedFile, ct),
            timeoutSeconds: 120,
            exportProgressText: "Export: analyzing voxel blobs",
            showDialogHandler: json => ShowVoxelAnalysisDialogAsync(json, "Blueprint Voxel Analysis"));
    }

    private async Task RunBlueprintVoxelDataAsync(MainWindowViewModel vm, bool showDialog, bool applyToGrid)
    {
        if (!vm.CanEditBlueprint || vm.SelectedBlueprint is null)
        {
            vm.BlueprintsStatus = "No blueprint selected or database offline.";
            return;
        }

        await RunElementSummaryExportAsync(
            vm,
            "Export: analyzing blueprint voxel materials",
            showDialog ? "Blueprint voxel material summary ready." : "Blueprint voxel data loaded.",
            static (targetVm, message) => targetVm.BlueprintsStatus = message,
            "Blueprint voxel material summary export failed",
            "Blueprint Voxel Materials JSON",
            ct => vm.ExportSelectedBlueprintVoxelMaterialSummaryJsonAsync(ct),
            timeoutSeconds: 90,
            onExportJsonReady: applyToGrid
                ? static (targetVm, json) => targetVm.ApplyVoxelMaterialSummaryToGrid(json)
                : null,
            showDialog: showDialog,
            refreshConstructBrowserGrid: applyToGrid);
    }

    private async Task<string> BuildBlueprintVoxelAnalysisFromStorageFileAsync(
        MainWindowViewModel vm,
        IStorageFile selectedFile,
        CancellationToken cancellationToken)
    {
        await using Stream readStream = await selectedFile.OpenReadAsync();
        using var reader = new StreamReader(readStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        string json = await reader.ReadToEndAsync(cancellationToken);
        string sourceName = ResolveStorageFilePath(selectedFile);
        UpdateLastUsedFolder(vm, sourceName);
        return await vm.ExportBlueprintVoxelAnalysisFromJsonAsync(json, sourceName, cancellationToken);
    }

    private async Task<IStorageFile?> PickBlueprintJsonFileAsync(MainWindowViewModel vm)
    {
        var options = new FilePickerOpenOptions
        {
            Title = "Select Blueprint JSON file",
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

        IReadOnlyList<IStorageFile> selected = await StorageProvider.OpenFilePickerAsync(options);
        return selected.Count > 0 ? selected[0] : null;
    }

    private async void OnExportBlueprintBothJsonClick(object? sender, RoutedEventArgs e)
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

        ulong blueprintId = vm.SelectedBlueprint.Id;
        await RunElementSummaryExportAsync(
            vm,
            "Export: preparing blueprint voxels + analysis",
            "Blueprint voxels + analysis export ready.",
            static (targetVm, message) => targetVm.BlueprintsStatus = message,
            "Blueprint voxels + analysis export failed",
            "Blueprint Voxels + Analysis JSON",
            async ct =>
            {
                string voxelsJson = await vm.ExportSelectedBlueprintVoxelMaterialSummaryJsonAsync(ct);
                string blueprintJson = await vm.ExportSelectedBlueprintJsonAsync(
                    excludeVoxels: false,
                    excludeElementsAndLinks: false,
                    ct);
                string analysisJson = await vm.ExportBlueprintVoxelAnalysisFromJsonAsync(
                    blueprintJson,
                    $"blueprint_{blueprintId.ToString(CultureInfo.InvariantCulture)}.json",
                    ct);
                return BuildVoxelAndAnalysisExportJson(voxelsJson, analysisJson);
            },
            timeoutSeconds: 150,
            exportProgressText: "Export: analyzing blueprint voxels + voxel blobs",
            onExportJsonReady: null,
            showDialog: true,
            refreshConstructBrowserGrid: false);
    }

    private async void OnOpenBlueprintInConstructBrowserClick(object? sender, RoutedEventArgs e)
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

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            bool opened = await vm.OpenSelectedBlueprintInConstructBrowserAsync(cts.Token);
            if (opened)
            {
                ConstructDataTabControl.SelectedIndex = 0;
            }
        }
        catch (Exception ex)
        {
            vm.BlueprintsStatus = $"Open failed: {ex.Message}";
        }
    }

    private async void OnExpandAllElementPropertiesClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.ExpandAllElementPropertiesCommand.Execute(null),
            "Expanding Construct Browser tree...");
    }

    private async void OnCollapseAllElementPropertiesClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.CollapseAllElementPropertiesCommand.Execute(null),
            "Collapsing Construct Browser tree...");
    }

    private void OnElementPropertiesGridPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        _ = sender;

        if (e.ClickCount != 2 || DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        Dispatcher.UIThread.Post(() =>
        {
            if (!TryResolvePropertyTreeRow(ElementPropertiesGrid.SelectedItem, out PropertyTreeRow? row) ||
                row?.ElementId is not ulong elementId ||
                elementId == 0UL)
            {
                return;
            }

            vm.TrySelectElementCodeBlockTab(elementId);
        }, DispatcherPriority.Background);
    }

    private async void OnElementPropertiesGridKeyDown(object? sender, KeyEventArgs e)
    {
        _ = sender;

        if (DataContext is not MainWindowViewModel vm ||
            e.Key != Key.C ||
            !e.KeyModifiers.HasFlag(KeyModifiers.Control))
        {
            return;
        }

        if (!TryResolvePropertyTreeRow(ElementPropertiesGrid.SelectedItem, out PropertyTreeRow? row) ||
            row is null)
        {
            return;
        }

        string copyText = !string.IsNullOrWhiteSpace(row.ValuePreview)
            ? row.ValuePreview
            : row.FullContent;
        if (string.IsNullOrWhiteSpace(copyText))
        {
            return;
        }

        IClipboard? clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
        if (clipboard is null)
        {
            vm.StatusMessage = "Clipboard unavailable.";
            return;
        }

        try
        {
            await ClipboardExtensions.SetTextAsync(clipboard, copyText);
            vm.StatusMessage = "Construct Browser preview copied to clipboard.";
            e.Handled = true;
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"Copy failed: {ex.Message}";
        }
    }

    private async void OnExpandAllLuaBlocksClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.ExpandAllLuaBlocksCommand.Execute(null),
            "Expanding LUA blocks tree...");
    }

    private async void OnCollapseAllLuaBlocksClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.CollapseAllLuaBlocksCommand.Execute(null),
            "Collapsing LUA blocks tree...");
    }

    private async void OnExpandAllHtmlRsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.ExpandAllHtmlRsCommand.Execute(null),
            "Expanding HTML/RS tree...");
    }

    private async void OnCollapseAllHtmlRsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.CollapseAllHtmlRsCommand.Execute(null),
            "Collapsing HTML/RS tree...");
    }

    private async void OnExpandAllDatabankClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.ExpandAllDatabankCommand.Execute(null),
            "Expanding Databank tree...");
    }

    private async void OnCollapseAllDatabankClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        await ExecuteWithWaitCursorAsync(
            vm => vm.CollapseAllDatabankCommand.Execute(null),
            "Collapsing Databank tree...");
    }

    private async Task ExecuteWithWaitCursorAsync(
        Action<MainWindowViewModel> executeCommand,
        string? inProgressStatusMessage = null)
    {
        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        Cursor? previousCursor = Cursor;
        string previousStatusMessage = vm.StatusMessage;
        string statusMessage = inProgressStatusMessage?.Trim() ?? string.Empty;
        Cursor = new Cursor(StandardCursorType.Wait);
        try
        {
            if (!string.IsNullOrWhiteSpace(statusMessage))
            {
                vm.StatusMessage = statusMessage;
            }

            await Dispatcher.UIThread.InvokeAsync(static () => { }, DispatcherPriority.Render);
            executeCommand(vm);
        }
        finally
        {
            if (!string.IsNullOrWhiteSpace(statusMessage) &&
                string.Equals(vm.StatusMessage, statusMessage, StringComparison.Ordinal))
            {
                vm.StatusMessage = previousStatusMessage;
            }

            Cursor = previousCursor;
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

        await RunImportBlueprintWorkflowAsync(vm, openVoxelAnalysisAfterImport: false);
    }

    private async void OnImportBlueprintAndAnalyzeVoxelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        await RunImportBlueprintWorkflowAsync(vm, openVoxelAnalysisAfterImport: true);
    }

    private async Task RunImportBlueprintWorkflowAsync(
        MainWindowViewModel vm,
        bool openVoxelAnalysisAfterImport)
    {
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

            string jsonContent;
            using var importCts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
            if (Path.IsPathRooted(sourcePath) && File.Exists(sourcePath))
            {
                jsonContent = await File.ReadAllTextAsync(sourcePath, importCts.Token);
            }
            else if (selectedFile is not null)
            {
                await using Stream readStream = await selectedFile.OpenReadAsync();
                using var reader = new StreamReader(readStream, Encoding.UTF8, true);
                jsonContent = await reader.ReadToEndAsync(importCts.Token);
            }
            else
            {
                throw new InvalidOperationException("Selected blueprint file is unavailable.");
            }

            await vm.ImportBlueprintJsonAsync(
                jsonContent,
                sourcePath,
                vm.BlueprintImportDryRunMode,
                vm.BlueprintImportIntoApp,
                vm.BlueprintImportIntoGameDatabase,
                vm.BlueprintImportAppendDateIfExists,
                importCts.Token);

            if (!openVoxelAnalysisAfterImport)
            {
                return;
            }

            using var analysisCts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            vm.BlueprintsStatus = "Analyzing imported blueprint voxels...";
            string analysisJson = await vm.ExportBlueprintVoxelAnalysisFromJsonAsync(
                jsonContent,
                sourcePath,
                analysisCts.Token);
            vm.BlueprintsStatus = "Blueprint import + voxel analysis ready.";
            await ShowVoxelAnalysisDialogAsync(analysisJson, "Blueprint Voxel Analysis");
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
            !vm.TryGetStatusDetails(out string title, out string details))
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

    private async void OnRefreshLuaClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await vm.RefreshLuaDisplayAsync(cts.Token);
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"LUA refresh failed: {ex.Message}";
        }
    }

    private async void OnRefreshHtmlRsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await vm.RefreshHtmlRsDisplayAsync(cts.Token);
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"HTML/RS refresh failed: {ex.Message}";
        }
    }

    private async void OnRefreshDatabankClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await vm.RefreshDatabankDisplayAsync(cts.Token);
        }
        catch (Exception ex)
        {
            vm.StatusMessage = $"Databank refresh failed: {ex.Message}";
        }
    }

    private async void OnClearDatabankClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        string elementLabel = TryResolvePropertyTreeRow(vm.SelectedDatabankNode, out PropertyTreeRow? selectedRow)
            ? selectedRow?.NodeLabel ?? "selected databank"
            : "selected databank";
        string prompt = $"Clear databank contents for '{elementLabel}'?";
        const string details = "This creates a live local backup from the current DB value, overwrites the databank payload in the database with an empty table, and then reloads the live construct snapshot.";
        var dialog = new ConfirmationDialog(
            "Clear Databank",
            prompt,
            "Clear DB",
            "Cancel",
            detailsText: details);
        bool confirmed = await dialog.ShowDialog<bool>(this);
        if (!confirmed)
        {
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            LuaBackupEntry? backupEntry = await vm.ClearSelectedDatabankAsync(
                async (backupRequest, token) =>
                {
                    try
                    {
                        return await _luaBackupService.CreateBackupAsync(backupRequest, token);
                    }
                    catch (Exception backupEx)
                    {
                        throw new InvalidOperationException($"Backup creation failed: {backupEx.Message}", backupEx);
                    }
                },
                cts.Token);
            vm.StatusMessage = backupEntry is null
                ? $"Cleared databank '{elementLabel}'."
                : $"Cleared databank '{elementLabel}' with backup {backupEntry.FileName}.";
        }
        catch (Exception ex)
        {
            string message = ex.Message.StartsWith("Backup creation failed:", StringComparison.Ordinal)
                ? $"Clear databank blocked: {ex.Message}"
                : $"Clear databank failed: {ex.Message}";
            vm.StatusMessage = message;
        }
    }

    private async void OnDatabankBackupsClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        try
        {
            string currentContent =
                vm.TryGetSelectedDatabankBackupRequest(out LuaBackupCreateRequest? currentRequest) &&
                currentRequest is not null
                    ? currentRequest.Content
                    : string.Empty;
            var dialog = new LuaBackupManagerDialog(
                _luaBackupService,
                currentContent,
                LuaBackupManagerDialog.CreateDatabankOptions());
            BackupManagerDialogResult? result = await dialog.ShowDialog<BackupManagerDialogResult?>(this);
            if (result is null)
            {
                return;
            }

            if (result.ContentKind != BackupContentKind.Databank ||
                !result.ElementId.HasValue ||
                result.ElementId.Value == 0UL)
            {
                vm.StatusMessage = "Selected backup cannot be restored because it does not contain a databank target.";
                return;
            }

            if (LuaBackupService.LooksLikeCorruptedDatabankBackupText(result.Content))
            {
                vm.StatusMessage = "Selected databank backup looks corrupted by the old backup decoding bug and cannot be restored safely.";
                return;
            }

            string targetLabel = string.IsNullOrWhiteSpace(result.NodeLabel)
                ? string.IsNullOrWhiteSpace(result.ElementDisplayName)
                    ? $"element {result.ElementId.Value.ToString(CultureInfo.InvariantCulture)}"
                    : result.ElementDisplayName
                : result.NodeLabel;
            string restorePrompt = $"Restore the selected backup to databank '{targetLabel}'?";
            string restoreDetails =
                $"This creates a live local backup of the current DB value for element {result.ElementId.Value.ToString(CultureInfo.InvariantCulture)}, restores the selected backup content, and reloads the live construct snapshot.";
            var confirmDialog = new ConfirmationDialog(
                "Restore Databank Backup",
                restorePrompt,
                "Restore",
                "Cancel",
                detailsText: restoreDetails);
            bool confirmed = await confirmDialog.ShowDialog<bool>(this);
            if (!confirmed)
            {
                return;
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            LuaBackupEntry? liveBackupEntry = await vm.RestoreDatabankBackupAsync(
                result,
                async (backupRequest, token) =>
                {
                    try
                    {
                        return await _luaBackupService.CreateBackupAsync(backupRequest, token);
                    }
                    catch (Exception backupEx)
                    {
                        throw new InvalidOperationException($"Backup creation failed: {backupEx.Message}", backupEx);
                    }
                },
                cts.Token);
            vm.StatusMessage = liveBackupEntry is null
                ? $"Restored databank backup to '{targetLabel}'."
                : $"Restored databank backup to '{targetLabel}' with current value backed up as {liveBackupEntry.FileName}.";
        }
        catch (Exception ex)
        {
            string message = ex.Message.StartsWith("Backup creation failed:", StringComparison.Ordinal)
                ? $"Restore databank backup blocked: {ex.Message}"
                : $"Restore databank backup failed: {ex.Message}";
            vm.StatusMessage = message;
        }
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

    private static void BeginExportProgress(MainWindowViewModel vm, string text, double percent)
    {
        vm.ExportInProgress = true;
        vm.ExportProgressText = string.IsNullOrWhiteSpace(text) ? "Export: running" : text;
        vm.ExportProgressPercent = ClampExportPercent(percent);
    }

    private static void SetExportProgress(MainWindowViewModel vm, string text, double percent)
    {
        vm.ExportProgressText = string.IsNullOrWhiteSpace(text) ? "Export: running" : text;
        vm.ExportProgressPercent = ClampExportPercent(percent);
    }

    private static void EndExportProgress(MainWindowViewModel vm)
    {
        vm.ExportInProgress = false;
        vm.ExportProgressPercent = 0d;
        vm.ExportProgressText = "Export: idle";
    }

    private static ulong[] GetAllLoadedBlueprintIds(MainWindowViewModel vm)
    {
        return vm.Blueprints
            .Select(static bp => bp.Id)
            .Where(static id => id > 0UL)
            .Distinct()
            .ToArray();
    }

    private static string BuildCombinedExportJson(string elementsJson, string voxelsJson)
    {
        var root = new JsonObject
        {
            ["elements"] = ParseJsonNodeOrString(elementsJson),
            ["voxels"] = ParseJsonNodeOrString(voxelsJson)
        };

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true
        };
        return root.ToJsonString(jsonOptions);
    }

    private static string BuildVoxelAndAnalysisExportJson(string voxelsJson, string voxelAnalysisJson)
    {
        var root = new JsonObject
        {
            ["voxels"] = ParseJsonNodeOrString(voxelsJson),
            ["voxelAnalysis"] = ParseJsonNodeOrString(voxelAnalysisJson)
        };

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true
        };
        return root.ToJsonString(jsonOptions);
    }

    private static JsonNode ParseJsonNodeOrString(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return JsonValue.Create(string.Empty)!;
        }

        try
        {
            return JsonNode.Parse(json) ?? JsonValue.Create(json)!;
        }
        catch
        {
            return JsonValue.Create(json)!;
        }
    }

    private async Task ShowVoxelAnalysisDialogAsync(string json, string title)
    {
        var dialog = new VoxelAnalysisDialog(json)
        {
            Title = title
        };
        await dialog.ShowDialog(this);
    }

    private async Task RefreshConstructBrowserGridAsync()
    {
        await Dispatcher.UIThread.InvokeAsync(() =>
        {
            ElementPropertiesGrid.InvalidateMeasure();
            ElementPropertiesGrid.InvalidateArrange();
            ElementPropertiesGrid.InvalidateVisual();
        }, DispatcherPriority.Background);

        await Dispatcher.UIThread.InvokeAsync(static () => { }, DispatcherPriority.Render);
    }

    private async Task RunElementSummaryExportAsync(
        MainWindowViewModel vm,
        string prepareProgressText,
        string successStatusText,
        Action<MainWindowViewModel, string> setStatus,
        string failureStatusPrefix,
        string dialogTitle,
        Func<CancellationToken, Task<string>> exportFactory,
        int timeoutSeconds = 30,
        Action<MainWindowViewModel, string>? onExportJsonReady = null,
        bool showDialog = true,
        bool refreshConstructBrowserGrid = false,
        string exportProgressText = "Export: aggregating element counts",
        Func<string, string>? dialogJsonTransform = null,
        Func<string, Task>? showDialogHandler = null)
    {
        try
        {
            vm.LastStatusErrorDetails = string.Empty;
            BeginExportProgress(vm, prepareProgressText, 10d);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
            SetExportProgress(vm, exportProgressText, 40d);
            string json = await exportFactory(cts.Token);
            onExportJsonReady?.Invoke(vm, json);
            if (refreshConstructBrowserGrid)
            {
                await RefreshConstructBrowserGridAsync();
            }
            SetExportProgress(vm, "Export: formatting result", 85d);

            setStatus(vm, successStatusText);
            vm.LastStatusErrorDetails = string.Empty;
            SetExportProgress(vm, "Export: ready", 100d);
            if (showDialog)
            {
                if (showDialogHandler is not null)
                {
                    await showDialogHandler(json);
                }
                else
                {
                    string dialogJson = dialogJsonTransform is null ? json : dialogJsonTransform(json);
                    var dialog = new ExportJsonDialog(dialogJson)
                    {
                        Title = dialogTitle
                    };
                    await dialog.ShowDialog(this);
                }
            }
        }
        catch (Exception ex)
        {
            vm.LastStatusErrorDetails = ex.ToString();
            setStatus(vm, $"{failureStatusPrefix}: {ex.Message}");
        }
        finally
        {
            EndExportProgress(vm);
        }
    }

    private static double ClampExportPercent(double percent)
    {
        if (percent < 0d)
        {
            return 0d;
        }

        if (percent > 100d)
        {
            return 100d;
        }

        return percent;
    }

    private IReadOnlyList<ulong> GetSelectedElementIdsFromConstructBrowserGrid()
    {
        var selectedIds = new HashSet<ulong>();
        foreach (object? selected in EnumerateGridSelectedItems(ElementPropertiesGrid))
        {
            if (!TryResolvePropertyTreeRow(selected, out PropertyTreeRow? row) ||
                row?.ElementId is not ulong elementId ||
                elementId == 0UL)
            {
                continue;
            }

            selectedIds.Add(elementId);
        }

        if (selectedIds.Count == 0 &&
            TryResolvePropertyTreeRow(ElementPropertiesGrid.SelectedItem, out PropertyTreeRow? fallbackRow) &&
            fallbackRow?.ElementId is ulong fallbackId &&
            fallbackId > 0UL)
        {
            selectedIds.Add(fallbackId);
        }

        return selectedIds.OrderBy(static id => id).ToArray();
    }

    private IReadOnlyList<ulong> GetSelectedBlueprintIds(BlueprintDbRecord? fallback)
    {
        var selectedIds = new HashSet<ulong>();
        foreach (object? selected in EnumerateGridSelectedItems(BlueprintsGrid))
        {
            if (selected is not BlueprintDbRecord row || row.Id == 0UL)
            {
                continue;
            }

            selectedIds.Add(row.Id);
        }

        if (selectedIds.Count == 0 && fallback is not null && fallback.Id > 0UL)
        {
            selectedIds.Add(fallback.Id);
        }

        return selectedIds.OrderBy(static id => id).ToArray();
    }

    private IReadOnlyList<BlueprintDbRecord> GetSelectedBlueprintRows(BlueprintDbRecord? fallback)
    {
        var rows = new List<BlueprintDbRecord>();
        var selectedIds = new HashSet<ulong>();
        foreach (object? selected in EnumerateGridSelectedItems(BlueprintsGrid))
        {
            if (selected is not BlueprintDbRecord row || row.Id == 0UL || !selectedIds.Add(row.Id))
            {
                continue;
            }

            rows.Add(row);
        }

        if (rows.Count == 0 && fallback is not null && fallback.Id > 0UL && selectedIds.Add(fallback.Id))
        {
            rows.Add(fallback);
        }

        return rows;
    }

    private static string BuildBlueprintDeleteDetails(IReadOnlyList<BlueprintDbRecord> blueprints)
    {
        const int maxPreviewCount = 8;

        IEnumerable<string> previewLines = blueprints
            .Take(maxPreviewCount)
            .Select(bp =>
            {
                string blueprintName = string.IsNullOrWhiteSpace(bp.Name) ? "(unnamed)" : bp.Name.Trim();
                return $"ID {bp.Id.ToString(CultureInfo.InvariantCulture)} | {blueprintName}";
            });

        string details = string.Join(Environment.NewLine, previewLines);
        if (blueprints.Count > maxPreviewCount)
        {
            details += Environment.NewLine +
                       $"+ {blueprints.Count - maxPreviewCount} more selected blueprint(s)";
        }

        return details;
    }

    private static IEnumerable<object?> EnumerateGridSelectedItems(DataGrid grid)
    {
        if (grid.SelectedItems is not IList selectedItems || selectedItems.Count == 0)
        {
            return Array.Empty<object?>();
        }

        return selectedItems.Cast<object?>();
    }

    private static bool TryResolvePropertyTreeRow(object? selected, out PropertyTreeRow? row)
    {
        switch (selected)
        {
            case HierarchicalNode<PropertyTreeRow> typedNode when typedNode.Item is not null:
                row = typedNode.Item;
                return true;
            case HierarchicalNode node when node.Item is PropertyTreeRow untypedRow:
                row = untypedRow;
                return true;
            case PropertyTreeRow directRow:
                row = directRow;
                return true;
            default:
                row = null;
                return false;
        }
    }
}
