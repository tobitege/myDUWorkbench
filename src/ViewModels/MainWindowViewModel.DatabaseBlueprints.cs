using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private async Task InitializeStartupAsync()
    {
        try
        {
            await ProbeDatabaseAvailabilityAsync(CancellationToken.None);
            RefreshAutoConnectLoopState();

            if (!string.Equals(DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (_startupAutoLoadPending && AutoLoadOnStartup)
            {
                await LoadDatabaseAsync();
            }

            if (AutoLoadPlayerNames)
            {
                QueuePlayerNameCacheRefresh(forceReload: false);
            }
            else if (CountSearchCharacters(PlayerNameSearchInput) >= 3)
            {
                QueuePlayerNameSearch(PlayerNameSearchInput);
            }

            if (CountSearchCharacters(ConstructNameSearchInput) >= 3 || TryGetScopedPlayerId(out _))
            {
                QueueConstructNameSearch(ConstructNameSearchInput);
            }
        }
        finally
        {
            _isStartupInitializing = false;
            RefreshAutoConnectLoopState();
        }
    }

    [RelayCommand]
    private async Task ConnectDatabaseAsync()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        bool connected = await TryConnectDatabaseAsync(cts.Token, initiatedByAutoConnect: false);
        if (!connected)
        {
            StatusMessage = "Database is offline.";
        }
    }

    [RelayCommand]
    private async Task LoadBlueprintsAsync()
    {
        if (IsBusy)
        {
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        try
        {
            IsBusy = true;
            BlueprintsStatus = "Loading blueprints...";
            DataConnectionOptions options = BuildDbOptions();
            IReadOnlyList<BlueprintDbRecord> records = await _dataService.GetBlueprintsAsync(
                options,
                BlueprintNameFilter,
                GetBlueprintCreatorFilterPlayerId(),
                cts.Token);

            Blueprints.Clear();
            foreach (BlueprintDbRecord record in records)
            {
                Blueprints.Add(record);
            }

            BlueprintsStatus = $"{records.Count} blueprint(s) loaded.";
        }
        catch (Exception ex)
        {
            BlueprintsStatus = $"Load failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    public Task DeleteBlueprintAsync(CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            return Task.CompletedTask;
        }

        return DeleteBlueprintsAsync(new[] { bp }, progress: null, cancellationToken);
    }

    public async Task DeleteBlueprintsAsync(
        IReadOnlyList<BlueprintDbRecord> blueprints,
        IProgress<BlueprintDeleteProgress>? progress,
        CancellationToken cancellationToken)
    {
        if (blueprints.Count == 0)
        {
            return;
        }

        int totalCount = blueprints.Count;
        int deletedCount = 0;
        int deletedBlueprintRows = 0;
        int deletedElementRows = 0;
        int deletedElementPropertyRows = 0;
        int voxelCleanupWarningCount = 0;

        try
        {
            IsBusy = true;
            DataConnectionOptions options = BuildDbOptions();

            for (int index = 0; index < blueprints.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                BlueprintDbRecord bp = blueprints[index];
                string blueprintName = string.IsNullOrWhiteSpace(bp.Name) ? "(unnamed)" : bp.Name.Trim();

                progress?.Report(new BlueprintDeleteProgress(index + 1, totalCount, bp.Id, blueprintName));
                BlueprintsStatus =
                    $"Deleting blueprint {index + 1}/{totalCount}: {bp.Id.ToString(CultureInfo.InvariantCulture)} | {blueprintName}";

                BlueprintDeleteResult result = await _dataService.DeleteBlueprintAsync(
                    options,
                    bp.Id,
                    EndpointTemplateInput,
                    BlueprintImportEndpointInput,
                    cancellationToken);

                if (result.BlueprintRowsDeleted <= 0)
                {
                    throw new InvalidOperationException(
                        $"Blueprint {bp.Id.ToString(CultureInfo.InvariantCulture)} ({blueprintName}) not found (already deleted?).");
                }

                deletedCount++;
                deletedBlueprintRows += result.BlueprintRowsDeleted;
                deletedElementRows += result.ElementRowsDeleted;
                deletedElementPropertyRows += result.ElementPropertyRowsDeleted;

                RemoveBlueprintById(bp.Id);

                if (result.VoxelCleanupAttempted && !result.VoxelCleanupSucceeded)
                {
                    voxelCleanupWarningCount++;
                }
            }

            SelectedBlueprint = null;

            string voxelSuffix = voxelCleanupWarningCount > 0
                ? $" Voxel cleanup warnings: {voxelCleanupWarningCount.ToString(CultureInfo.InvariantCulture)}."
                : string.Empty;
            BlueprintsStatus =
                $"Deleted {deletedCount.ToString(CultureInfo.InvariantCulture)}/{totalCount.ToString(CultureInfo.InvariantCulture)} blueprint(s) " +
                $"(rows: blueprint={deletedBlueprintRows.ToString(CultureInfo.InvariantCulture)}, " +
                $"element={deletedElementRows.ToString(CultureInfo.InvariantCulture)}, " +
                $"element_property={deletedElementPropertyRows.ToString(CultureInfo.InvariantCulture)}).{voxelSuffix}";
        }
        catch (OperationCanceledException)
        {
            BlueprintsStatus = deletedCount > 0
                ? $"Delete cancelled after {deletedCount.ToString(CultureInfo.InvariantCulture)}/{totalCount.ToString(CultureInfo.InvariantCulture)} blueprint(s)."
                : "Delete cancelled.";
            throw;
        }
        catch (Exception ex)
        {
            BlueprintsStatus = deletedCount > 0
                ? $"Delete failed after {deletedCount.ToString(CultureInfo.InvariantCulture)}/{totalCount.ToString(CultureInfo.InvariantCulture)} blueprint(s): {ex.Message}"
                : $"Delete failed: {ex.Message}";
            throw;
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(CanDeleteBlueprint));
            OnPropertyChanged(nameof(CanSaveBlueprint));
        }
    }

    private void RemoveBlueprintById(ulong blueprintId)
    {
        BlueprintDbRecord? existing = Blueprints.FirstOrDefault(row => row.Id == blueprintId);
        if (existing is not null)
        {
            Blueprints.Remove(existing);
        }
    }

    [RelayCommand]
    private async Task SaveBlueprintAsync()
    {
        if (SelectedBlueprint is not { } bp)
        {
            return;
        }

        if (!TryNormalizeBlueprintName(BlueprintEditName, out string normalizedName, out string nameError))
        {
            BlueprintsStatus = nameError;
            return;
        }

        long? maxUse = bp.MaxUse;
        if (BlueprintEditApplyMaxUse)
        {
            maxUse = BlueprintEditCoreBlueprint ? null : 1L;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            IsBusy = true;
            BlueprintsStatus = $"Saving blueprint {bp.Id}...";
            DataConnectionOptions options = BuildDbOptions();
            BlueprintUpdateResult result = await _dataService.UpdateBlueprintFieldsAsync(
                options,
                bp.Id,
                normalizedName,
                BlueprintEditFreeDeploy,
                bp.HasMaterials,
                BlueprintEditApplyMaxUse,
                maxUse,
                cts.Token);

            if (result.RowsUpdated <= 0)
            {
                BlueprintsStatus = $"Blueprint {bp.Id} not found or unchanged by DB operation.";
                return;
            }

            int idx = Blueprints.IndexOf(bp);
            BlueprintDbRecord updated = bp with
            {
                Name = normalizedName,
                FreeDeploy = BlueprintEditFreeDeploy,
                MaxUse = BlueprintEditApplyMaxUse ? maxUse : bp.MaxUse
            };
            if (idx >= 0)
            {
                Blueprints[idx] = updated;
            }
            SelectedBlueprint = updated;
            BlueprintsStatus = $"Blueprint {bp.Id} saved (rows updated: {result.RowsUpdated}).";
        }
        catch (Exception ex)
        {
            BlueprintsStatus = $"Save failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(CanDeleteBlueprint));
            OnPropertyChanged(nameof(CanSaveBlueprint));
        }
    }

    public async Task CopyBlueprintAsync(string requestedName, CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } source)
        {
            return;
        }

        if (!TryNormalizeBlueprintName(requestedName, out string normalizedName, out string nameError))
        {
            BlueprintsStatus = nameError;
            return;
        }

        try
        {
            IsBusy = true;
            BlueprintsStatus = $"Copying blueprint {source.Id}...";
            DataConnectionOptions options = BuildDbOptions();
            BlueprintCopyResult copyResult = await _dataService.CopyBlueprintAsync(
                options,
                source.Id,
                normalizedName,
                cancellationToken);

            if (copyResult.BlueprintRowsInserted <= 0 || !copyResult.CopiedBlueprintId.HasValue)
            {
                BlueprintsStatus = $"Copy failed: source blueprint {source.Id} not found.";
                return;
            }

            IReadOnlyList<BlueprintDbRecord> refreshed = await _dataService.GetBlueprintsAsync(
                options,
                BlueprintNameFilter,
                GetBlueprintCreatorFilterPlayerId(),
                cancellationToken);

            Blueprints.Clear();
            BlueprintDbRecord? inserted = null;
            foreach (BlueprintDbRecord record in refreshed)
            {
                Blueprints.Add(record);
                if (record.Id == copyResult.CopiedBlueprintId.Value)
                {
                    inserted = record;
                }
            }

            SelectedBlueprint = inserted;
            string copySuffix = string.IsNullOrWhiteSpace(copyResult.CopyNote)
                ? string.Empty
                : $" ({copyResult.CopyNote})";
            BlueprintsStatus =
                $"Blueprint copied: {source.Id} -> {copyResult.CopiedBlueprintId.Value} (element={copyResult.ElementRowsCopied}, element_property={copyResult.ElementPropertyRowsCopied}).{copySuffix}";
        }
        catch (Exception ex)
        {
            BlueprintsStatus = $"Copy failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(CanEditBlueprint));
            OnPropertyChanged(nameof(CanCopyBlueprint));
            OnPropertyChanged(nameof(CanDeleteBlueprint));
            OnPropertyChanged(nameof(CanSaveBlueprint));
        }
    }

    public async Task GiveSelectedBlueprintToPlayerAsync(bool singleUse, CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            BlueprintsStatus = "No blueprint selected.";
            return;
        }

        if (!TryResolveBlueprintTargetPlayerId(out ulong playerId))
        {
            BlueprintsStatus = "Select a valid player (Player ID > 0) before giving a blueprint.";
            return;
        }

        try
        {
            IsBusy = true;
            string modeLabel = singleUse ? "single-use" : "core";
            BlueprintsStatus = $"Giving blueprint {bp.Id} to player {playerId} ({modeLabel})...";

            DataConnectionOptions options = BuildDbOptions();
            BlueprintGrantResult result = await _dataService.GiveBlueprintToPlayerInventoryAsync(
                options,
                bp.Id,
                playerId,
                singleUse,
                cancellationToken);

            if (result.AlreadyPresent)
            {
                BlueprintsStatus = result.Note;
                return;
            }

            BlueprintsStatus =
                $"Blueprint {bp.Id} granted to player {playerId} as {(singleUse ? "SingleUseBlueprint" : "Blueprint")} in slot {result.SlotNumber.ToString(CultureInfo.InvariantCulture)}.";
        }
        catch (Exception ex)
        {
            BlueprintsStatus = $"Grant failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(CanGiveBlueprintToPlayer));
        }
    }

    public string? ValidateBlueprintNameInput(string? candidate)
    {
        return TryNormalizeBlueprintName(candidate, out _, out string error)
            ? null
            : error;
    }

    public async Task<string> ExportSelectedBlueprintJsonAsync(
        bool excludeVoxels,
        bool excludeElementsAndLinks,
        CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            throw new InvalidOperationException("No blueprint selected.");
        }

        DataConnectionOptions options = BuildDbOptions();
        return await _dataService.ExportBlueprintJsonAsync(
            options,
            bp.Id,
            EndpointTemplateInput,
            BlueprintImportEndpointInput,
            excludeVoxels,
            excludeElementsAndLinks,
            cancellationToken);
    }

    private bool TryResolveBlueprintTargetPlayerId(out ulong playerId)
    {
        if (SelectedPlayerNameSuggestion?.PlayerId is ulong selectedPlayerId && selectedPlayerId > 0UL)
        {
            playerId = selectedPlayerId;
            return true;
        }

        string raw = PlayerIdInput?.Trim() ?? string.Empty;
        if (ulong.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed) &&
            parsed > 0UL)
        {
            playerId = parsed;
            return true;
        }

        playerId = 0UL;
        return false;
    }

    public async Task<bool> OpenSelectedBlueprintInConstructBrowserAsync(CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            BlueprintsStatus = "No blueprint selected.";
            return false;
        }

        return await OpenBlueprintInConstructBrowserAsync(bp.Id, bp.Name, cancellationToken);
    }

    private async Task<bool> OpenBlueprintInConstructBrowserAsync(
        ulong blueprintId,
        string? blueprintName,
        CancellationToken cancellationToken)
    {
        if (blueprintId == 0UL)
        {
            StatusMessage = "No blueprint selected.";
            return false;
        }

        if (IsBusy)
        {
            return false;
        }

        ElementFilterSnapshot filterSnapshot = CaptureElementFilterSnapshot();

        try
        {
            IsBusy = true;
            string openingName = string.IsNullOrWhiteSpace(blueprintName)
                ? $"Blueprint {blueprintId.ToString(CultureInfo.InvariantCulture)}"
                : blueprintName.Trim();
            BlueprintsStatus = $"Opening blueprint {blueprintId} in Construct Browser...";
            StatusMessage = $"Opening blueprint {blueprintId} in Construct Browser...";
            DataConnectionOptions options = BuildDbOptions();

            string exportedJson = await _dataService.ExportBlueprintJsonAsync(
                options,
                blueprintId,
                EndpointTemplateInput,
                BlueprintImportEndpointInput,
                excludeVoxels: false,
                excludeElementsAndLinks: false,
                cancellationToken);

            BlueprintImportResult importResult = await Task.Run(
                () => _dataService.ParseBlueprintJson(
                    exportedJson,
                    $"blueprint_{blueprintId.ToString(CultureInfo.InvariantCulture)}",
                    ServerRootPathInput,
                    NqUtilsDllPathInput),
                cancellationToken);
            IReadOnlyList<ElementPropertyRecord> dbElementProperties =
                await _dataService.GetBlueprintElementPropertiesAsync(options, blueprintId, cancellationToken);
            if (dbElementProperties.Count > 0)
            {
                importResult = importResult with
                {
                    Properties = MergeBlueprintPropertyRecords(importResult.Properties, dbElementProperties)
                };
            }

            (IReadOnlyList<ElementPropertyRecord> enrichedProperties, int renamedCount) =
                await TryEnrichBlueprintElementDisplayNamesAsync(importResult.Properties, cancellationToken);
            if (renamedCount > 0)
            {
                importResult = importResult with { Properties = enrichedProperties };
            }

            ClearFiltersForBlueprintImport(applyFilter: false);
            _lastSnapshot = null;
            RefreshDamagedFilterAvailability();
            OnPropertyChanged(nameof(CanRepairDestroyedElements));

            ActiveConstructName = string.IsNullOrWhiteSpace(importResult.BlueprintName)
                ? $"{openingName} [BP {blueprintId.ToString(CultureInfo.InvariantCulture)}]"
                : $"{importResult.BlueprintName} [BP {blueprintId.ToString(CultureInfo.InvariantCulture)}]";

            await ApplyLoadedPropertyCollectionsAsync(
                importResult.Properties,
                cancellationToken,
                buildFilteredView: false);
            RestoreElementFilters(filterSnapshot, applyFilter: false);
            await ApplyElementPropertyFilterAsync(
                cancellationToken,
                progressUpdate: null,
                progressStart: 0d,
                progressEnd: 100d);
            UpdateDatabaseBlueprintSummary(importResult, blueprintId, openingName);

            string openMessage =
                $"Opened blueprint {blueprintId.ToString(CultureInfo.InvariantCulture)} in Construct Browser ({importResult.ElementCount.ToString(CultureInfo.InvariantCulture)} element(s)).";
            BlueprintsStatus = openMessage;
            StatusMessage = openMessage;
            return true;
        }
        catch (OperationCanceledException)
        {
            BlueprintsStatus = "Open blueprint cancelled.";
            StatusMessage = "Open blueprint cancelled.";
            return false;
        }
        catch (Exception ex)
        {
            RestoreElementFilters(filterSnapshot, applyFilter: false);
            BlueprintsStatus = $"Open failed: {ex.Message}";
            StatusMessage = $"DB load failed: {ex.Message}";
            return false;
        }
        finally
        {
            IsBusy = false;
        }
    }

    private static IReadOnlyList<ElementPropertyRecord> MergeBlueprintPropertyRecords(
        IReadOnlyList<ElementPropertyRecord> exportedProperties,
        IReadOnlyList<ElementPropertyRecord> dbElementProperties)
    {
        if (dbElementProperties.Count == 0)
        {
            return exportedProperties;
        }

        var merged = new Dictionary<string, ElementPropertyRecord>(StringComparer.OrdinalIgnoreCase);
        foreach (ElementPropertyRecord record in exportedProperties)
        {
            merged[BuildElementPropertyMergeKey(record)] = record;
        }

        foreach (ElementPropertyRecord record in dbElementProperties)
        {
            merged[BuildElementPropertyMergeKey(record)] = record;
        }

        return merged.Values
            .OrderBy(record => record.ElementId)
            .ThenBy(record => NormalizePropertyName(record.Name), StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string BuildElementPropertyMergeKey(ElementPropertyRecord record)
    {
        string normalizedName = NormalizePropertyName(record.Name);
        if (string.IsNullOrWhiteSpace(normalizedName))
        {
            normalizedName = record.Name ?? string.Empty;
        }

        return record.ElementId.ToString(CultureInfo.InvariantCulture) + "|" + normalizedName;
    }

    private static bool TryNormalizeBlueprintName(string? input, out string normalizedName, out string error)
    {
        normalizedName = string.Empty;
        error = string.Empty;

        if (string.IsNullOrEmpty(input))
        {
            error = "Name is required.";
            return false;
        }

        string trimmed = input.Trim();
        if (!string.Equals(input, trimmed, StringComparison.Ordinal))
        {
            error = "Name must not start or end with blanks.";
            return false;
        }

        if (trimmed.Length > BlueprintNameMaxLength)
        {
            error = $"Name must be at most {BlueprintNameMaxLength} characters.";
            return false;
        }

        normalizedName = trimmed;
        return true;
    }

    private bool IsBlueprintEditInputValid(out string normalizedName, out string error)
    {
        if (!TryNormalizeBlueprintName(BlueprintEditName, out normalizedName, out error))
        {
            return false;
        }

        error = string.Empty;
        return true;
    }

    private string BuildBlueprintEditValidationMessage()
    {
        if (SelectedBlueprint is null)
        {
            return string.Empty;
        }

        if (!CanEditBlueprint)
        {
            return string.Equals(DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase)
                ? "Editing is unavailable while another task is running."
                : "Database is offline.";
        }

        if (!TryNormalizeBlueprintName(BlueprintEditName, out _, out string nameError))
        {
            return nameError;
        }

        return string.Empty;
    }
}
