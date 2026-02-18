using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorker.Models;
using myDUWorker.Services;
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

namespace myDUWorker.ViewModels;

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

    public async Task DeleteBlueprintAsync(CancellationToken cancellationToken)
    {
        if (SelectedBlueprint is not { } bp)
        {
            return;
        }

        try
        {
            IsBusy = true;
            BlueprintsStatus = $"Deleting blueprint {bp.Id}...";
            DataConnectionOptions options = BuildDbOptions();
            BlueprintDeleteResult result = await _dataService.DeleteBlueprintAsync(
                options,
                bp.Id,
                EndpointTemplateInput,
                BlueprintImportEndpointInput,
                cancellationToken);

            if (result.BlueprintRowsDeleted <= 0)
            {
                BlueprintsStatus = $"Blueprint {bp.Id} not found (already deleted?).";
                return;
            }

            Blueprints.Remove(bp);
            SelectedBlueprint = null;

            string voxelSuffix = string.Empty;
            if (result.VoxelCleanupAttempted)
            {
                voxelSuffix = result.VoxelCleanupSucceeded
                    ? " Voxel cleanup: ok."
                    : $" Voxel cleanup warning: {result.VoxelCleanupNote}";
            }

            BlueprintsStatus =
                $"Blueprint {bp.Id} deleted (rows: blueprint={result.BlueprintRowsDeleted}, element={result.ElementRowsDeleted}, element_property={result.ElementPropertyRowsDeleted}).{voxelSuffix}";
        }
        catch (Exception ex)
        {
            BlueprintsStatus = $"Delete failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(CanDeleteBlueprint));
            OnPropertyChanged(nameof(CanSaveBlueprint));
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

    [RelayCommand]
    private async Task LoadDatabaseAsync()
    {
        if (IsBusy)
        {
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        try
        {
            IsBusy = true;
            StatusMessage = "Loading construct snapshot from PostgreSQL...";

            ulong? constructId = TryParseOptionalUlong(ConstructIdInput);
            ulong? playerId = TryParseOptionalUlong(PlayerIdInput);
            int propertyLimit = ParsePropertyLimit(PropertyLimitInput);
            DataConnectionOptions options = BuildDbOptions();

            DatabaseConstructSnapshot snapshot = await _dataService.LoadConstructSnapshotAsync(
                options,
                constructId,
                playerId,
                propertyLimit,
                cts.Token);

            _lastSnapshot = snapshot;
            OnPropertyChanged(nameof(CanRepairDestroyedElements));
            ActiveConstructName = string.IsNullOrWhiteSpace(snapshot.ConstructName)
                ? snapshot.ConstructId.ToString(CultureInfo.InvariantCulture)
                : snapshot.ConstructName;
            UpdateDatabaseSummary(snapshot);
            await ApplyLoadedPropertyCollectionsAsync(snapshot.Properties, cancellationToken: cts.Token);

            StatusMessage = $"DB snapshot loaded for construct {snapshot.ConstructId}.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"DB load failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    public async Task ImportBlueprintJsonAsync(
        string jsonContent,
        string sourceName,
        bool dryRunMode,
        bool importIntoApp,
        bool importIntoGameDatabase,
        bool appendDateIfExists,
        CancellationToken cancellationToken = default)
    {
        await ImportBlueprintCoreAsync(
            dryRunMode,
            importIntoApp,
            importIntoGameDatabase,
            () => _dataService.ParseBlueprintJson(jsonContent, sourceName, ServerRootPathInput, NqUtilsDllPathInput),
            ct => _dataService.ImportBlueprintIntoGameDatabaseAsync(
                jsonContent,
                EndpointTemplateInput,
                BlueprintImportEndpointInput,
                ParseOptionalUnsignedOrDefault(PlayerIdInput),
                creatorOrganizationId: 0UL,
                appendDateIfExists,
                BuildDbOptions(),
                ct),
            sourceName,
            cancellationToken);
    }

    public async Task ImportBlueprintFileAsync(
        string sourcePath,
        bool dryRunMode,
        bool importIntoApp,
        bool importIntoGameDatabase,
        bool appendDateIfExists,
        CancellationToken cancellationToken = default)
    {
        string fullSourcePath = Path.GetFullPath(sourcePath);
        await ImportBlueprintCoreAsync(
            dryRunMode,
            importIntoApp,
            importIntoGameDatabase,
            () => _dataService.ParseBlueprintJsonFile(fullSourcePath, fullSourcePath, ServerRootPathInput, NqUtilsDllPathInput),
            ct => _dataService.ImportBlueprintFileIntoGameDatabaseAsync(
                fullSourcePath,
                EndpointTemplateInput,
                BlueprintImportEndpointInput,
                ParseOptionalUnsignedOrDefault(PlayerIdInput),
                creatorOrganizationId: 0UL,
                appendDateIfExists,
                BuildDbOptions(),
                ct),
            fullSourcePath,
            cancellationToken);
    }

    private async Task ImportBlueprintCoreAsync(
        bool dryRunMode,
        bool importIntoApp,
        bool importIntoGameDatabase,
        Func<BlueprintImportResult> importFactory,
        Func<CancellationToken, Task<BlueprintGameDatabaseImportResult>>? gameDatabaseImportFactory,
        string? sourcePathHint,
        CancellationToken cancellationToken)
    {
        if (IsBusy)
        {
            return;
        }

        ElementFilterSnapshot filterSnapshot = CaptureElementFilterSnapshot();
        bool shouldApplyToApp = importIntoApp && !dryRunMode;
        bool requestGameDatabaseImport = importIntoGameDatabase;

        try
        {
            IsBusy = true;
            BlueprintImportInProgress = true;
            BlueprintImportProgressPercent = 0d;
            BlueprintImportProgressText = "Import: preparing";
            LastBlueprintImportErrorDetails = string.Empty;
            StatusMessage = dryRunMode
                ? "Validating blueprint JSON (dry run)..."
                : "Importing blueprint JSON...";

            if (shouldApplyToApp)
            {
                ClearFiltersForBlueprintImport(applyFilter: false);
            }

            SetBlueprintImportProgress(20d, "Import: parsing blueprint");
            BlueprintImportResult importResult = await Task.Run(importFactory, cancellationToken);
            SetBlueprintImportProgress(40d, "Import: parsed");
            string importSourcePath = ResolveBlueprintImportSourcePath(sourcePathHint, importResult.SourceName);

            string blueprintIdText = FormatBlueprintIdForDisplay(importResult.BlueprintId, "<none>");
            BlueprintGameDatabaseImportResult? gameDatabaseImportResult = null;

            if (shouldApplyToApp)
            {
                SetBlueprintImportProgress(50d, "Import: resolving element names");
                (IReadOnlyList<ElementPropertyRecord> enrichedProperties, int renamedCount) =
                    await TryEnrichBlueprintElementDisplayNamesAsync(importResult.Properties, cancellationToken);
                if (renamedCount > 0)
                {
                    importResult = importResult with { Properties = enrichedProperties };
                }

                _lastSnapshot = null;
                OnPropertyChanged(nameof(CanRepairDestroyedElements));

                ActiveConstructName = string.IsNullOrWhiteSpace(importResult.BlueprintName)
                    ? "Blueprint import"
                    : importResult.BlueprintName;

                SetBlueprintImportProgress(60d, "Import: updating property trees");
                await ApplyLoadedPropertyCollectionsAsync(
                    importResult.Properties,
                    cancellationToken,
                    (percent, text) => SetBlueprintImportProgress(percent, text),
                    buildFilteredView: false);
                SetBlueprintImportProgress(88d, "Import: applying filters");
                RestoreElementFilters(filterSnapshot, applyFilter: false);
                await ApplyElementPropertyFilterAsync(
                    cancellationToken,
                    (percent, text) => SetBlueprintImportProgress(percent, text),
                    progressStart: 88d,
                    progressEnd: 98d);
                UpdateBlueprintSummary(importResult);
            }

            if (requestGameDatabaseImport && !dryRunMode)
            {
                if (gameDatabaseImportFactory is null)
                {
                    throw new InvalidOperationException("Game DB import is not available for this blueprint source.");
                }

                SetBlueprintImportProgress(99d, "Import: writing to game database");
                gameDatabaseImportResult = await gameDatabaseImportFactory(cancellationToken);
                if (!NormalizeBlueprintId(importResult.BlueprintId).HasValue &&
                    NormalizeBlueprintId(gameDatabaseImportResult.BlueprintId).HasValue)
                {
                    blueprintIdText = FormatBlueprintIdForDisplay(gameDatabaseImportResult.BlueprintId, "<none>");
                }
            }

            string scope = shouldApplyToApp
                ? "imported into app"
                : "validated only";
            if (dryRunMode)
            {
                scope += " (dry run)";
            }

            string message =
                $"Blueprint {scope} ({importResult.ImportPipeline}): {importResult.ElementCount.ToString(CultureInfo.InvariantCulture)} elements, id={blueprintIdText}.";
            if (requestGameDatabaseImport)
            {
                if (dryRunMode)
                {
                    message += " Game DB import skipped (dry run).";
                }
                else if (gameDatabaseImportResult is not null)
                {
                    string importedId = FormatBlueprintIdForDisplay(gameDatabaseImportResult.BlueprintId, "<unknown>");
                    message +=
                        $" Game DB import OK (HTTP {gameDatabaseImportResult.StatusCode}, imported blueprintId={importedId}).";
                    if (!string.IsNullOrWhiteSpace(gameDatabaseImportResult.RequestNotes))
                    {
                        message += $" {gameDatabaseImportResult.RequestNotes}";
                    }

                    LastBlueprintImportErrorDetails =
                        BuildBlueprintImportDetailsWithSource(
                            importSourcePath,
                            $"Game DB endpoint: {gameDatabaseImportResult.Endpoint}{Environment.NewLine}" +
                            $"HTTP: {gameDatabaseImportResult.StatusCode}{Environment.NewLine}" +
                            $"Response: {gameDatabaseImportResult.ResponseText}{Environment.NewLine}" +
                            $"Request notes: {gameDatabaseImportResult.RequestNotes}");
                }
            }

            AppendImportNotesToStatus(importResult, importSourcePath, ref message);
            StatusMessage = message;
            SetBlueprintImportProgress(100d, "Import: completed");
        }
        catch (OperationCanceledException)
        {
            LastBlueprintImportErrorDetails = string.Empty;
            StatusMessage = "Blueprint import cancelled.";
        }
        catch (Exception ex)
        {
            if (shouldApplyToApp)
            {
                RestoreElementFilters(filterSnapshot, applyFilter: false);
            }

            string importSourcePath = ResolveBlueprintImportSourcePath(sourcePathHint);
            LastBlueprintImportErrorDetails = BuildBlueprintImportDetailsWithSource(importSourcePath, ex.ToString());
            StatusMessage = $"Blueprint import failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
            BlueprintImportInProgress = false;
            BlueprintImportProgressText = "Import: idle";
        }
    }

    public bool TryGetBlueprintImportErrorDetails(out string title, out string details)
    {
        const string failedPrefix = "Blueprint import failed:";
        bool isFailure = !string.IsNullOrWhiteSpace(StatusMessage) &&
            StatusMessage.StartsWith(failedPrefix, StringComparison.OrdinalIgnoreCase);
        bool hasImportDetails =
            !string.IsNullOrWhiteSpace(LastBlueprintImportErrorDetails) &&
            !string.IsNullOrWhiteSpace(StatusMessage) &&
            StatusMessage.StartsWith("Blueprint ", StringComparison.OrdinalIgnoreCase);
        if (!isFailure && !hasImportDetails)
        {
            title = string.Empty;
            details = string.Empty;
            return false;
        }

        title = isFailure ? "Blueprint Import Error" : "Blueprint Import Details";
        details = string.IsNullOrWhiteSpace(LastBlueprintImportErrorDetails)
            ? StatusMessage
            : LastBlueprintImportErrorDetails;
        return true;
    }

    public void SetBlueprintImportError(Exception ex)
    {
        LastBlueprintImportErrorDetails = ex?.ToString() ?? string.Empty;
        StatusMessage = $"Blueprint import failed: {ex?.Message ?? "unknown error"}";
    }

    private void SetBlueprintImportProgress(double percent, string text)
    {
        double clamped = percent < 0d ? 0d : percent > 100d ? 100d : percent;
        BlueprintImportProgressPercent = clamped;
        BlueprintImportProgressText = string.IsNullOrWhiteSpace(text) ? "Import: running" : text;
    }

    private async Task<(IReadOnlyList<ElementPropertyRecord> Records, int RenamedTypeCount)> TryEnrichBlueprintElementDisplayNamesAsync(
        IReadOnlyList<ElementPropertyRecord> records,
        CancellationToken cancellationToken)
    {
        if (!IsDatabaseOnline() || records.Count == 0)
        {
            return (records, 0);
        }

        Dictionary<ulong, ulong> elementTypeByElementId = ExtractElementTypeByElementId(records);
        if (elementTypeByElementId.Count == 0)
        {
            return (records, 0);
        }

        DataConnectionOptions options;
        try
        {
            options = BuildDbOptions();
        }
        catch
        {
            return (records, 0);
        }

        IReadOnlyDictionary<ulong, string> namesByTypeId;
        try
        {
            namesByTypeId = await _dataService.GetItemDefinitionDisplayNamesAsync(
                options,
                elementTypeByElementId.Values.Distinct().ToArray(),
                cancellationToken);
        }
        catch
        {
            return (records, 0);
        }

        if (namesByTypeId.Count == 0)
        {
            return (records, 0);
        }

        bool changed = false;
        var renamedTypeIds = new HashSet<ulong>();
        var enriched = new List<ElementPropertyRecord>(records.Count);
        foreach (ElementPropertyRecord record in records)
        {
            if (!elementTypeByElementId.TryGetValue(record.ElementId, out ulong typeId) ||
                !namesByTypeId.TryGetValue(typeId, out string? displayName) ||
                string.IsNullOrWhiteSpace(displayName))
            {
                enriched.Add(record);
                continue;
            }

            string replaced = ReplaceTypeTokenWithDisplayName(record.ElementDisplayName, displayName.Trim(), typeId);
            if (string.Equals(replaced, record.ElementDisplayName, StringComparison.Ordinal))
            {
                enriched.Add(record);
                continue;
            }

            changed = true;
            renamedTypeIds.Add(typeId);
            enriched.Add(record with { ElementDisplayName = replaced });
        }

        return changed ? (enriched, renamedTypeIds.Count) : (records, 0);
    }

    private static Dictionary<ulong, ulong> ExtractElementTypeByElementId(IReadOnlyList<ElementPropertyRecord> records)
    {
        var result = new Dictionary<ulong, ulong>();
        foreach (ElementPropertyRecord record in records)
        {
            if (record.ElementId == 0UL ||
                !string.Equals(record.Name, "elementType", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrWhiteSpace(record.DecodedValue))
            {
                continue;
            }

            string normalized = record.DecodedValue.Trim();
            if (!ulong.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong typeId) ||
                typeId == 0UL)
            {
                continue;
            }

            result[record.ElementId] = typeId;
        }

        return result;
    }

    private static string ReplaceTypeTokenWithDisplayName(string currentDisplayName, string typeDisplayName, ulong typeId)
    {
        if (string.IsNullOrWhiteSpace(currentDisplayName) || string.IsNullOrWhiteSpace(typeDisplayName))
        {
            return currentDisplayName;
        }

        string expectedPrefix = $"type_{typeId.ToString(CultureInfo.InvariantCulture)}";
        if (!currentDisplayName.StartsWith(expectedPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return currentDisplayName;
        }

        string suffix = currentDisplayName[expectedPrefix.Length..];
        return typeDisplayName + suffix;
    }

    private void AppendImportNotesToStatus(BlueprintImportResult importResult, string importSourcePath, ref string message)
    {
        if (string.IsNullOrWhiteSpace(importResult.ImportNotes))
        {
            return;
        }

        bool shouldSurfaceDetails =
            importResult.ImportPipeline.Contains("failed", StringComparison.OrdinalIgnoreCase) ||
            importResult.ImportPipeline.Contains("warning", StringComparison.OrdinalIgnoreCase) ||
            importResult.ImportPipeline.Contains("skipped", StringComparison.OrdinalIgnoreCase) ||
            importResult.ImportPipeline.Contains("unavailable", StringComparison.OrdinalIgnoreCase);
        if (!shouldSurfaceDetails)
        {
            return;
        }

        LastBlueprintImportErrorDetails = BuildBlueprintImportDetailsWithSource(importSourcePath, importResult.ImportNotes);

        string notePreview = BuildSingleLinePreview(importResult.ImportNotes, 240);
        if (string.IsNullOrWhiteSpace(notePreview))
        {
            return;
        }

        message += $" Details: {notePreview}";
    }

    private static string ResolveBlueprintImportSourcePath(string? sourcePathHint, string? sourceName = null)
    {
        string candidate = string.IsNullOrWhiteSpace(sourcePathHint) ? sourceName ?? string.Empty : sourcePathHint;
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return string.Empty;
        }

        try
        {
            return Path.GetFullPath(candidate);
        }
        catch
        {
            return candidate;
        }
    }

    private static string BuildBlueprintImportDetailsWithSource(string sourcePath, string details)
    {
        if (string.IsNullOrWhiteSpace(sourcePath))
        {
            return details;
        }

        if (string.IsNullOrWhiteSpace(details))
        {
            return $"Blueprint file: {sourcePath}";
        }

        return $"Blueprint file: {sourcePath}{Environment.NewLine}{details}";
    }

    private static string BuildSingleLinePreview(string value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        string[] lines = value
            .Split(new[] {"\r\n", "\n", "\r"}, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string firstLine = lines.Length > 0 ? lines[0] : value.Trim();
        if (firstLine.Length <= maxLength)
        {
            return firstLine;
        }

        int truncatedLength = Math.Max(1, maxLength - 3);
        return firstLine[..truncatedLength] + "...";
    }

    private static ulong? NormalizeBlueprintId(ulong? blueprintId)
    {
        return blueprintId.HasValue && blueprintId.Value > 0UL
            ? blueprintId
            : null;
    }

    private static string FormatBlueprintIdForDisplay(ulong? blueprintId, string fallback)
    {
        ulong? normalized = NormalizeBlueprintId(blueprintId);
        return normalized.HasValue
            ? normalized.Value.ToString(CultureInfo.InvariantCulture)
            : fallback;
    }

    [RelayCommand]
    private void ExpandAllElementProperties()
    {
        ElementPropertiesModel.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllElementProperties()
    {
        ElementPropertiesModel.CollapseAll(minDepth: 0);
    }

    [RelayCommand]
    private void ApplyElementTypeNameFilter()
    {
        AddElementTypeFilterHistory(ElementTypeNameFilterInput);
        ApplyElementPropertyFilter();
    }

    [RelayCommand]
    private void ClearElementTypeFilterHistory()
    {
        ElementTypeNameFilterInput = string.Empty;
        SelectedElementTypeFilterHistoryItem = null;
        ApplyElementPropertyFilter();
        if (AutoCollapseToFirstLevel)
        {
            ElementPropertiesModel.CollapseAll(minDepth: 0);
        }
    }

    [RelayCommand]
    private void CheckAllElementPropertyFilters()
    {
        SetAllElementPropertyFilters(isActive: true);
    }

    [RelayCommand]
    private void UncheckAllElementPropertyFilters()
    {
        SetAllElementPropertyFilters(isActive: false);
    }

    [RelayCommand]
    private void ExpandAllLuaBlocks()
    {
        Dpuyaml6Model.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllLuaBlocks()
    {
        Dpuyaml6Model.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private void ExpandAllHtmlRs()
    {
        Content2Model.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllHtmlRs()
    {
        Content2Model.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private void ExpandAllDatabank()
    {
        DatabankModel.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllDatabank()
    {
        DatabankModel.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private async Task ProbeEndpointAsync()
    {
        if (IsBusy)
        {
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        try
        {
            IsBusy = true;
            StatusMessage = "Probing construct endpoint...";

            ulong constructId = ParseRequiredConstructId(ConstructIdInput);
            Uri uri = BuildEndpointUri(EndpointTemplateInput, constructId);

            EndpointProbeResult result = await _dataService.ProbeEndpointAsync(uri, cts.Token);
            _lastEndpointResult = result;
            UpdateEndpointSummary(result);

            StatusMessage = $"Endpoint probe finished with HTTP {result.StatusCode}.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Endpoint probe failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    public string BuildGetConstructDataExportJson()
    {
        ConstructUpdate? endpointUpdate = _lastEndpointResult?.ConstructUpdate;
        ConstructInfoPreamble? endpointInfoPreamble = _lastEndpointResult?.ConstructInfoPreamble;
        DatabaseConstructSnapshot? snapshot = _lastSnapshot;

        if (endpointUpdate is null && endpointInfoPreamble is null && snapshot is null)
        {
            throw new InvalidOperationException(
                "No data available for export. Load DB snapshot and/or probe endpoint first.");
        }

        Vec3 constructPosition = endpointUpdate?.Position ?? endpointInfoPreamble?.Position ?? snapshot?.Position
            ?? throw new InvalidOperationException("Cannot resolve construct position.");

        Quat constructRotation = endpointUpdate?.Rotation ?? endpointInfoPreamble?.Rotation ?? snapshot?.Rotation
            ?? throw new InvalidOperationException("Cannot resolve construct rotation.");

        Vec3 worldVelocity = endpointUpdate?.WorldAbsoluteVelocity
            ?? snapshot?.ResumeLinearVelocity
            ?? new Vec3(0, 0, 0);

        Vec3 worldAngularVelocity = endpointUpdate?.WorldAbsoluteAngularVelocity
            ?? snapshot?.ResumeAngularVelocity
            ?? new Vec3(0, 0, 0);

        double constructMass = snapshot?.ConstructMass ?? snapshot?.CurrentMass ?? 0d;
        double constructSpeed = worldVelocity.Magnitude;

        var payload = new
        {
            constructPosition = new[] { constructPosition.X, constructPosition.Y, constructPosition.Z },
            constructRotation = new[] { constructRotation.W, constructRotation.X, constructRotation.Y, constructRotation.Z },
            worldVelocity = new[] { worldVelocity.X, worldVelocity.Y, worldVelocity.Z },
            worldAngularVelocity = new[] { worldAngularVelocity.X, worldAngularVelocity.Y, worldAngularVelocity.Z },
            constructMass,
            constructSpeed
        };

        string json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        StatusMessage = "getConstructData export JSON prepared.";
        return json;
    }

    public async Task RepairDestroyedElementsAsync(CancellationToken cancellationToken)
    {
        if (IsBusy || RepairInProgress)
        {
            return;
        }

        if (_lastSnapshot is null)
        {
            throw new InvalidOperationException("Load a DB snapshot before running repair.");
        }

        if (!IsDatabaseOnline())
        {
            throw new InvalidOperationException("DB is offline.");
        }

        try
        {
            IsBusy = true;
            RepairInProgress = true;
            RepairProgressPercent = 0d;
            RepairStatusText = "Repair: starting...";
            StatusMessage = "Repairing element state properties...";
            await Task.Yield();

            DataConnectionOptions options = BuildDbOptions();
            ulong constructId = _lastSnapshot.ConstructId;

            var progress = new Progress<DestroyedRepairProgress>(state =>
            {
                if (state.TotalCount <= 0)
                {
                    RepairProgressPercent = 0d;
                    RepairStatusText = "Repair: no matching properties found.";
                    return;
                }

                RepairProgressPercent = state.ProcessedCount * 100d / state.TotalCount;
                RepairStatusText = $"Repair: {state.ProcessedCount}/{state.TotalCount}";
            });

            DestroyedRepairResult result = await _dataService.RepairDestroyedPropertiesAsync(
                options,
                constructId,
                progress,
                cancellationToken);

            ApplyRepairToLoadedSnapshot();

            if (result.TotalCount == 0)
            {
                RepairProgressPercent = 0d;
                RepairStatusText = "Repair: no matching properties found.";
                StatusMessage = "Repair finished: no destroyed/restoreCount properties found.";
                return;
            }

            RepairProgressPercent = 100d;
            RepairStatusText = $"Repair complete: {result.UpdatedCount}/{result.TotalCount}";
            StatusMessage = $"Repair finished: removed {result.UpdatedCount} destroyed/restoreCount row(s).";
        }
        finally
        {
            RepairInProgress = false;
            IsBusy = false;
        }
    }

}
