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
            RefreshDamagedFilterAvailability();
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
                RefreshDamagedFilterAvailability();
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
                !IsElementTypeIdPropertyName(record.Name) ||
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

    private static bool IsElementTypeIdPropertyName(string? propertyName)
    {
        string normalized = NormalizePropertyName(propertyName);
        return string.Equals(normalized, "elementType", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalized, "element_type_id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalized, "elementTypeId", StringComparison.OrdinalIgnoreCase);
    }

    private static string ReplaceTypeTokenWithDisplayName(string currentDisplayName, string typeDisplayName, ulong typeId)
    {
        if (string.IsNullOrWhiteSpace(currentDisplayName) || string.IsNullOrWhiteSpace(typeDisplayName))
        {
            return currentDisplayName;
        }

        string expectedPrefix = $"type_{typeId.ToString(CultureInfo.InvariantCulture)}";
        if (currentDisplayName.StartsWith(expectedPrefix, StringComparison.OrdinalIgnoreCase))
        {
            string typedSuffix = currentDisplayName[expectedPrefix.Length..];
            return typeDisplayName + typedSuffix;
        }

        const string fallbackPrefix = "BlueprintElement";
        if (currentDisplayName.StartsWith(fallbackPrefix, StringComparison.OrdinalIgnoreCase))
        {
            string fallbackSuffix = currentDisplayName[fallbackPrefix.Length..];
            return typeDisplayName + fallbackSuffix;
        }

        return currentDisplayName;
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
}
