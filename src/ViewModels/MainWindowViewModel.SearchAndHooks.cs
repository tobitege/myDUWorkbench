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
    private void SetSelectedConstructSuggestion(ConstructNameLookupRecord? value, bool suppressAutoLoad)
    {
        bool previous = _suppressConstructSelectionAutoLoad;
        _suppressConstructSelectionAutoLoad = suppressAutoLoad;
        try
        {
            SelectedConstructNameSuggestion = value;
        }
        finally
        {
            _suppressConstructSelectionAutoLoad = previous;
        }
    }

    private ConstructNameLookupRecord? ResolveConstructSuggestionToSelect()
    {
        if (_restoredConstructSuggestionId.HasValue)
        {
            ConstructNameLookupRecord? restored = ConstructNameSuggestions.FirstOrDefault(s =>
                s.ConstructId == _restoredConstructSuggestionId.Value &&
                (string.IsNullOrWhiteSpace(_restoredConstructSuggestionName) ||
                 string.Equals(s.ConstructName, _restoredConstructSuggestionName, StringComparison.OrdinalIgnoreCase)));
            if (restored is not null)
            {
                return restored;
            }
        }

        if (ulong.TryParse(ConstructIdInput, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong constructIdFromInput))
        {
            ConstructNameLookupRecord? byInputId = ConstructNameSuggestions.FirstOrDefault(s => s.ConstructId == constructIdFromInput);
            if (byInputId is not null)
            {
                return byInputId;
            }
        }

        if (SelectedConstructNameSuggestion is not null)
        {
            ConstructNameLookupRecord? current = ConstructNameSuggestions.FirstOrDefault(s =>
                s.ConstructId == SelectedConstructNameSuggestion.ConstructId);
            if (current is not null)
            {
                return current;
            }
        }

        return null;
    }

    private void QueueConstructNameSearch(string? input)
    {
        _constructSearchCts?.Cancel();
        _constructSearchCts?.Dispose();
        _constructSearchCts = null;

        bool hasPlayerScope = TryGetScopedPlayerId(out ulong scopedPlayerId);
        bool hasConstructFilter = CountSearchCharacters(input) >= 3;
        if (!hasPlayerScope && !hasConstructFilter)
        {
            ConstructNameSuggestions.Clear();
            SetSelectedConstructSuggestion(null, suppressAutoLoad: true);
            ConstructSearchStatus = string.Empty;
            return;
        }

        ConstructSearchStatus = hasPlayerScope ? "Loading player constructs..." : "Searching...";
        var cts = new CancellationTokenSource();
        _constructSearchCts = cts;
        _ = RefreshConstructNameSuggestionsAsync(input, hasPlayerScope ? scopedPlayerId : null, cts.Token);
    }

    private async Task RefreshConstructNameSuggestionsAsync(
        string? input,
        ulong? scopedPlayerId,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(250, cancellationToken);
            DataConnectionOptions options = BuildDbOptions();
            IReadOnlyList<ConstructNameLookupRecord> results;
            if (scopedPlayerId.HasValue)
            {
                IReadOnlyList<UserConstructRecord> userConstructs = await _dataService.GetUserConstructsSortedByNameAsync(
                    options,
                    scopedPlayerId.Value,
                    2000,
                    cancellationToken);

                string normalizedFilter = input?.Trim() ?? string.Empty;
                IEnumerable<ConstructNameLookupRecord> scopedResults = userConstructs
                    .Select(c => new ConstructNameLookupRecord(c.ConstructId, c.ConstructName))
                    .GroupBy(c => c.ConstructId)
                    .Select(g => g.First());

                if (!string.IsNullOrWhiteSpace(normalizedFilter))
                {
                    scopedResults = scopedResults.Where(c =>
                        MatchesWildcardPattern(c.ConstructName, normalizedFilter) ||
                        MatchesWildcardPattern(c.ConstructId.ToString(CultureInfo.InvariantCulture), normalizedFilter));
                }

                results = scopedResults
                    .OrderBy(c => c.ConstructName, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(c => c.ConstructId)
                    .ToList();
            }
            else
            {
                string searchInput = input?.Trim() ?? string.Empty;
                results = await _dataService.SearchConstructsByNameAsync(
                    options,
                    searchInput,
                    25,
                    cancellationToken);
            }

            ConstructNameSuggestions.Clear();
            foreach (ConstructNameLookupRecord result in results)
            {
                ConstructNameSuggestions.Add(result);
            }

            ConstructNameLookupRecord? toSelect = ResolveConstructSuggestionToSelect();
            if (toSelect is not null)
            {
                SetSelectedConstructSuggestion(toSelect, suppressAutoLoad: true);
            }
            else if (ConstructNameSuggestions.Count == 0)
            {
                SetSelectedConstructSuggestion(null, suppressAutoLoad: true);
            }

            ConstructSearchStatus = scopedPlayerId.HasValue
                ? $"{ConstructNameSuggestions.Count} found for player {scopedPlayerId.Value}"
                : $"{ConstructNameSuggestions.Count} found";
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            ConstructSearchStatus = "Search failed";
            StatusMessage = $"Construct name search failed: {ex.Message}";
        }
    }

    [RelayCommand]
    private async Task UseConstructSuggestionAndLoadAsync()
    {
        ConstructNameLookupRecord? selected = SelectedConstructNameSuggestion;
        if (selected is null && ConstructNameSuggestions.Count > 0)
        {
            selected = ConstructNameSuggestions[0];
        }

        if (selected is null)
        {
            return;
        }

        ConstructIdInput = selected.ConstructId.ToString(CultureInfo.InvariantCulture);
        await LoadDatabaseAsync();
    }

    [RelayCommand]
    private void RefreshPlayerNames()
    {
        QueuePlayerNameCacheRefresh(forceReload: true);
    }

    private void QueuePlayerNameSearch(string? input)
    {
        if (_allPlayerNameSuggestions.Count > 0)
        {
            ApplyPlayerNameFilter(input);
            return;
        }

        if (AutoLoadPlayerNames)
        {
            QueuePlayerNameCacheRefresh(forceReload: false);
            return;
        }

        _playerSearchCts?.Cancel();
        _playerSearchCts?.Dispose();
        _playerSearchCts = null;

        if (CountSearchCharacters(input) < 3)
        {
            PlayerNameSuggestions.Clear();
            SelectedPlayerNameSuggestion = null;
            PlayerSearchStatus = string.Empty;
            return;
        }

        PlayerSearchStatus = "Searching...";
        var cts = new CancellationTokenSource();
        _playerSearchCts = cts;
        _ = RefreshPlayerNameSuggestionsFromDatabaseAsync(input!, cts.Token);
    }

    private void QueuePlayerNameCacheRefresh(bool forceReload)
    {
        if (!forceReload && _allPlayerNameSuggestions.Count > 0)
        {
            ApplyPlayerNameFilter(PlayerNameSearchInput);
            return;
        }

        _playerSearchCts?.Cancel();
        _playerSearchCts?.Dispose();
        _playerSearchCts = null;

        PlayerSearchStatus = "Loading players...";
        var cts = new CancellationTokenSource();
        _playerSearchCts = cts;
        _ = RefreshAllPlayerNamesAsync(cts.Token);
    }

    private async Task RefreshAllPlayerNamesAsync(CancellationToken cancellationToken)
    {
        try
        {
            DataConnectionOptions options = BuildDbOptions();
            IReadOnlyList<PlayerNameLookupRecord> results = await _dataService.GetPlayersSortedByNameAsync(
                options,
                cancellationToken);

            _allPlayerNameSuggestions.Clear();
            _allPlayerNameSuggestions.AddRange(results);

            ApplyPlayerNameFilter(PlayerNameSearchInput);
            if (LimitToSelectedPlayerConstructs && SelectedPlayerNameSuggestion?.PlayerId.HasValue == true)
            {
                QueueConstructNameSearch(ConstructNameSearchInput);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            PlayerSearchStatus = "Load failed";
            StatusMessage = $"Player list load failed: {ex.Message}";
        }
    }

    private void ApplyPlayerNameFilter(string? input)
    {
        string normalizedFilter = input?.Trim() ?? string.Empty;
        ulong? selectedPlayerId = SelectedPlayerNameSuggestion?.PlayerId ?? TryParseOptionalUlong(PlayerIdInput);

        IEnumerable<PlayerNameLookupRecord> filtered = _allPlayerNameSuggestions;
        if (!string.IsNullOrWhiteSpace(normalizedFilter))
        {
            filtered = filtered.Where(player =>
                MatchesWildcardPattern(player.PlayerName, normalizedFilter) ||
                (player.PlayerId.HasValue &&
                 MatchesWildcardPattern(player.PlayerId.Value.ToString(CultureInfo.InvariantCulture), normalizedFilter)));
        }

        PlayerNameSuggestions.Clear();
        foreach (PlayerNameLookupRecord player in filtered)
        {
            PlayerNameSuggestions.Add(player);
        }

        if (selectedPlayerId.HasValue)
        {
            PlayerNameLookupRecord? matched = PlayerNameSuggestions.FirstOrDefault(p => p.PlayerId == selectedPlayerId.Value);
            if (matched is not null)
            {
                SelectedPlayerNameSuggestion = matched;
            }
        }

        if (_allPlayerNameSuggestions.Count == 0)
        {
            PlayerSearchStatus = string.Empty;
            return;
        }

        PlayerSearchStatus = string.IsNullOrWhiteSpace(normalizedFilter)
            ? $"{PlayerNameSuggestions.Count} loaded"
            : $"{PlayerNameSuggestions.Count} of {_allPlayerNameSuggestions.Count} loaded";
    }

    private async Task RefreshPlayerNameSuggestionsFromDatabaseAsync(string input, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(250, cancellationToken);
            DataConnectionOptions options = BuildDbOptions();
            ulong? selectedPlayerId = SelectedPlayerNameSuggestion?.PlayerId ?? TryParseOptionalUlong(PlayerIdInput);
            IReadOnlyList<PlayerNameLookupRecord> results = await _dataService.SearchPlayersByNameAsync(
                options,
                input,
                25,
                cancellationToken);

            PlayerNameSuggestions.Clear();
            foreach (PlayerNameLookupRecord player in results)
            {
                PlayerNameSuggestions.Add(player);
            }

            if (selectedPlayerId.HasValue)
            {
                PlayerNameLookupRecord? matched = PlayerNameSuggestions.FirstOrDefault(p => p.PlayerId == selectedPlayerId.Value);
                if (matched is not null)
                {
                    SelectedPlayerNameSuggestion = matched;
                }
            }

            PlayerSearchStatus = $"{PlayerNameSuggestions.Count} found";
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            PlayerSearchStatus = "Search failed";
            StatusMessage = $"Player name search failed: {ex.Message}";
        }
    }

    private bool TryGetScopedPlayerId(out ulong playerId)
    {
        if (!LimitToSelectedPlayerConstructs)
        {
            playerId = 0UL;
            return false;
        }

        if (SelectedPlayerNameSuggestion?.PlayerId.HasValue == true)
        {
            playerId = SelectedPlayerNameSuggestion.PlayerId.Value;
            return true;
        }

        playerId = 0UL;
        return false;
    }

    private void RefreshConstructSuggestionsForCurrentPlayer()
    {
        QueueConstructNameSearch(ConstructNameSearchInput);
    }

    private static int CountSearchCharacters(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return 0;
        }

        int count = 0;
        foreach (char c in input)
        {
            if (!char.IsWhiteSpace(c) && c != '*' && c != '%' && c != '?' && c != '_')
            {
                count++;
            }
        }

        return count;
    }

    private static bool MatchesWildcardPattern(string candidate, string pattern)
    {
        if (string.IsNullOrWhiteSpace(candidate) || string.IsNullOrWhiteSpace(pattern))
        {
            return false;
        }

        string wildcard = pattern.Trim();
        if (wildcard.IndexOf('*') < 0 &&
            wildcard.IndexOf('?') < 0 &&
            wildcard.IndexOf('%') < 0 &&
            wildcard.IndexOf('_') < 0)
        {
            wildcard += "*";
        }

        string regexPattern = "^" + Regex.Escape(wildcard)
            .Replace("\\*", ".*")
            .Replace("\\?", ".")
            .Replace("%", ".*")
            .Replace("_", ".") + "$";

        return Regex.IsMatch(candidate, regexPattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private void UpdateDatabaseSummary(DatabaseConstructSnapshot snapshot)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Construct: {snapshot.ConstructId} ({snapshot.ConstructName})");
        sb.AppendLine($"Position: {snapshot.Position}");
        sb.AppendLine($"Rotation: {snapshot.Rotation}");

        if (snapshot.PlayerId.HasValue)
        {
            sb.AppendLine(
                $"Player: {snapshot.PlayerId} ({snapshot.PlayerName ?? "<unknown>"}) | player.construct_id={snapshot.PlayerConstructId?.ToString(CultureInfo.InvariantCulture) ?? "<null>"}");
        }

        if (snapshot.ConstructMass.HasValue)
        {
            sb.AppendLine($"construct_mass_total: {snapshot.ConstructMass.Value.ToString("R", CultureInfo.InvariantCulture)}");
        }

        if (snapshot.CurrentMass.HasValue)
        {
            sb.AppendLine($"current_mass: {snapshot.CurrentMass.Value.ToString("R", CultureInfo.InvariantCulture)}");
        }

        if (snapshot.SpeedFactor.HasValue)
        {
            sb.AppendLine($"speedFactor: {snapshot.SpeedFactor.Value.ToString("R", CultureInfo.InvariantCulture)}");
        }

        if (snapshot.ResumeLinearVelocity.HasValue)
        {
            Vec3 value = snapshot.ResumeLinearVelocity.Value;
            sb.AppendLine($"resumeLinearVelocity: {value} | speed={value.Magnitude.ToString("R", CultureInfo.InvariantCulture)}");
        }

        if (snapshot.ResumeAngularVelocity.HasValue)
        {
            sb.AppendLine($"resumeAngularVelocity: {snapshot.ResumeAngularVelocity.Value}");
        }

        sb.AppendLine($"Element properties loaded: {snapshot.Properties.Count}");
        DatabaseSummary = sb.ToString();
    }

    private void UpdateBlueprintSummary(BlueprintImportResult importResult)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Blueprint import (local file)");
        sb.AppendLine($"Source: {importResult.SourceName}");
        sb.AppendLine($"Name: {importResult.BlueprintName}");
        sb.AppendLine($"BlueprintId: {FormatBlueprintIdForDisplay(importResult.BlueprintId, "<none>")}");
        sb.AppendLine($"Elements: {importResult.ElementCount.ToString(CultureInfo.InvariantCulture)}");
        sb.AppendLine($"Properties loaded: {importResult.Properties.Count.ToString(CultureInfo.InvariantCulture)}");
        if (!string.IsNullOrWhiteSpace(importResult.ImportPipeline))
        {
            sb.AppendLine($"Import pipeline: {importResult.ImportPipeline}");
        }

        if (!string.IsNullOrWhiteSpace(importResult.ImportNotes))
        {
            sb.AppendLine($"Import notes: {importResult.ImportNotes}");
        }

        DatabaseSummary = sb.ToString();
    }

    private void UpdateEndpointSummary(EndpointProbeResult result)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"URL: {result.Url}");
        sb.AppendLine($"HTTP: {result.StatusCode}");
        sb.AppendLine($"Content-Type: {result.ContentType}");
        sb.AppendLine($"Bytes: {result.PayloadSize}");

        if (result.ConstructUpdate is not null)
        {
            ConstructUpdate update = result.ConstructUpdate;
            sb.AppendLine("Decoded as ConstructUpdate:");
            sb.AppendLine($"  constructId: {update.ConstructId}");
            sb.AppendLine($"  baseId: {update.BaseId}");
            sb.AppendLine($"  position: {update.Position}");
            sb.AppendLine($"  rotation: {update.Rotation}");
            sb.AppendLine($"  worldAbsoluteVelocity: {update.WorldAbsoluteVelocity}");
            sb.AppendLine($"  worldAbsoluteAngularVelocity: {update.WorldAbsoluteAngularVelocity}");
            sb.AppendLine($"  pilotId: {update.PilotId}");
            sb.AppendLine($"  grounded: {update.Grounded}");
            sb.AppendLine($"  networkTime: {update.NetworkTime}");
        }
        else if (result.ConstructInfoPreamble is not null)
        {
            ConstructInfoPreamble preamble = result.ConstructInfoPreamble;
            sb.AppendLine("Decoded as /constructs/{id}/info preamble:");
            sb.AppendLine($"  constructId: {preamble.ConstructId}");
            sb.AppendLine($"  parentId: {preamble.ParentId}");
            sb.AppendLine($"  position: {preamble.Position}");
            sb.AppendLine($"  rotation: {preamble.Rotation}");
        }
        else if (result.BlobHeader is not null)
        {
            NqStructBlobHeader blob = result.BlobHeader;
            sb.AppendLine("Decoded as NQ struct blob header:");
            sb.AppendLine($"  timestamp: {blob.Timestamp}");
            sb.AppendLine($"  target: {blob.Target}");
            sb.AppendLine($"  messageType: {blob.MessageType}");
            sb.AppendLine($"  format: {blob.Format}");
            sb.AppendLine($"  payloadLength: {blob.PayloadLength}");
        }

        if (!string.IsNullOrWhiteSpace(result.Notes))
        {
            sb.AppendLine($"Notes: {result.Notes}");
        }

        EndpointSummary = sb.ToString();
        EndpointRawPreview = result.RawPreview;
    }

    partial void OnConstructIdInputChanged(string value)
    {
    }

    partial void OnPlayerIdInputChanged(string value)
    {
        OnPropertyChanged(nameof(SelectedPlayerIdDisplay));

        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            RefreshConstructSuggestionsForCurrentPlayer();
        }
    }

    partial void OnDbHostInputChanged(string value)
    {
    }

    partial void OnServerRootPathInputChanged(string value)
    {
    }

    partial void OnNqUtilsDllPathInputChanged(string value)
    {
    }

    partial void OnBlueprintImportEndpointInputChanged(string value)
    {
    }

    partial void OnDbPortInputChanged(string value)
    {
    }

    partial void OnDbNameInputChanged(string value)
    {
    }

    partial void OnDbUserInputChanged(string value)
    {
    }

    partial void OnDbPasswordInputChanged(string value)
    {
    }

    partial void OnPropertyLimitInputChanged(string value)
    {
    }

    partial void OnElementTypeNameFilterInputChanged(string value)
    {
    }

    partial void OnDamagedOnlyChanged(bool value)
    {
        if (value && !CanUseDamagedFilter)
        {
            DamagedOnly = false;
            return;
        }

        ApplyElementPropertyFilter();
    }

    partial void OnSelectedElementTypeFilterHistoryItemChanged(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        ElementTypeNameFilterInput = value;
        ApplyElementPropertyFilter();
    }

    partial void OnAutoLoadOnStartupChanged(bool value)
    {
    }

    partial void OnAutoLoadPlayerNamesChanged(bool value)
    {
        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        if (value && _allPlayerNameSuggestions.Count == 0)
        {
            QueuePlayerNameCacheRefresh(forceReload: false);
        }

        PersistSettingsNow();
    }

    partial void OnLimitToSelectedPlayerConstructsChanged(bool value)
    {
        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        QueueConstructNameSearch(ConstructNameSearchInput);
        PersistSettingsNow();
    }

    partial void OnAutoConnectDatabaseChanged(bool value)
    {
        RefreshAutoConnectLoopState();
        OnPropertyChanged(nameof(DatabaseAvailabilityDisplay));

        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        PersistSettingsNow();
    }

    partial void OnAutoConnectRetrySecondsChanged(int value)
    {
        int clamped = ClampAutoConnectRetrySeconds(value);
        if (clamped != value)
        {
            AutoConnectRetrySeconds = clamped;
            return;
        }

        RefreshAutoConnectLoopState();
        OnPropertyChanged(nameof(DatabaseAvailabilityDisplay));

        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        PersistSettingsNow();
    }

    partial void OnAutoWrapContentChanged(bool value)
    {
        OnPropertyChanged(nameof(ContentTextWrapping));
    }

    partial void OnAutoCollapseToFirstLevelChanged(bool value)
    {
    }

    partial void OnLuaVersioningEnabledChanged(bool value)
    {
    }

    partial void OnDatabaseAvailabilityStatusChanged(string value)
    {
        if (AutoLoadPlayerNames &&
            string.Equals(value, "Ok", StringComparison.OrdinalIgnoreCase) &&
            _allPlayerNameSuggestions.Count == 0)
        {
            QueuePlayerNameCacheRefresh(forceReload: false);
        }

        RefreshAutoConnectLoopState();
        OnPropertyChanged(nameof(DatabaseAvailabilityDisplay));
        OnPropertyChanged(nameof(CanEditBlueprint));
        OnPropertyChanged(nameof(CanEditBlueprintMaxUse));
        OnPropertyChanged(nameof(CanCopyBlueprint));
        OnPropertyChanged(nameof(CanDeleteBlueprint));
        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
        OnPropertyChanged(nameof(CanExportBlueprintElementSummary));
    }

    partial void OnAutoConnectNextRetrySecondsChanged(int? value)
    {
        OnPropertyChanged(nameof(DatabaseAvailabilityDisplay));
    }

    partial void OnLastSavedFolderChanged(string value)
    {
        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        PersistSettingsNow();
    }

    partial void OnEndpointTemplateInputChanged(string value)
    {
    }

    partial void OnSelectedDpuyaml6NodeChanged(object? value)
    {
        PropertyTreeRow? row = ResolveSelectedTreeRow(value);
        _selectedDpuyamlNodeKey = BuildSelectionKey(row);
        SelectedDpuyaml6Content = row?.FullContent ?? string.Empty;
        OnPropertyChanged(nameof(CanSaveSelectedLuaBlob));
    }

    partial void OnSelectedElementPropertyNodeChanged(object? value)
    {
        PropertyTreeRow? row = ResolveSelectedTreeRow(value);
        _selectedElementNodeKey = BuildSelectionKey(row);
    }

    partial void OnConstructNameSearchInputChanged(string value)
    {
        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        QueueConstructNameSearch(value);
    }

    partial void OnSelectedConstructNameSuggestionChanged(ConstructNameLookupRecord? value)
    {
        if (value is null)
        {
            _restoredConstructSuggestionId = null;
            _restoredConstructSuggestionName = string.Empty;
            return;
        }

        _restoredConstructSuggestionId = value.ConstructId;
        _restoredConstructSuggestionName = value.ConstructName;

        if (_isRestoringSettings || _suppressConstructSelectionAutoLoad)
        {
            return;
        }

        _ = UseConstructSuggestionAndLoadAsync();
    }

    partial void OnPlayerNameSearchInputChanged(string value)
    {
        if (_isRestoringSettings || _isStartupInitializing)
        {
            return;
        }

        QueuePlayerNameSearch(value);
    }

    partial void OnSelectedPlayerNameSuggestionChanged(PlayerNameLookupRecord? value)
    {
        if (value?.PlayerId.HasValue == true)
        {
            PlayerIdInput = value.PlayerId.Value.ToString(CultureInfo.InvariantCulture);
        }
        else if (!_isRestoringSettings && !_isStartupInitializing)
        {
            PlayerIdInput = string.Empty;
        }

        OnPropertyChanged(nameof(SelectedPlayerIdDisplay));
        OnPropertyChanged(nameof(CanImportBlueprint));

        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            RefreshConstructSuggestionsForCurrentPlayer();
        }

        HandleBlueprintPlayerFilterChange();
    }

    partial void OnSelectedContent2NodeChanged(object? value)
    {
        PropertyTreeRow? row = ResolveSelectedTreeRow(value);
        _selectedContent2NodeKey = BuildSelectionKey(row);
        SelectedContent2Content = row?.FullContent ?? string.Empty;
        OnPropertyChanged(nameof(CanSaveSelectedHtmlRsBlob));
    }

    partial void OnSelectedDatabankNodeChanged(object? value)
    {
        PropertyTreeRow? row = ResolveSelectedTreeRow(value);
        _selectedDatabankNodeKey = BuildSelectionKey(row);
        SelectedDatabankContent = row?.FullContent ?? string.Empty;
        OnPropertyChanged(nameof(CanSaveSelectedDatabankBlob));
    }

    partial void OnIsBusyChanged(bool value)
    {
        OnPropertyChanged(nameof(CanEditBlueprint));
        OnPropertyChanged(nameof(CanEditBlueprintMaxUse));
        OnPropertyChanged(nameof(CanCopyBlueprint));
        OnPropertyChanged(nameof(CanImportBlueprint));
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
        OnPropertyChanged(nameof(CanDeleteBlueprint));
        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
        OnPropertyChanged(nameof(CanExportConstructBrowserElementSummary));
        OnPropertyChanged(nameof(CanExportBlueprintElementSummary));
    }

    partial void OnRepairInProgressChanged(bool value)
    {
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
    }

    partial void OnSelectedBlueprintChanged(BlueprintDbRecord? value)
    {
        if (value is not null)
        {
            BlueprintEditName = value.Name;
            BlueprintEditFreeDeploy = value.FreeDeploy;
            BlueprintEditApplyMaxUse = false;
            SetBlueprintTypeSelectionFromMaxUse(value.MaxUse);
        }
        else
        {
            BlueprintEditName = string.Empty;
            BlueprintEditFreeDeploy = false;
            BlueprintEditApplyMaxUse = false;
            SetBlueprintTypeSelection(isCore: true);
        }

        OnPropertyChanged(nameof(CanEditBlueprint));
        OnPropertyChanged(nameof(CanEditBlueprintMaxUse));
        OnPropertyChanged(nameof(CanCopyBlueprint));
        OnPropertyChanged(nameof(CanDeleteBlueprint));
        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
        OnPropertyChanged(nameof(BlueprintCurrentMaxUseStateDisplay));
    }

    partial void OnBlueprintEditNameChanged(string value)
    {
        _ = value;
        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
    }

    partial void OnBlueprintEditCoreBlueprintChanged(bool value)
    {
        if (_isUpdatingBlueprintTypeSelection)
        {
            return;
        }

        SetBlueprintTypeSelection(isCore: value);
    }

    partial void OnBlueprintEditSingleUseBlueprintChanged(bool value)
    {
        if (_isUpdatingBlueprintTypeSelection)
        {
            return;
        }

        SetBlueprintTypeSelection(isCore: !value);
    }

    partial void OnBlueprintEditApplyMaxUseChanged(bool value)
    {
        _ = value;
        OnPropertyChanged(nameof(CanEditBlueprintMaxUse));
        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
    }

    private void SetBlueprintTypeSelectionFromMaxUse(long? maxUse)
    {
        bool isSingleUse = maxUse.HasValue && maxUse.Value == 1L;
        SetBlueprintTypeSelection(isCore: !isSingleUse);
    }

    private void SetBlueprintTypeSelection(bool isCore)
    {
        _isUpdatingBlueprintTypeSelection = true;
        try
        {
            BlueprintEditCoreBlueprint = isCore;
            BlueprintEditSingleUseBlueprint = !isCore;
        }
        finally
        {
            _isUpdatingBlueprintTypeSelection = false;
        }

        OnPropertyChanged(nameof(CanSaveBlueprint));
        OnPropertyChanged(nameof(BlueprintEditValidationMessage));
        OnPropertyChanged(nameof(HasBlueprintEditValidationError));
    }

    private string BuildBlueprintCurrentMaxUseStateDisplay()
    {
        if (SelectedBlueprint is not { } bp)
        {
            return string.Empty;
        }

        return bp.MaxUse switch
        {
            null => "Current DB type: Core BP (unlimited)",
            1 => "Current DB type: Single-use BP",
            0 => "Current DB type: Expired BP (max_use = 0)",
            long value => $"Current DB type: Custom max_use = {value.ToString(CultureInfo.InvariantCulture)}"
        };
    }

    private ulong? GetBlueprintCreatorFilterPlayerId()
    {
        return SelectedPlayerNameSuggestion?.PlayerId;
    }

    private string BuildBlueprintsPlayerFilterDisplay()
    {
        ulong? playerId = GetBlueprintCreatorFilterPlayerId();
        return playerId.HasValue
            ? $"Player filter: ON (creator_id = {playerId.Value.ToString(CultureInfo.InvariantCulture)})"
            : "Player filter: OFF";
    }

    private void HandleBlueprintPlayerFilterChange()
    {
        ulong? currentPlayerId = GetBlueprintCreatorFilterPlayerId();
        if (_lastBlueprintCreatorFilterPlayerId == currentPlayerId)
        {
            return;
        }

        _lastBlueprintCreatorFilterPlayerId = currentPlayerId;
        OnPropertyChanged(nameof(BlueprintsPlayerFilterDisplay));

        if (_isRestoringSettings || _isStartupInitializing || IsBusy || !IsDatabaseOnline())
        {
            return;
        }

        _ = LoadBlueprintsAsync();
    }
}
