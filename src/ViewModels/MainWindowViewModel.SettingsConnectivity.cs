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
    private static DataConnectionOptions BuildDbOptions(
        string? serverRootPath = null,
        string? host = null,
        string? port = null,
        string? database = null,
        string? username = null,
        string? password = null)
    {
        return new DataConnectionOptions(
            serverRootPath ?? string.Empty,
            host ?? string.Empty,
            ParsePort(port),
            database ?? string.Empty,
            username ?? string.Empty,
            password ?? string.Empty);
    }

    private DataConnectionOptions BuildDbOptions()
    {
        return BuildDbOptions(ServerRootPathInput, DbHostInput, DbPortInput, DbNameInput, DbUserInput, DbPasswordInput);
    }

    private void RestoreSettingsFromDisk()
    {
        _isRestoringSettings = true;
        try
        {
            WorkbenchSettings settings = _settingsService.Load();
            ApplySettings(settings);
        }
        finally
        {
            _isRestoringSettings = false;
        }
    }

    private void ApplySettings(WorkbenchSettings settings)
    {
        if (settings.HasPersistedPlayerFilterState || !string.IsNullOrWhiteSpace(settings.PlayerIdInput))
        {
            PlayerIdInput = settings.PlayerIdInput ?? string.Empty;
        }

        bool hasExplicitlyClearedPlayerFilter =
            settings.HasPersistedPlayerFilterState &&
            string.IsNullOrWhiteSpace(settings.PlayerIdInput);

        if (hasExplicitlyClearedPlayerFilter)
        {
            ConstructIdInput = string.Empty;
            ConstructNameSearchInput = string.Empty;
        }
        else
        {
            if (!string.IsNullOrWhiteSpace(settings.ConstructIdInput))
            {
                ConstructIdInput = settings.ConstructIdInput;
            }

            if (!string.IsNullOrWhiteSpace(settings.ConstructNameSearchInput))
            {
                ConstructNameSearchInput = settings.ConstructNameSearchInput;
            }
        }

        if (!string.IsNullOrWhiteSpace(settings.PlayerNameSearchInput))
        {
            PlayerNameSearchInput = settings.PlayerNameSearchInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.EndpointTemplateInput))
        {
            EndpointTemplateInput = settings.EndpointTemplateInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.DbHostInput))
        {
            DbHostInput = settings.DbHostInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.ServerRootPathInput))
        {
            ServerRootPathInput = settings.ServerRootPathInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.NqUtilsDllPathInput))
        {
            NqUtilsDllPathInput = settings.NqUtilsDllPathInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.BlueprintImportEndpointInput))
        {
            BlueprintImportEndpointInput = settings.BlueprintImportEndpointInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.DbPortInput))
        {
            DbPortInput = settings.DbPortInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.DbNameInput))
        {
            DbNameInput = settings.DbNameInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.DbUserInput))
        {
            DbUserInput = settings.DbUserInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.DbPassword))
        {
            DbPasswordInput = settings.DbPassword;
        }

        if (!string.IsNullOrWhiteSpace(settings.PropertyLimitInput))
        {
            PropertyLimitInput = settings.PropertyLimitInput;
        }

        BlueprintImportAppendDateIfExists = settings.BlueprintImportAppendDateIfExists;

        ElementTypeNameFilterInput = settings.ElementTypeNameFilterInput ?? string.Empty;
        ElementTypeFilterHistory.Clear();
        foreach (string entry in settings.ElementTypeFilterHistory ?? new List<string>())
        {
            if (!string.IsNullOrWhiteSpace(entry) &&
                !ElementTypeFilterHistory.Any(x => string.Equals(x, entry, StringComparison.OrdinalIgnoreCase)))
            {
                ElementTypeFilterHistory.Add(entry);
            }
        }
        SelectedElementTypeFilterHistoryItem = null;

        AutoLoadOnStartup = settings.AutoLoadOnStartup;
        AutoLoadPlayerNames = settings.AutoLoadPlayerNames;
        LimitToSelectedPlayerConstructs = settings.LimitToSelectedPlayerConstructs;
        IncludeBlueprintOnlySuggestions = settings.IncludeBlueprintOnlySuggestions;
        AutoConnectDatabase = settings.AutoConnectDatabase;
        AutoConnectRetrySeconds = ClampAutoConnectRetrySeconds(settings.AutoConnectRetrySeconds);
        AutoWrapContent = settings.AutoWrapContent;
        AutoCollapseToFirstLevel = settings.AutoCollapseToFirstLevel;
        LuaVersioningEnabled = settings.LuaVersioningEnabled;
        LastSavedFolder = settings.LastSavedFolder ?? string.Empty;

        bool hasPersistedConstructContext =
            !hasExplicitlyClearedPlayerFilter &&
            settings.SelectedConstructSuggestionId.HasValue ||
            (!hasExplicitlyClearedPlayerFilter && !string.IsNullOrWhiteSpace(settings.SelectedConstructSuggestionName)) ||
            (!hasExplicitlyClearedPlayerFilter && !string.IsNullOrWhiteSpace(settings.ConstructIdInput)) ||
            (!hasExplicitlyClearedPlayerFilter && !string.IsNullOrWhiteSpace(settings.ConstructNameSearchInput));

        _restoredConstructSuggestionId = hasExplicitlyClearedPlayerFilter ? null : settings.SelectedConstructSuggestionId;
        _restoredConstructSuggestionName = hasExplicitlyClearedPlayerFilter
            ? string.Empty
            : settings.SelectedConstructSuggestionName ?? string.Empty;
        _restoredConstructSuggestionKind = hasExplicitlyClearedPlayerFilter
            ? ConstructSuggestionKind.Construct
            : settings.SelectedConstructSuggestionKind;
        if (!hasExplicitlyClearedPlayerFilter &&
            hasPersistedConstructContext &&
            !_restoredConstructSuggestionId.HasValue &&
            ulong.TryParse(ConstructIdInput, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong constructIdFromInput))
        {
            _restoredConstructSuggestionId = constructIdFromInput;
        }

        _startupAutoLoadPending = hasPersistedConstructContext && _restoredConstructSuggestionId.HasValue;

        if (_restoredConstructSuggestionId.HasValue && !string.IsNullOrWhiteSpace(_restoredConstructSuggestionName))
        {
            var restoredSuggestion = new ConstructNameLookupRecord(
                _restoredConstructSuggestionId.Value,
                _restoredConstructSuggestionName,
                _restoredConstructSuggestionKind);
            ConstructNameSuggestions.Clear();
            ConstructNameSuggestions.Add(restoredSuggestion);
            SetSelectedConstructSuggestion(restoredSuggestion, suppressAutoLoad: true);
        }

        _selectedElementNodeKey = settings.SelectedElementNodeKey ?? string.Empty;
        _selectedDpuyamlNodeKey = settings.SelectedDpuyamlNodeKey ?? string.Empty;
        _selectedContent2NodeKey = settings.SelectedContent2NodeKey ?? string.Empty;
        _selectedDatabankNodeKey = settings.SelectedDatabankNodeKey ?? string.Empty;
        _gridColumnWidths = new Dictionary<string, string>(settings.GridColumnWidths ?? new(), StringComparer.Ordinal);
        _elementPropertyActiveStates = SanitizeElementPropertyActiveStates(settings.ElementPropertyActiveStates);
        _windowPlacement = CloneWindowPlacement(settings.WindowPlacement);
    }

    private WorkbenchSettings CreateSettingsSnapshot()
    {
        bool hasClearedPlayerFilter = string.IsNullOrWhiteSpace(PlayerIdInput);
        ulong? selectedConstructId = SelectedConstructNameSuggestion?.ConstructId;
        string selectedConstructName = SelectedConstructNameSuggestion?.ConstructName ?? string.Empty;
        ConstructSuggestionKind selectedConstructKind = SelectedConstructNameSuggestion?.Kind ?? ConstructSuggestionKind.Construct;
        return new WorkbenchSettings
        {
            ConstructIdInput = hasClearedPlayerFilter ? string.Empty : ConstructIdInput,
            PlayerIdInput = PlayerIdInput,
            HasPersistedPlayerFilterState = true,
            ConstructNameSearchInput = hasClearedPlayerFilter ? string.Empty : ConstructNameSearchInput,
            PlayerNameSearchInput = PlayerNameSearchInput,
            EndpointTemplateInput = EndpointTemplateInput,
            DbHostInput = DbHostInput,
            ServerRootPathInput = ServerRootPathInput,
            NqUtilsDllPathInput = NqUtilsDllPathInput,
            BlueprintImportEndpointInput = BlueprintImportEndpointInput,
            DbPortInput = DbPortInput,
            DbNameInput = DbNameInput,
            DbUserInput = DbUserInput,
            DbPassword = DbPasswordInput,
            PropertyLimitInput = PropertyLimitInput,
            BlueprintImportAppendDateIfExists = BlueprintImportAppendDateIfExists,
            ElementTypeNameFilterInput = ElementTypeNameFilterInput,
            ElementTypeFilterHistory = ElementTypeFilterHistory.ToList(),
            AutoLoadOnStartup = AutoLoadOnStartup,
            AutoLoadPlayerNames = AutoLoadPlayerNames,
            LimitToSelectedPlayerConstructs = LimitToSelectedPlayerConstructs,
            IncludeBlueprintOnlySuggestions = IncludeBlueprintOnlySuggestions,
            AutoConnectDatabase = AutoConnectDatabase,
            AutoConnectRetrySeconds = ClampAutoConnectRetrySeconds(AutoConnectRetrySeconds),
            AutoWrapContent = AutoWrapContent,
            AutoCollapseToFirstLevel = AutoCollapseToFirstLevel,
            LuaVersioningEnabled = LuaVersioningEnabled,
            LastSavedFolder = LastSavedFolder,
            SelectedConstructSuggestionId = hasClearedPlayerFilter ? null : selectedConstructId,
            SelectedConstructSuggestionName = hasClearedPlayerFilter ? string.Empty : selectedConstructName,
            SelectedConstructSuggestionKind = hasClearedPlayerFilter ? ConstructSuggestionKind.Construct : selectedConstructKind,
            SelectedElementNodeKey = _selectedElementNodeKey,
            SelectedDpuyamlNodeKey = _selectedDpuyamlNodeKey,
            SelectedContent2NodeKey = _selectedContent2NodeKey,
            SelectedDatabankNodeKey = _selectedDatabankNodeKey,
            GridColumnWidths = new Dictionary<string, string>(_gridColumnWidths, StringComparer.Ordinal),
            ElementPropertyActiveStates = SanitizeElementPropertyActiveStates(_elementPropertyActiveStates),
            WindowPlacement = CloneWindowPlacement(_windowPlacement)
        };
    }

    public IReadOnlyDictionary<string, string> GetSavedGridColumnWidths()
    {
        return new Dictionary<string, string>(_gridColumnWidths, StringComparer.Ordinal);
    }

    public void UpdateGridColumnWidths(IReadOnlyDictionary<string, string> widths)
    {
        _gridColumnWidths = new Dictionary<string, string>(widths, StringComparer.Ordinal);
    }

    public WindowPlacementSettings GetSavedWindowPlacement()
    {
        return CloneWindowPlacement(_windowPlacement);
    }

    public void UpdateWindowPlacement(WindowPlacementSettings placement)
    {
        _windowPlacement = CloneWindowPlacement(placement);
    }

    private static WindowPlacementSettings CloneWindowPlacement(WindowPlacementSettings? placement)
    {
        placement ??= new WindowPlacementSettings();
        return new WindowPlacementSettings
        {
            Width = placement.Width,
            Height = placement.Height,
            PositionX = placement.PositionX,
            PositionY = placement.PositionY,
            ScreenKey = placement.ScreenKey ?? string.Empty,
            StartMaximized = placement.StartMaximized
        };
    }

    public void PersistSettingsNow()
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            _settingsService.SaveAsync(CreateSettingsSnapshot(), cts.Token).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch
        {
        }
    }

    private async Task ProbeDatabaseAvailabilityAsync(CancellationToken cancellationToken)
    {
        DatabaseAvailabilityStatus = "Checking...";

        try
        {
            DataConnectionOptions options = BuildDbOptions();
            bool available = await _dataService.IsDatabaseAvailableAsync(
                options,
                TimeSpan.FromSeconds(5),
                cancellationToken);
            DatabaseAvailabilityStatus = available ? "Ok" : "Offline";
            SyncReadyStatusWithConnectionState(available);
        }
        catch
        {
            DatabaseAvailabilityStatus = "Offline";
            SyncReadyStatusWithConnectionState(false);
        }
    }

    private void SyncReadyStatusWithConnectionState(bool isConnected)
    {
        if (isConnected)
        {
            if (string.Equals(StatusMessage, "Checking database connection...", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(StatusMessage, "Database offline.", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(StatusMessage, "Database is offline.", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(StatusMessage, "Ready.", StringComparison.OrdinalIgnoreCase))
            {
                StatusMessage = "Ready.";
            }

            return;
        }

        if (string.Equals(StatusMessage, "Checking database connection...", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(StatusMessage, "Ready.", StringComparison.OrdinalIgnoreCase))
        {
            StatusMessage = "Database offline.";
        }
    }

    private async Task<bool> TryConnectDatabaseAsync(CancellationToken cancellationToken, bool initiatedByAutoConnect)
    {
        bool lockTaken = false;
        try
        {
            await _databaseConnectGate.WaitAsync(cancellationToken);
            lockTaken = true;
            await ProbeDatabaseAvailabilityAsync(cancellationToken);
            bool connected = IsDatabaseOnline();
            if (connected)
            {
                StatusMessage = initiatedByAutoConnect
                    ? "Database auto-connect succeeded."
                    : "Database connection is available.";
            }

            return connected;
        }
        catch (OperationCanceledException)
        {
            return IsDatabaseOnline();
        }
        finally
        {
            if (lockTaken)
            {
                _databaseConnectGate.Release();
            }

            RefreshAutoConnectLoopState();
        }
    }

    private void RefreshAutoConnectLoopState()
    {
        if (!AutoConnectDatabase || IsDatabaseOnline())
        {
            StopAutoConnectLoop();
            return;
        }

        if (string.Equals(DatabaseAvailabilityStatus, "Checking...", StringComparison.OrdinalIgnoreCase))
        {
            AutoConnectNextRetrySeconds = null;
            return;
        }

        StartAutoConnectLoop();
    }

    private void StartAutoConnectLoop()
    {
        if (_autoConnectCts is not null)
        {
            return;
        }

        var cts = new CancellationTokenSource();
        _autoConnectCts = cts;
        _ = RunAutoConnectLoopAsync(cts);
    }

    private void StopAutoConnectLoop()
    {
        CancellationTokenSource? cts = _autoConnectCts;
        if (cts is null)
        {
            AutoConnectNextRetrySeconds = null;
            return;
        }

        _autoConnectCts = null;
        AutoConnectNextRetrySeconds = null;
        cts.Cancel();
    }

    private async Task RunAutoConnectLoopAsync(CancellationTokenSource cts)
    {
        try
        {
            while (!cts.IsCancellationRequested)
            {
                int retrySeconds = ClampAutoConnectRetrySeconds(AutoConnectRetrySeconds);
                for (int remaining = retrySeconds; remaining > 0; remaining--)
                {
                    AutoConnectNextRetrySeconds = remaining;
                    await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

                    if (cts.IsCancellationRequested || !AutoConnectDatabase || IsDatabaseOnline())
                    {
                        break;
                    }
                }

                AutoConnectNextRetrySeconds = null;

                if (cts.IsCancellationRequested || !AutoConnectDatabase || IsDatabaseOnline())
                {
                    break;
                }

                bool connected = await TryConnectDatabaseAsync(cts.Token, initiatedByAutoConnect: true);
                if (connected)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (ReferenceEquals(_autoConnectCts, cts))
            {
                _autoConnectCts = null;
            }

            AutoConnectNextRetrySeconds = null;
            cts.Dispose();
        }
    }

    private bool IsDatabaseOnline()
    {
        return string.Equals(DatabaseAvailabilityStatus, "Ok", StringComparison.OrdinalIgnoreCase);
    }

    private string BuildDatabaseAvailabilityDisplay()
    {
        if (!string.Equals(DatabaseAvailabilityStatus, "Offline", StringComparison.OrdinalIgnoreCase))
        {
            return DatabaseAvailabilityStatus;
        }

        if (!AutoConnectDatabase || !AutoConnectNextRetrySeconds.HasValue)
        {
            return "Offline";
        }

        return $"Offline (next check in {AutoConnectNextRetrySeconds.Value}s)";
    }

    private static int ClampAutoConnectRetrySeconds(int value)
    {
        if (value < AutoConnectRetryMinSeconds)
        {
            return AutoConnectRetryMinSeconds;
        }

        if (value > AutoConnectRetryMaxSeconds)
        {
            return AutoConnectRetryMaxSeconds;
        }

        return value;
    }

    private static int ParsePort(string? value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) || parsed <= 0)
        {
            throw new InvalidOperationException("DB port must be a positive integer.");
        }

        return parsed;
    }

    private static int ParsePropertyLimit(string? value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) || parsed <= 0)
        {
            throw new InvalidOperationException("Property limit must be a positive integer.");
        }

        return parsed;
    }

    private static ulong ParseRequiredConstructId(string? input)
    {
        ulong? value = TryParseOptionalUlong(input);
        if (!value.HasValue)
        {
            throw new InvalidOperationException("Construct id is required.");
        }

        return value.Value;
    }

    private static ulong? TryParseOptionalUlong(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return null;
        }

        return ulong.TryParse(input.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed)
            ? parsed
            : throw new InvalidOperationException($"Invalid unsigned integer value: {input}");
    }

    private static ulong ParseOptionalUnsignedOrDefault(string? input, ulong fallback = 0UL)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        return ulong.TryParse(input.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed)
            ? parsed
            : fallback;
    }

    private static Uri BuildEndpointUri(string template, ulong constructId)
    {
        if (string.IsNullOrWhiteSpace(template))
        {
            throw new InvalidOperationException("Endpoint template cannot be empty.");
        }

        string candidate = template.Replace("{id}", constructId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);

        if (!Uri.TryCreate(candidate, UriKind.Absolute, out Uri? uri))
        {
            throw new InvalidOperationException("Endpoint URI is invalid.");
        }

        return uri;
    }

}
