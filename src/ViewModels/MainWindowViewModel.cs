// Helper Index:
// - LoadDatabaseAsync: Loads DB construct snapshot, categorizes properties, and refreshes tree models.
// - ProbeEndpointAsync: Probes endpoint payloads and updates decoded transport summaries.
// - BuildGetConstructDataExportJson: Produces export JSON merging endpoint data with DB fallback values.
// - QueueConstructNameSearch: Debounces construct-name search and schedules async suggestion refresh.
// - TryGetSelectedLuaBlobSaveRequest: Builds save metadata for currently selected LUA blob node.
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
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorker.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private const string LuaPartNodeKindPrefix = "Part:";
    private const int AutoConnectRetryMinSeconds = 10;
    private const int AutoConnectRetryMaxSeconds = 9999;
    private const int AutoConnectRetryDefaultSeconds = 30;

    public sealed record BlobSaveRequest(string SuggestedFileName, string Content, string DefaultExtension);
    public sealed record LuaEditorSourceContext(
        ulong? ElementId,
        string ElementDisplayName,
        string NodeLabel,
        string PropertyName,
        string SuggestedFileName);

    private readonly MyDuDataService _dataService = new();
    private readonly WorkbenchSettingsService _settingsService = new();
    private readonly SemaphoreSlim _databaseConnectGate = new(1, 1);
    private DatabaseConstructSnapshot? _lastSnapshot;
    private EndpointProbeResult? _lastEndpointResult;
    private CancellationTokenSource? _autoConnectCts;
    private CancellationTokenSource? _constructSearchCts;
    private CancellationTokenSource? _playerSearchCts;
    private bool _isRestoringSettings;
    private bool _isStartupInitializing;
    private bool _suppressConstructSelectionAutoLoad;
    private bool _startupAutoLoadPending;
    private ulong? _restoredConstructSuggestionId;
    private string _restoredConstructSuggestionName = string.Empty;
    private string _selectedElementNodeKey = string.Empty;
    private string _selectedDpuyamlNodeKey = string.Empty;
    private string _selectedContent2NodeKey = string.Empty;
    private string _selectedDatabankNodeKey = string.Empty;
    private Dictionary<string, string> _gridColumnWidths = new(StringComparer.Ordinal);
    private readonly List<ElementPropertyRecord> _allRegularProperties = new();
    private readonly List<PlayerNameLookupRecord> _allPlayerNameSuggestions = new();
    private Dictionary<string, bool> _elementPropertyActiveStates = new(StringComparer.OrdinalIgnoreCase);
    private bool _isBulkUpdatingElementPropertyFilters;
    private WindowPlacementSettings _windowPlacement = new();

    public ObservableCollection<ElementPropertyRecord> ElementProperties { get; } = new();
    public ObservableCollection<ElementPropertyRecord> Dpuyaml6Properties { get; } = new();
    public ObservableCollection<ElementPropertyRecord> Content2Properties { get; } = new();
    public ObservableCollection<ElementPropertyRecord> DatabankProperties { get; } = new();
    public ObservableCollection<PropertyFilterRecord> ElementPropertyFilters { get; } = new();
    public ObservableCollection<string> ElementTypeFilterHistory { get; } = new();
    public ObservableCollection<ConstructNameLookupRecord> ConstructNameSuggestions { get; } = new();
    public ObservableCollection<PlayerNameLookupRecord> PlayerNameSuggestions { get; } = new();
    public HierarchicalModel<PropertyTreeRow> ElementPropertiesModel { get; }
    public HierarchicalModel<PropertyTreeRow> Dpuyaml6Model { get; }
    public HierarchicalModel<PropertyTreeRow> Content2Model { get; }
    public HierarchicalModel<PropertyTreeRow> DatabankModel { get; }

    [ObservableProperty]
    private string constructIdInput = "1000061";

    [ObservableProperty]
    private string constructNameSearchInput = string.Empty;

    [ObservableProperty]
    private ConstructNameLookupRecord? selectedConstructNameSuggestion;

    [ObservableProperty]
    private string constructSearchStatus = string.Empty;

    [ObservableProperty]
    private string playerIdInput = "10000";

    [ObservableProperty]
    private string playerNameSearchInput = string.Empty;

    [ObservableProperty]
    private PlayerNameLookupRecord? selectedPlayerNameSuggestion;

    [ObservableProperty]
    private string playerSearchStatus = string.Empty;

    [ObservableProperty]
    private string endpointTemplateInput = "http://[::1]:12003/constructs/{id}/info";

    [ObservableProperty]
    private string dbHostInput = "127.0.0.1";

    [ObservableProperty]
    private string serverRootPathInput = @"D:\MyDUserver";

    [ObservableProperty]
    private string dbPortInput = "5432";

    [ObservableProperty]
    private string dbNameInput = "dual";

    [ObservableProperty]
    private string dbUserInput = "dual";

    [ObservableProperty]
    private string dbPasswordInput = "dual";

    [ObservableProperty]
    private string propertyLimitInput = "300";

    [ObservableProperty]
    private string elementTypeNameFilterInput = string.Empty;

    [ObservableProperty]
    private string? selectedElementTypeFilterHistoryItem;

    [ObservableProperty]
    private bool damagedOnly;

    [ObservableProperty]
    private bool autoLoadOnStartup = true;

    [ObservableProperty]
    private bool autoLoadPlayerNames = true;

    [ObservableProperty]
    private bool limitToSelectedPlayerConstructs = true;

    [ObservableProperty]
    private bool autoConnectDatabase;

    [ObservableProperty]
    private int autoConnectRetrySeconds = AutoConnectRetryDefaultSeconds;

    [ObservableProperty]
    private bool autoWrapContent;

    [ObservableProperty]
    private bool autoCollapseToFirstLevel;

    [ObservableProperty]
    private bool luaVersioningEnabled;

    [ObservableProperty]
    private string lastSavedFolder = string.Empty;

    [ObservableProperty]
    private string statusMessage = "Checking database connection...";

    [ObservableProperty]
    private string activeConstructName = string.Empty;

    [ObservableProperty]
    private string databaseAvailabilityStatus = "Checking...";

    [ObservableProperty]
    private int? autoConnectNextRetrySeconds;

    [ObservableProperty]
    private string databaseSummary = string.Empty;

    [ObservableProperty]
    private string endpointSummary = string.Empty;

    [ObservableProperty]
    private string endpointRawPreview = string.Empty;

    [ObservableProperty]
    private object? selectedDpuyaml6Node;

    [ObservableProperty]
    private string selectedDpuyaml6Content = string.Empty;

    [ObservableProperty]
    private object? selectedContent2Node;

    [ObservableProperty]
    private string selectedContent2Content = string.Empty;

    [ObservableProperty]
    private object? selectedDatabankNode;

    [ObservableProperty]
    private string selectedDatabankContent = string.Empty;

    [ObservableProperty]
    private object? selectedElementPropertyNode;

    [ObservableProperty]
    private bool isBusy;

    [ObservableProperty]
    private bool repairInProgress;

    [ObservableProperty]
    private double repairProgressPercent;

    [ObservableProperty]
    private string repairStatusText = "Repair: idle";

    public TextWrapping ContentTextWrapping => AutoWrapContent ? TextWrapping.Wrap : TextWrapping.NoWrap;
    public bool CanSaveSelectedLuaBlob => IsLuaSaveNode(ResolveSelectedTreeRow(SelectedDpuyaml6Node));
    public bool CanSaveSelectedHtmlRsBlob => IsMainBlobNode(ResolveSelectedTreeRow(SelectedContent2Node));
    public bool CanSaveSelectedDatabankBlob => IsMainBlobNode(ResolveSelectedTreeRow(SelectedDatabankNode));
    public bool CanRepairDestroyedElements => !IsBusy && !RepairInProgress && _lastSnapshot is not null && IsDatabaseOnline();
    public string DatabaseAvailabilityDisplay => BuildDatabaseAvailabilityDisplay();
    public string SelectedPlayerIdDisplay => SelectedPlayerNameSuggestion?.PlayerId?.ToString(CultureInfo.InvariantCulture) ?? "-";

    public MainWindowViewModel()
    {
        _isStartupInitializing = true;
        ElementPropertiesModel = CreateTreeModel();
        Dpuyaml6Model = CreateTreeModel();
        Content2Model = CreateTreeModel();
        DatabankModel = CreateTreeModel();

        ElementPropertiesModel.SetRoots(Array.Empty<PropertyTreeRow>());
        Dpuyaml6Model.SetRoot(CreateRootNode("LUA blocks"));
        Content2Model.SetRoot(CreateRootNode("HTML/RS"));
        DatabankModel.SetRoot(CreateRootNode("Databank"));

        RestoreSettingsFromDisk();
        _ = InitializeStartupAsync();
    }

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

            List<ElementPropertyRecord> regularProperties = new();
            List<ElementPropertyRecord> dpuyamlProperties = new();
            List<ElementPropertyRecord> content2Properties = new();
            List<ElementPropertyRecord> databankProperties = new();

            foreach (ElementPropertyRecord record in snapshot.Properties)
            {
                if (string.Equals(record.Name, "dpuyaml_6", StringComparison.OrdinalIgnoreCase))
                {
                    dpuyamlProperties.Add(record);
                }
                else if (string.Equals(record.Name, "content_2", StringComparison.OrdinalIgnoreCase))
                {
                    content2Properties.Add(record);
                }
                else if (string.Equals(record.Name, "databank", StringComparison.OrdinalIgnoreCase))
                {
                    databankProperties.Add(record);
                }
                else
                {
                    regularProperties.Add(record);
                }
            }

            _allRegularProperties.Clear();
            _allRegularProperties.AddRange(regularProperties);
            RebuildPropertyFilterRows(regularProperties);
            ApplyElementPropertyFilter();

            Dpuyaml6Properties.Clear();
            foreach (ElementPropertyRecord record in dpuyamlProperties)
            {
                Dpuyaml6Properties.Add(record);
            }

            Content2Properties.Clear();
            foreach (ElementPropertyRecord record in content2Properties)
            {
                Content2Properties.Add(record);
            }

            DatabankProperties.Clear();
            foreach (ElementPropertyRecord record in databankProperties)
            {
                DatabankProperties.Add(record);
            }

            RebuildCodeBlockTree(Dpuyaml6Model, dpuyamlProperties, BuildLuaPartRows, "LUA blocks");
            RebuildCodeBlockTree(Content2Model, content2Properties, BuildContentPartRows, "HTML/RS");
            RebuildCodeBlockTree(DatabankModel, databankProperties, BuildDatabankPartRows, "Databank");

            if (AutoCollapseToFirstLevel)
            {
                ElementPropertiesModel.CollapseAll(minDepth: 0);
                Dpuyaml6Model.CollapseAll(minDepth: 1);
                Content2Model.CollapseAll(minDepth: 1);
                DatabankModel.CollapseAll(minDepth: 1);
            }

            SelectedElementPropertyNode = FindNodeBySelectionKey(ElementPropertiesModel, _selectedElementNodeKey);
            SelectedDpuyaml6Node = FindNodeBySelectionKey(Dpuyaml6Model, _selectedDpuyamlNodeKey);
            SelectedContent2Node = FindNodeBySelectionKey(Content2Model, _selectedContent2NodeKey);
            SelectedDatabankNode = FindNodeBySelectionKey(DatabankModel, _selectedDatabankNodeKey);

            if (SelectedDpuyaml6Node is null)
            {
                SelectedDpuyaml6Content = string.Empty;
            }

            if (SelectedContent2Node is null)
            {
                SelectedContent2Content = string.Empty;
            }

            if (SelectedDatabankNode is null)
            {
                SelectedDatabankContent = string.Empty;
            }

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
        if (!string.IsNullOrWhiteSpace(settings.ConstructIdInput))
        {
            ConstructIdInput = settings.ConstructIdInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.PlayerIdInput))
        {
            PlayerIdInput = settings.PlayerIdInput;
        }

        if (!string.IsNullOrWhiteSpace(settings.ConstructNameSearchInput))
        {
            ConstructNameSearchInput = settings.ConstructNameSearchInput;
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
        AutoConnectDatabase = settings.AutoConnectDatabase;
        AutoConnectRetrySeconds = ClampAutoConnectRetrySeconds(settings.AutoConnectRetrySeconds);
        AutoWrapContent = settings.AutoWrapContent;
        AutoCollapseToFirstLevel = settings.AutoCollapseToFirstLevel;
        LuaVersioningEnabled = settings.LuaVersioningEnabled;
        LastSavedFolder = settings.LastSavedFolder ?? string.Empty;

        bool hasPersistedConstructContext =
            settings.SelectedConstructSuggestionId.HasValue ||
            !string.IsNullOrWhiteSpace(settings.SelectedConstructSuggestionName) ||
            !string.IsNullOrWhiteSpace(settings.ConstructIdInput) ||
            !string.IsNullOrWhiteSpace(settings.ConstructNameSearchInput);

        _restoredConstructSuggestionId = settings.SelectedConstructSuggestionId;
        _restoredConstructSuggestionName = settings.SelectedConstructSuggestionName ?? string.Empty;
        if (hasPersistedConstructContext &&
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
                _restoredConstructSuggestionName);
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
        ulong? selectedConstructId = SelectedConstructNameSuggestion?.ConstructId;
        string selectedConstructName = SelectedConstructNameSuggestion?.ConstructName ?? string.Empty;
        return new WorkbenchSettings
        {
            ConstructIdInput = ConstructIdInput,
            PlayerIdInput = PlayerIdInput,
            ConstructNameSearchInput = ConstructNameSearchInput,
            PlayerNameSearchInput = PlayerNameSearchInput,
            EndpointTemplateInput = EndpointTemplateInput,
            DbHostInput = DbHostInput,
            ServerRootPathInput = ServerRootPathInput,
            DbPortInput = DbPortInput,
            DbNameInput = DbNameInput,
            DbUserInput = DbUserInput,
            DbPassword = DbPasswordInput,
            PropertyLimitInput = PropertyLimitInput,
            ElementTypeNameFilterInput = ElementTypeNameFilterInput,
            ElementTypeFilterHistory = ElementTypeFilterHistory.ToList(),
            AutoLoadOnStartup = AutoLoadOnStartup,
            AutoLoadPlayerNames = AutoLoadPlayerNames,
            LimitToSelectedPlayerConstructs = LimitToSelectedPlayerConstructs,
            AutoConnectDatabase = AutoConnectDatabase,
            AutoConnectRetrySeconds = ClampAutoConnectRetrySeconds(AutoConnectRetrySeconds),
            AutoWrapContent = AutoWrapContent,
            AutoCollapseToFirstLevel = AutoCollapseToFirstLevel,
            LuaVersioningEnabled = LuaVersioningEnabled,
            LastSavedFolder = LastSavedFolder,
            SelectedConstructSuggestionId = selectedConstructId,
            SelectedConstructSuggestionName = selectedConstructName,
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

    private static HierarchicalModel<PropertyTreeRow> CreateTreeModel()
    {
        var options = new HierarchicalOptions<PropertyTreeRow>
        {
            ItemsSelector = row => row.Children,
            IsLeafSelector = row => row.Children.Count == 0,
            AutoExpandRoot = true,
            MaxAutoExpandDepth = 1,
            VirtualizeChildren = true
        };

        return new HierarchicalModel<PropertyTreeRow>(options);
    }

    private static PropertyTreeRow CreateRootNode(string label)
    {
        return new PropertyTreeRow(
            label,
            "Root",
            null,
            string.Empty,
            string.Empty,
            null,
            null,
            string.Empty,
            string.Empty);
    }

    private static Dictionary<string, bool> SanitizeElementPropertyActiveStates(IEnumerable<KeyValuePair<string, bool>>? source)
    {
        var sanitized = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        if (source is null)
        {
            return sanitized;
        }

        foreach (KeyValuePair<string, bool> entry in source)
        {
            string propertyName = NormalizePropertyName(entry.Key);
            if (string.IsNullOrWhiteSpace(propertyName))
            {
                continue;
            }

            sanitized[propertyName] = entry.Value;
        }

        return sanitized;
    }

    private static string NormalizePropertyName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var sb = new StringBuilder(value.Length);
        foreach (char ch in value)
        {
            if (char.IsControl(ch))
            {
                continue;
            }

            sb.Append(ch);
        }

        return sb.ToString().Trim();
    }

    private static bool IsElementNameProperty(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "name",
            StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsDestroyedPropertyName(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "destroyed",
            StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsRestoreCountPropertyName(string? propertyName)
    {
        return string.Equals(
            NormalizePropertyName(propertyName),
            "restoreCount",
            StringComparison.OrdinalIgnoreCase);
    }

    private void RebuildPropertyFilterRows(IReadOnlyList<ElementPropertyRecord> records)
    {
        _elementPropertyActiveStates = SanitizeElementPropertyActiveStates(_elementPropertyActiveStates);
        foreach (PropertyFilterRecord row in ElementPropertyFilters)
        {
            row.PropertyChanged -= OnElementPropertyFilterChanged;
        }

        ElementPropertyFilters.Clear();

        string[] propertyNames = records
            .Select(r => NormalizePropertyName(r.Name))
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Where(n => !IsElementNameProperty(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var validNames = new HashSet<string>(propertyNames, StringComparer.OrdinalIgnoreCase);
        string[] staleNames = _elementPropertyActiveStates.Keys
            .Where(name => !validNames.Contains(name))
            .ToArray();
        foreach (string staleName in staleNames)
        {
            _elementPropertyActiveStates.Remove(staleName);
        }

        foreach (string propertyName in propertyNames)
        {
            bool isActive = !_elementPropertyActiveStates.TryGetValue(propertyName, out bool savedState) || savedState;
            _elementPropertyActiveStates[propertyName] = isActive;
            var filterRow = new PropertyFilterRecord(propertyName, isActive);
            filterRow.PropertyChanged += OnElementPropertyFilterChanged;
            ElementPropertyFilters.Add(filterRow);
        }
    }

    private void OnElementPropertyFilterChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (!string.Equals(e.PropertyName, nameof(PropertyFilterRecord.IsActive), StringComparison.Ordinal))
        {
            return;
        }

        if (sender is not PropertyFilterRecord row)
        {
            return;
        }

        string propertyName = NormalizePropertyName(row.PropertyName);
        if (string.IsNullOrWhiteSpace(propertyName))
        {
            return;
        }

        _elementPropertyActiveStates[propertyName] = row.IsActive;
        if (_isBulkUpdatingElementPropertyFilters)
        {
            return;
        }

        ApplyElementPropertyFilter();
        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            PersistSettingsNow();
        }
    }

    private void SetAllElementPropertyFilters(bool isActive)
    {
        if (ElementPropertyFilters.Count == 0)
        {
            return;
        }

        bool changed = false;
        _isBulkUpdatingElementPropertyFilters = true;
        try
        {
            foreach (PropertyFilterRecord row in ElementPropertyFilters)
            {
                if (row.IsActive == isActive)
                {
                    continue;
                }

                row.IsActive = isActive;
                changed = true;
            }
        }
        finally
        {
            _isBulkUpdatingElementPropertyFilters = false;
        }

        if (!changed)
        {
            return;
        }

        ApplyElementPropertyFilter();
        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            PersistSettingsNow();
        }
    }

    private void ApplyElementPropertyFilter()
    {
        HashSet<string> activeNames = BuildActivePropertyNameSet();
        string elementTypeFilter = ElementTypeNameFilterInput?.Trim() ?? string.Empty;
        HashSet<ulong>? damagedElementIds = DamagedOnly
            ? BuildDamagedElementIdSet(_allRegularProperties)
            : null;

        List<ElementPropertyRecord> filtered = _allRegularProperties
            .Where(r => !IsElementNameProperty(r.Name) &&
                        activeNames.Contains(NormalizePropertyName(r.Name)) &&
                        (damagedElementIds is null || damagedElementIds.Contains(r.ElementId)) &&
                        MatchesElementTypeFilter(DeriveElementTypeName(r.ElementDisplayName), elementTypeFilter))
            .ToList();

        ElementProperties.Clear();
        foreach (ElementPropertyRecord record in filtered)
        {
            ElementProperties.Add(record);
        }

        RebuildElementPropertiesTree(_allRegularProperties, activeNames, elementTypeFilter, damagedElementIds);
        SelectedElementPropertyNode = FindNodeBySelectionKey(ElementPropertiesModel, _selectedElementNodeKey);
    }

    private HashSet<string> BuildActivePropertyNameSet()
    {
        return ElementPropertyFilters
            .Where(f => f.IsActive)
            .Select(f => NormalizePropertyName(f.PropertyName))
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private void AddElementTypeFilterHistory(string? filterText)
    {
        string normalized = filterText?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return;
        }

        int existingIndex = -1;
        for (int i = 0; i < ElementTypeFilterHistory.Count; i++)
        {
            if (string.Equals(ElementTypeFilterHistory[i], normalized, StringComparison.OrdinalIgnoreCase))
            {
                existingIndex = i;
                break;
            }
        }

        if (existingIndex >= 0)
        {
            ElementTypeFilterHistory.RemoveAt(existingIndex);
        }

        ElementTypeFilterHistory.Insert(0, normalized);
    }

    private void RebuildElementPropertiesTree(
        IReadOnlyList<ElementPropertyRecord> records,
        HashSet<string> activePropertyNames,
        string elementTypeFilter,
        HashSet<ulong>? damagedElementIds)
    {
        var typeRoots = new List<PropertyTreeRow>();
        foreach (IGrouping<string, ElementPropertyRecord> byType in records
                     .GroupBy(r => DeriveElementTypeName(r.ElementDisplayName), StringComparer.OrdinalIgnoreCase)
                     .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase))
        {
            if (!MatchesElementTypeFilter(byType.Key, elementTypeFilter))
            {
                continue;
            }

            var elementNodes = new List<PropertyTreeRow>();

            foreach (IGrouping<ulong, ElementPropertyRecord> byElement in byType
                         .OrderBy(r => r.ElementId)
                         .GroupBy(r => r.ElementId))
            {
                if (damagedElementIds is not null && !damagedElementIds.Contains(byElement.Key))
                {
                    continue;
                }

                ElementPropertyRecord first = byElement.First();
                string elementName = ResolveElementName(byElement);
                int totalElementProperties = byElement.Count(p => !IsElementNameProperty(p.Name));
                List<ElementPropertyRecord> visibleProperties = byElement
                    .Where(p => !IsElementNameProperty(p.Name) &&
                                activePropertyNames.Contains(NormalizePropertyName(p.Name)))
                    .OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var elementNode = new PropertyTreeRow(
                    first.ElementDisplayName,
                    "Element",
                    first.ElementId,
                    first.ElementDisplayName,
                    string.Empty,
                    null,
                    null,
                    $"{visibleProperties.Count}/{totalElementProperties} properties",
                    string.Empty,
                    elementName);

                foreach (ElementPropertyRecord property in visibleProperties)
                {
                    elementNode.Children.Add(CreatePropertyLeaf(property, property.Name, "Property", elementName));
                }

                elementNodes.Add(elementNode);
            }

            if (elementNodes.Count == 0)
            {
                continue;
            }

            var typeNode = new PropertyTreeRow(
                byType.Key,
                "Element Type",
                null,
                byType.Key,
                string.Empty,
                null,
                null,
                $"{elementNodes.Count} elements",
                string.Empty);

            foreach (PropertyTreeRow elementNode in elementNodes)
            {
                typeNode.Children.Add(elementNode);
            }

            typeRoots.Add(typeNode);
        }

        ElementPropertiesModel.SetRoots(typeRoots);
    }

    private static HashSet<ulong> BuildDamagedElementIdSet(IReadOnlyList<ElementPropertyRecord> records)
    {
        var damaged = new HashSet<ulong>();
        foreach (IGrouping<ulong, ElementPropertyRecord> byElement in records.GroupBy(r => r.ElementId))
        {
            bool hasDestroyedTrue = byElement.Any(p =>
                IsDestroyedPropertyName(p.Name) &&
                TryReadBooleanTrue(p.DecodedValue));
            if (hasDestroyedTrue)
            {
                damaged.Add(byElement.Key);
                continue;
            }

            bool hasRestoreCountPositive = byElement.Any(p =>
                IsRestoreCountPropertyName(p.Name) &&
                TryReadPositiveNumber(p.DecodedValue));
            if (hasRestoreCountPositive)
            {
                damaged.Add(byElement.Key);
            }
        }

        return damaged;
    }

    private static bool TryReadBooleanTrue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string normalized = value.Trim();
        if (string.Equals(normalized, "1", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(normalized, "0", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return bool.TryParse(normalized, out bool parsed) && parsed;
    }

    private static bool TryReadPositiveNumber(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string normalized = value.Trim();
        if (long.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
        {
            return longValue > 0;
        }

        return double.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue) &&
               doubleValue > 0d;
    }

    private static string ResolveElementName(IEnumerable<ElementPropertyRecord> properties)
    {
        ElementPropertyRecord? nameProperty = properties
            .FirstOrDefault(p => IsElementNameProperty(p.Name) && !string.IsNullOrWhiteSpace(p.DecodedValue));

        return nameProperty?.DecodedValue?.Trim() ?? string.Empty;
    }

    private void ApplyRepairToLoadedSnapshot()
    {
        if (_lastSnapshot is null)
        {
            return;
        }

        _allRegularProperties.RemoveAll(record =>
            IsDestroyedPropertyName(record.Name) ||
            IsRestoreCountPropertyName(record.Name));

        IReadOnlyList<ElementPropertyRecord> snapshotProperties = _lastSnapshot.Properties
            .Where(record =>
                !IsDestroyedPropertyName(record.Name) &&
                !IsRestoreCountPropertyName(record.Name))
            .ToList();

        _lastSnapshot = _lastSnapshot with { Properties = snapshotProperties };
        RebuildPropertyFilterRows(_allRegularProperties);
        ApplyElementPropertyFilter();
    }

    private static bool MatchesElementTypeFilter(string elementTypeName, string wildcardFilter)
    {
        if (string.IsNullOrWhiteSpace(wildcardFilter))
        {
            return true;
        }

        return MatchesWildcardPattern(elementTypeName, wildcardFilter);
    }

    private static string DeriveElementTypeName(string elementDisplayName)
    {
        if (string.IsNullOrWhiteSpace(elementDisplayName))
        {
            return string.Empty;
        }

        int bracketStart = elementDisplayName.LastIndexOf(" [", StringComparison.Ordinal);
        if (bracketStart < 0 || !elementDisplayName.EndsWith("]", StringComparison.Ordinal))
        {
            return elementDisplayName;
        }

        int idStart = bracketStart + 2;
        int idLength = elementDisplayName.Length - idStart - 1;
        if (idLength <= 0)
        {
            return elementDisplayName;
        }

        ReadOnlySpan<char> idSpan = elementDisplayName.AsSpan(idStart, idLength);
        for (int i = 0; i < idSpan.Length; i++)
        {
            if (!char.IsDigit(idSpan[i]))
            {
                return elementDisplayName;
            }
        }

        return elementDisplayName[..bracketStart];
    }

    private static void RebuildCodeBlockTree(
        HierarchicalModel<PropertyTreeRow> model,
        IReadOnlyList<ElementPropertyRecord> records,
        Func<ElementPropertyRecord, IReadOnlyList<PropertyTreeRow>> partBuilder,
        string rootLabel)
    {
        PropertyTreeRow root = CreateRootNode(rootLabel);
        foreach (ElementPropertyRecord record in records
                     .OrderBy(r => r.ElementId)
                     .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase))
        {
            string blockLabel = string.IsNullOrWhiteSpace(record.ElementDisplayName)
                ? $"Element {record.ElementId.ToString(CultureInfo.InvariantCulture)}"
                : record.ElementDisplayName;
            var blockNode = CreatePropertyLeaf(record, blockLabel, "Block");
            IReadOnlyList<PropertyTreeRow> parts = partBuilder(record);
            foreach (PropertyTreeRow part in parts)
            {
                blockNode.Children.Add(part);
            }

            root.Children.Add(blockNode);
        }

        model.SetRoot(root);
    }

    private static PropertyTreeRow CreatePropertyLeaf(
        ElementPropertyRecord record,
        string nodeLabel,
        string nodeKind,
        string elementName = "")
    {
        return new PropertyTreeRow(
            nodeLabel,
            nodeKind,
            record.ElementId,
            record.ElementDisplayName,
            record.Name,
            record.PropertyType,
            record.ByteLength,
            BuildPreview(record.DecodedValue),
            record.DecodedValue,
            elementName);
    }

    private static IReadOnlyList<PropertyTreeRow> BuildLuaPartRows(ElementPropertyRecord record)
    {
        IReadOnlyList<(string Title, string Content)> sections = SplitLuaSections(record.DecodedValue);
        var rows = new List<PropertyTreeRow>(sections.Count > 0 ? sections.Count : 1);
        if (sections.Count == 0)
        {
            rows.Add(new PropertyTreeRow(
                "part_001",
                "Part",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                BuildPreview(record.DecodedValue),
                record.DecodedValue));
            return rows;
        }

        var groupedSections = new Dictionary<string, List<(string EventLabel, string Content)>>(StringComparer.Ordinal);
        var componentDisplayByKey = new Dictionary<string, string>(StringComparer.Ordinal);
        int index = 0;
        foreach ((string title, string content) in sections)
        {
            index++;
            (string componentDisplay, string eventLabel) = SplitLuaSectionTitle(title, index);
            string componentKey = NormalizeLuaComponentKey(componentDisplay);
            if (!groupedSections.TryGetValue(componentKey, out List<(string EventLabel, string Content)>? componentRows))
            {
                componentRows = new List<(string EventLabel, string Content)>();
                groupedSections[componentKey] = componentRows;
                componentDisplayByKey[componentKey] = componentDisplay;
            }

            componentRows.Add((eventLabel, content));
        }

        foreach (string componentKey in OrderLuaComponentKeys(groupedSections.Keys))
        {
            string componentDisplay = componentDisplayByKey[componentKey];
            List<(string EventLabel, string Content)> componentSections = groupedSections[componentKey];
            var componentNode = new PropertyTreeRow(
                componentDisplay,
                "Component",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                $"{componentSections.Count.ToString(CultureInfo.InvariantCulture)} handlers",
                string.Empty);

            foreach ((string eventLabel, string content) in componentSections)
            {
                componentNode.Children.Add(new PropertyTreeRow(
                    eventLabel,
                    LuaPartNodeKindPrefix + componentKey,
                    record.ElementId,
                    record.ElementDisplayName,
                    record.Name,
                    record.PropertyType,
                    record.ByteLength,
                    BuildPreview(content),
                    content));
            }

            rows.Add(componentNode);
        }

        return rows;
    }

    private static (string ComponentDisplay, string EventLabel) SplitLuaSectionTitle(string title, int index)
    {
        string normalized = title?.Trim() ?? string.Empty;
        if (normalized.Length == 0)
        {
            return ("misc", $"handler_{index:000}");
        }

        int separatorIndex = normalized.IndexOf(" / ", StringComparison.Ordinal);
        if (separatorIndex > 0 && separatorIndex + 3 < normalized.Length)
        {
            string component = normalized[..separatorIndex].Trim();
            string eventLabel = normalized[(separatorIndex + 3)..].Trim();
            if (component.Length > 0 && eventLabel.Length > 0)
            {
                return (component, eventLabel);
            }
        }

        return ("misc", normalized);
    }

    private static string NormalizeLuaComponentKey(string componentDisplay)
    {
        string normalized = (componentDisplay ?? string.Empty).Trim().ToLowerInvariant();
        normalized = Regex.Replace(normalized, "\\s+", " ");
        return string.IsNullOrWhiteSpace(normalized) ? "misc" : normalized;
    }

    private static IReadOnlyList<string> OrderLuaComponentKeys(IEnumerable<string> componentKeys)
    {
        return componentKeys
            .Distinct(StringComparer.Ordinal)
            .OrderBy(GetLuaComponentSortRank)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static int GetLuaComponentSortRank(string key)
    {
        return key switch
        {
            "library" => 0,
            "system" => 1,
            "player" => 2,
            "construct" => 3,
            "unit" => 4,
            _ => TryGetSlotSortRank(key)
        };
    }

    private static int TryGetSlotSortRank(string key)
    {
        if (!key.StartsWith("slot", StringComparison.OrdinalIgnoreCase))
        {
            return 1000;
        }

        ReadOnlySpan<char> suffix = key.AsSpan(4).Trim();
        if (!int.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out int slotNumber) ||
            slotNumber <= 0)
        {
            return 1000;
        }

        return 100 + slotNumber;
    }

    private static IReadOnlyList<PropertyTreeRow> BuildContentPartRows(ElementPropertyRecord record)
    {
        return Array.Empty<PropertyTreeRow>();
    }

    private static IReadOnlyList<PropertyTreeRow> BuildDatabankPartRows(ElementPropertyRecord record)
    {
        if (!TryParseDatabankJson(record.DecodedValue, out JsonDocument? document) || document is null)
        {
            return new[]
            {
                new PropertyTreeRow(
                    "databank",
                    "Part",
                    record.ElementId,
                    record.ElementDisplayName,
                    record.Name,
                    record.PropertyType,
                    record.ByteLength,
                    BuildPreview(record.DecodedValue),
                    record.DecodedValue)
            };
        }

        using (document)
        {
            JsonElement root = document.RootElement;
            var rows = new List<PropertyTreeRow>();
            if (root.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in root.EnumerateObject())
                {
                    rows.Add(BuildDatabankJsonNode(
                        property.Name,
                        property.Value,
                        record));
                }
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement item in root.EnumerateArray())
                {
                    index++;
                    rows.Add(BuildDatabankJsonNode(
                        $"item_{index:000}",
                        item,
                        record));
                }
            }
            else
            {
                rows.Add(BuildDatabankJsonNode("value", root, record));
            }

            if (rows.Count > 0)
            {
                return rows;
            }
        }

        return new[]
        {
            new PropertyTreeRow(
                "databank",
                "Part",
                record.ElementId,
                record.ElementDisplayName,
                record.Name,
                record.PropertyType,
                record.ByteLength,
                BuildPreview(record.DecodedValue),
                record.DecodedValue)
        };
    }

    private static PropertyTreeRow BuildDatabankJsonNode(
        string label,
        JsonElement element,
        ElementPropertyRecord sourceRecord)
    {
        string fullContent;
        string nodeKind = element.ValueKind switch
        {
            JsonValueKind.Object => "Json Object",
            JsonValueKind.Array => "Json Array",
            JsonValueKind.String => "Json String",
            JsonValueKind.Number => "Json Number",
            JsonValueKind.True => "Json Bool",
            JsonValueKind.False => "Json Bool",
            JsonValueKind.Null => "Json Null",
            _ => "Json Value"
        };

        string preview;
        bool hasEmbeddedJsonChildren = false;
        JsonElement embeddedJsonRoot = default;

        if (element.ValueKind == JsonValueKind.String)
        {
            string textValue = element.GetString() ?? string.Empty;
            fullContent = textValue;
            preview = BuildPreview(textValue);

            if (TryParseDatabankJson(textValue, out JsonDocument? embeddedDocument) &&
                embeddedDocument is not null)
            {
                using (embeddedDocument)
                {
                    embeddedJsonRoot = embeddedDocument.RootElement.Clone();
                }

                hasEmbeddedJsonChildren = true;
                nodeKind = "Json String (embedded JSON)";
                fullContent = SerializeJsonElement(embeddedJsonRoot);
                preview = embeddedJsonRoot.ValueKind switch
                {
                    JsonValueKind.Object => $"{embeddedJsonRoot.EnumerateObject().Count()} keys",
                    JsonValueKind.Array => $"{embeddedJsonRoot.GetArrayLength()} items",
                    _ => BuildPreview(embeddedJsonRoot.ToString())
                };
            }
        }
        else
        {
            fullContent = SerializeJsonElement(element);
            preview = element.ValueKind switch
            {
                JsonValueKind.Object => $"{element.EnumerateObject().Count()} keys",
                JsonValueKind.Array => $"{element.GetArrayLength()} items",
                _ => BuildPreview(element.ToString())
            };
        }

        var node = new PropertyTreeRow(
            label,
            nodeKind,
            sourceRecord.ElementId,
            sourceRecord.ElementDisplayName,
            sourceRecord.Name,
            sourceRecord.PropertyType,
            sourceRecord.ByteLength,
            preview,
            fullContent);

        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty childProperty in element.EnumerateObject())
            {
                node.Children.Add(BuildDatabankJsonNode(childProperty.Name, childProperty.Value, sourceRecord));
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            int index = 0;
            foreach (JsonElement childItem in element.EnumerateArray())
            {
                index++;
                node.Children.Add(BuildDatabankJsonNode($"item_{index:000}", childItem, sourceRecord));
            }
        }
        else if (hasEmbeddedJsonChildren)
        {
            if (embeddedJsonRoot.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty childProperty in embeddedJsonRoot.EnumerateObject())
                {
                    node.Children.Add(BuildDatabankJsonNode(childProperty.Name, childProperty.Value, sourceRecord));
                }
            }
            else if (embeddedJsonRoot.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement childItem in embeddedJsonRoot.EnumerateArray())
                {
                    index++;
                    node.Children.Add(BuildDatabankJsonNode($"item_{index:000}", childItem, sourceRecord));
                }
            }
        }

        return node;
    }

    private static string SerializeJsonElement(JsonElement element)
    {
        return JsonSerializer.Serialize(element, new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    private static bool TryParseDatabankJson(string input, out JsonDocument? document)
    {
        document = null;
        if (string.IsNullOrWhiteSpace(input))
        {
            return false;
        }

        if (TryParseJsonDocument(input.Trim(), out document))
        {
            return true;
        }

        string trimmed = input.Trim();
        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[^1] == '"' &&
            TryDeserializeJsonString(trimmed, out string unescaped) &&
            TryParseJsonDocument(unescaped.Trim(), out document))
        {
            return true;
        }

        if (TryNormalizeJsObjectLiteral(trimmed, out string normalizedLiteral) &&
            TryParseJsonDocument(normalizedLiteral, out document))
        {
            return true;
        }

        if (TryExtractJsonSubstring(trimmed, out string extracted))
        {
            if (TryParseJsonDocument(extracted, out document))
            {
                return true;
            }

            if (TryNormalizeJsObjectLiteral(extracted, out string normalizedExtracted) &&
                TryParseJsonDocument(normalizedExtracted, out document))
            {
                return true;
            }
        }

        if (TryWrapAsJsonObject(trimmed, out string wrappedObject))
        {
            if (TryParseJsonDocument(wrappedObject, out document))
            {
                return true;
            }

            if (TryNormalizeJsObjectLiteral(wrappedObject, out string normalizedWrapped) &&
                TryParseJsonDocument(normalizedWrapped, out document))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryParseJsonDocument(string candidate, out JsonDocument? document)
    {
        document = null;
        try
        {
            document = JsonDocument.Parse(candidate);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryDeserializeJsonString(string candidate, out string text)
    {
        text = string.Empty;
        try
        {
            text = JsonSerializer.Deserialize<string>(candidate) ?? string.Empty;
            return !string.IsNullOrWhiteSpace(text);
        }
        catch
        {
            return false;
        }
    }

    private static bool TryExtractJsonSubstring(string input, out string extracted)
    {
        extracted = string.Empty;
        int objectStart = input.IndexOf('{');
        int objectEnd = input.LastIndexOf('}');
        int arrayStart = input.IndexOf('[');
        int arrayEnd = input.LastIndexOf(']');

        string objectCandidate = objectStart >= 0 && objectEnd > objectStart
            ? input[objectStart..(objectEnd + 1)]
            : string.Empty;
        string arrayCandidate = arrayStart >= 0 && arrayEnd > arrayStart
            ? input[arrayStart..(arrayEnd + 1)]
            : string.Empty;

        string candidate = objectCandidate.Length >= arrayCandidate.Length
            ? objectCandidate
            : arrayCandidate;

        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        extracted = candidate;
        return true;
    }

    private static bool TryWrapAsJsonObject(string input, out string wrapped)
    {
        wrapped = string.Empty;
        string candidate = input.Trim().Trim(',');
        if (candidate.Length == 0 || candidate.Contains('{') || candidate.Contains('['))
        {
            return false;
        }

        if (!candidate.Contains(':'))
        {
            return false;
        }

        wrapped = "{" + candidate + "}";
        return true;
    }

    private static bool TryNormalizeJsObjectLiteral(string input, out string normalizedJson)
    {
        normalizedJson = string.Empty;
        if (string.IsNullOrWhiteSpace(input))
        {
            return false;
        }

        string candidate = input.Trim();
        if (!candidate.Contains(':'))
        {
            return false;
        }

        candidate = Regex.Replace(candidate, ",\\s*(?=[}\\]])", string.Empty);
        candidate = Regex.Replace(
            candidate,
            "(?<prefix>[{,]\\s*)(?<key>[A-Za-z_][A-Za-z0-9_]*)(?<suffix>\\s*:)",
            "${prefix}\"${key}\"${suffix}");
        candidate = Regex.Replace(
            candidate,
            "'((?:\\\\.|[^'\\\\])*)'",
            match =>
            {
                string value = match.Groups[1].Value;
                value = value.Replace("\\'", "'");
                value = value.Replace("\"", "\\\"");
                return "\"" + value + "\"";
            });

        normalizedJson = candidate;
        return true;
    }

    private static IReadOnlyList<(string Title, string Content)> SplitLuaSections(string text)
    {
        string normalized = text.Replace("\r\n", "\n");
        string[] lines = normalized.Split('\n');
        var sections = new List<(string Title, string Content)>();
        int currentStart = -1;
        string currentTitle = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i];
            if (!line.StartsWith("-- ===== ", StringComparison.Ordinal))
            {
                continue;
            }

            if (currentStart >= 0)
            {
                sections.Add((currentTitle, JoinLines(lines, currentStart, i - 1)));
            }

            currentTitle = ExtractLuaSectionTitle(line);
            currentStart = i + 1;
        }

        if (currentStart >= 0)
        {
            sections.Add((currentTitle, JoinLines(lines, currentStart, lines.Length - 1)));
        }

        return sections;
    }

    private static string ExtractLuaSectionTitle(string markerLine)
    {
        const string prefix = "-- ===== ";
        const string suffix = " =====";
        string inner = markerLine;
        if (inner.StartsWith(prefix, StringComparison.Ordinal))
        {
            inner = inner[prefix.Length..];
        }

        if (inner.EndsWith(suffix, StringComparison.Ordinal))
        {
            inner = inner[..^suffix.Length];
        }

        return inner.Trim();
    }

    private static string JoinLines(string[] lines, int start, int end)
    {
        if (start > end || start < 0 || end >= lines.Length)
        {
            return string.Empty;
        }

        return string.Join(Environment.NewLine, lines[start..(end + 1)]).Trim();
    }

    private static bool TryBuildJsonParts(ElementPropertyRecord record, out List<PropertyTreeRow> parts)
    {
        parts = new List<PropertyTreeRow>();
        string trimmed = record.DecodedValue.Trim();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '['))
        {
            return false;
        }

        try
        {
            using JsonDocument document = JsonDocument.Parse(trimmed);
            JsonElement root = document.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in root.EnumerateObject())
                {
                    string serialized = JsonSerializer.Serialize(property.Value, new JsonSerializerOptions { WriteIndented = true });
                    parts.Add(new PropertyTreeRow(
                        property.Name,
                        "Part",
                        record.ElementId,
                        record.ElementDisplayName,
                        record.Name,
                        record.PropertyType,
                        record.ByteLength,
                        BuildPreview(serialized),
                        serialized));
                }
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (JsonElement item in root.EnumerateArray())
                {
                    index++;
                    string serialized = JsonSerializer.Serialize(item, new JsonSerializerOptions { WriteIndented = true });
                    parts.Add(new PropertyTreeRow(
                        $"item_{index:000}",
                        "Part",
                        record.ElementId,
                        record.ElementDisplayName,
                        record.Name,
                        record.PropertyType,
                        record.ByteLength,
                        BuildPreview(serialized),
                        serialized));
                }
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildPreview(string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return string.Empty;
        }

        string normalized = content.Replace("\r\n", "\n").Replace('\r', '\n');
        int firstBreak = normalized.IndexOf('\n');
        string firstLine = firstBreak >= 0 ? normalized[..firstBreak] : normalized;
        string collapsed = Regex.Replace(firstLine.Trim(), "\\s+", " ");
        return collapsed.Length > 140 ? $"{collapsed[..140]}..." : collapsed;
    }

    private static PropertyTreeRow? ResolveSelectedTreeRow(object? selected)
    {
        if (selected is HierarchicalNode<PropertyTreeRow> typedNode)
        {
            return typedNode.Item;
        }

        if (selected is HierarchicalNode node && node.Item is PropertyTreeRow row)
        {
            return row;
        }

        return selected as PropertyTreeRow;
    }

    public bool TryGetSelectedLuaBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedDpuyaml6Node, ".lua", IsLuaSaveNode, out request);
    }

    public bool TryGetSelectedLuaEditorSourceContext(out LuaEditorSourceContext? context)
    {
        context = null;
        object? selectedNode = SelectedDpuyaml6Node;
        PropertyTreeRow? row = ResolveSelectedTreeRow(selectedNode);
        if (!IsLuaSaveNode(row))
        {
            return false;
        }

        string nodeLabelForContext = ResolveLuaNodeLabelForContext(selectedNode, row!);
        context = new LuaEditorSourceContext(
            row!.ElementId,
            row.ElementDisplayName ?? string.Empty,
            nodeLabelForContext,
            row.PropertyName ?? string.Empty,
            BuildSuggestedFileName(row, ".lua"));
        return true;
    }

    private static string ResolveLuaNodeLabelForContext(object? selectedNode, PropertyTreeRow row)
    {
        string nodeLabel = row.NodeLabel ?? string.Empty;
        if (!row.NodeKind.StartsWith(LuaPartNodeKindPrefix, StringComparison.Ordinal))
        {
            return nodeLabel;
        }

        string? componentLabel = selectedNode switch
        {
            HierarchicalNode<PropertyTreeRow> typedNode =>
                typedNode.Parent?.Item is PropertyTreeRow parentTypedRow &&
                string.Equals(parentTypedRow.NodeKind, "Component", StringComparison.Ordinal)
                    ? parentTypedRow.NodeLabel
                    : null,
            HierarchicalNode untypedNode =>
                untypedNode.Parent?.Item is PropertyTreeRow parentRow &&
                string.Equals(parentRow.NodeKind, "Component", StringComparison.Ordinal)
                    ? parentRow.NodeLabel
                    : null,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(componentLabel))
        {
            return nodeLabel;
        }

        return $"{componentLabel} / {nodeLabel}";
    }

    public bool TryGetSelectedHtmlRsBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedContent2Node, ".lua", IsMainBlobNode, out request);
    }

    public bool TryGetSelectedDatabankBlobSaveRequest(out BlobSaveRequest? request)
    {
        return TryBuildSelectedBlobSaveRequest(SelectedDatabankNode, ".json", IsMainBlobNode, out request);
    }

    private static bool TryBuildSelectedBlobSaveRequest(
        object? selectedNode,
        string extension,
        Func<PropertyTreeRow?, bool> saveRule,
        out BlobSaveRequest? request)
    {
        request = null;
        PropertyTreeRow? row = ResolveSelectedTreeRow(selectedNode);
        if (!saveRule(row))
        {
            return false;
        }

        string suggestedName = BuildSuggestedFileName(row!, extension);
        string content = row!.FullContent ?? string.Empty;
        request = new BlobSaveRequest(suggestedName, content, extension);
        return true;
    }

    private static bool IsMainBlobNode(PropertyTreeRow? row)
    {
        return row is not null && string.Equals(row.NodeKind, "Block", StringComparison.Ordinal);
    }

    private static bool IsLuaSaveNode(PropertyTreeRow? row)
    {
        if (row is null)
        {
            return false;
        }

        return string.Equals(row.NodeKind, "Block", StringComparison.Ordinal) ||
               string.Equals(row.NodeKind, "Part", StringComparison.Ordinal) ||
               row.NodeKind.StartsWith(LuaPartNodeKindPrefix, StringComparison.Ordinal);
    }

    private static string BuildSuggestedFileName(PropertyTreeRow row, string extension)
    {
        string elementName = !string.IsNullOrWhiteSpace(row.ElementDisplayName)
            ? row.ElementDisplayName
            : row.ElementTypeName;
        if (string.IsNullOrWhiteSpace(elementName))
        {
            elementName = "element";
        }

        string idPart = row.ElementId.HasValue
            ? row.ElementId.Value.ToString(CultureInfo.InvariantCulture)
            : "unknown";
        string propertyPart = string.IsNullOrWhiteSpace(row.PropertyName) ? "blob" : row.PropertyName;
        string baseName = $"{elementName}_{idPart}_{propertyPart}";
        if (!string.Equals(row.NodeKind, "Block", StringComparison.Ordinal) && !string.IsNullOrWhiteSpace(row.NodeLabel))
        {
            baseName += "_" + row.NodeLabel;
        }

        string sanitized = SanitizeFileName(baseName);
        return sanitized.EndsWith(extension, StringComparison.OrdinalIgnoreCase)
            ? sanitized
            : sanitized + extension;
    }

    private static string SanitizeFileName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "blob";
        }

        string invalid = Regex.Escape(new string(System.IO.Path.GetInvalidFileNameChars()));
        string sanitized = Regex.Replace(value, $"[{invalid}]+", "_");
        sanitized = Regex.Replace(sanitized, "\\s+", "_");
        sanitized = sanitized.Trim('_');
        return string.IsNullOrWhiteSpace(sanitized) ? "blob" : sanitized;
    }

    private static object? FindNodeBySelectionKey(HierarchicalModel<PropertyTreeRow> model, string selectionKey)
    {
        if (string.IsNullOrWhiteSpace(selectionKey))
        {
            return null;
        }

        foreach (var node in model.Flattened)
        {
            if (node.Item is PropertyTreeRow row &&
                string.Equals(BuildSelectionKey(row), selectionKey, StringComparison.Ordinal))
            {
                return node;
            }
        }

        return null;
    }

    private static string BuildSelectionKey(PropertyTreeRow? row)
    {
        if (row is null || string.Equals(row.NodeKind, "Root", StringComparison.Ordinal))
        {
            return string.Empty;
        }

        string elementPart = row.ElementId.HasValue
            ? row.ElementId.Value.ToString(CultureInfo.InvariantCulture)
            : string.Empty;

        return string.Join(
            "|",
            row.NodeKind ?? string.Empty,
            elementPart,
            row.PropertyName ?? string.Empty,
            row.NodeLabel ?? string.Empty);
    }

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
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
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

        if (!_isRestoringSettings && !_isStartupInitializing)
        {
            RefreshConstructSuggestionsForCurrentPlayer();
        }
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
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
    }

    partial void OnRepairInProgressChanged(bool value)
    {
        OnPropertyChanged(nameof(CanRepairDestroyedElements));
    }
}
