// Helper Index:
// - LoadDatabaseAsync: Loads DB construct snapshot, categorizes properties, and refreshes tree models.
// - ImportBlueprintJsonAsync: Imports blueprint JSON and projects it into grid/property models.
// - ProbeEndpointAsync: Probes endpoint payloads and updates decoded transport summaries.
// - QueueConstructNameSearch: Debounces construct-name search and schedules async suggestion refresh.
// - TryGetSelectedLuaBlobSaveRequest: Builds save metadata for currently selected LUA blob node.
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
    private const string LuaPartNodeKindPrefix = "Part:";
    private const int AutoConnectRetryMinSeconds = 10;
    private const int AutoConnectRetryMaxSeconds = 9999;
    private const int AutoConnectRetryDefaultSeconds = 30;
    private const int BlueprintNameMaxLength = 30;

    public sealed record BlobSaveRequest(string SuggestedFileName, string Content, string DefaultExtension);
    public sealed record LuaEditorSourceContext(
        ulong? ElementId,
        string ElementDisplayName,
        string NodeLabel,
        string PropertyName,
        string SuggestedFileName);
    private sealed record ElementFilterSnapshot(
        string ElementTypeFilterInput,
        bool DamagedOnly,
        Dictionary<string, bool> PropertyStates);
    private readonly record struct TreeBuildProgress(int ProcessedElements, int TotalElements);

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
    private bool _isUpdatingBlueprintTypeSelection;
    private ulong? _lastBlueprintCreatorFilterPlayerId;
    private readonly Dictionary<ulong, PropertyTreeRow> _luaBlockNodeByElementId = new();
    private readonly Dictionary<ulong, PropertyTreeRow> _htmlRsBlockNodeByElementId = new();
    private readonly Dictionary<ulong, PropertyTreeRow> _databankBlockNodeByElementId = new();
    private int _constructBrowserLevel1EntryCount;
    private long _constructBrowserLevel2EntryCount;
    private int _blueprintsLevel1EntryCount;

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
    public ObservableCollection<BlueprintDbRecord> Blueprints { get; } = new();

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
    private string nqUtilsDllPathInput = string.Empty;

    [ObservableProperty]
    private string blueprintImportEndpointInput = string.Empty;

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
    private bool blueprintImportDryRunMode;

    [ObservableProperty]
    private bool blueprintImportIntoApp = true;

    [ObservableProperty]
    private bool blueprintImportIntoGameDatabase;

    [ObservableProperty]
    private bool blueprintImportAppendDateIfExists;

    [ObservableProperty]
    private string blueprintNameFilter = string.Empty;

    [ObservableProperty]
    private BlueprintDbRecord? selectedBlueprint;

    [ObservableProperty]
    private string blueprintsStatus = string.Empty;

    [ObservableProperty]
    private string blueprintEditName = string.Empty;

    [ObservableProperty]
    private bool blueprintEditFreeDeploy;

    [ObservableProperty]
    private bool blueprintEditApplyMaxUse;

    [ObservableProperty]
    private bool blueprintEditCoreBlueprint = true;

    [ObservableProperty]
    private bool blueprintEditSingleUseBlueprint;

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
    private string lastBlueprintImportErrorDetails = string.Empty;

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

    [ObservableProperty]
    private bool blueprintImportInProgress;

    [ObservableProperty]
    private double blueprintImportProgressPercent;

    [ObservableProperty]
    private string blueprintImportProgressText = "Import: idle";

    [ObservableProperty]
    private bool exportInProgress;

    [ObservableProperty]
    private double exportProgressPercent;

    [ObservableProperty]
    private string exportProgressText = "Export: idle";

    [ObservableProperty]
    private int constructDataTabIndex;

    [ObservableProperty]
    private string gridEntryCountStatus = string.Empty;

    public TextWrapping ContentTextWrapping => AutoWrapContent ? TextWrapping.Wrap : TextWrapping.NoWrap;
    public bool CanSaveSelectedLuaBlob => IsLuaSaveNode(ResolveSelectedTreeRow(SelectedDpuyaml6Node));
    public bool CanSaveSelectedHtmlRsBlob => IsMainBlobNode(ResolveSelectedTreeRow(SelectedContent2Node));
    public bool CanSaveSelectedDatabankBlob => IsMainBlobNode(ResolveSelectedTreeRow(SelectedDatabankNode));
    public bool CanRepairDestroyedElements => !IsBusy && !RepairInProgress && _lastSnapshot is not null && IsDatabaseOnline();
    public bool CanUseDamagedFilter => _lastSnapshot is not null;
    public bool CanEditBlueprint => SelectedBlueprint is not null && !IsBusy && IsDatabaseOnline();
    public bool CanEditBlueprintMaxUse => CanEditBlueprint && BlueprintEditApplyMaxUse;
    public bool CanCopyBlueprint => CanEditBlueprint;
    public bool CanDeleteBlueprint => CanEditBlueprint;
    public bool CanImportBlueprint => !IsBusy && SelectedPlayerNameSuggestion?.PlayerId is ulong playerId && playerId > 0UL;
    public bool CanSaveBlueprint => CanEditBlueprint && IsBlueprintEditInputValid(out _, out _);
    public bool CanExportConstructBrowserElementSummary => !IsBusy && HasLoadedConstructBrowserElementData();
    public bool CanExportBlueprintElementSummary => !IsBusy && IsDatabaseOnline() && Blueprints.Count > 0;
    public string BlueprintEditValidationMessage => BuildBlueprintEditValidationMessage();
    public bool HasBlueprintEditValidationError => !string.IsNullOrWhiteSpace(BlueprintEditValidationMessage);
    public string BlueprintCurrentMaxUseStateDisplay => BuildBlueprintCurrentMaxUseStateDisplay();
    public string BlueprintsPlayerFilterDisplay => BuildBlueprintsPlayerFilterDisplay();
    public string DatabaseAvailabilityDisplay => BuildDatabaseAvailabilityDisplay();
    public string SelectedPlayerIdDisplay => SelectedPlayerNameSuggestion?.PlayerId?.ToString(CultureInfo.InvariantCulture) ?? "-";
    public bool HasGridEntryCountStatus => !string.IsNullOrWhiteSpace(GridEntryCountStatus);

    public MainWindowViewModel()
    {
        _isStartupInitializing = true;
        ElementPropertiesModel = CreateTreeModel();
        Dpuyaml6Model = CreateTreeModel();
        Content2Model = CreateTreeModel();
        DatabankModel = CreateTreeModel();

        PropertyTreeRow[] emptyRoots = Array.Empty<PropertyTreeRow>();
        ElementPropertiesModel.SetRoots(emptyRoots);
        UpdateConstructBrowserEntryCounts(emptyRoots);
        Dpuyaml6Model.SetRoot(CreateRootNode("LUA blocks"));
        Content2Model.SetRoot(CreateRootNode("HTML/RS"));
        DatabankModel.SetRoot(CreateRootNode("Databank"));
        Blueprints.CollectionChanged += (_, _) =>
        {
            OnPropertyChanged(nameof(CanExportBlueprintElementSummary));
            RefreshBlueprintEntryCounts();
        };
        RefreshBlueprintEntryCounts();

        RestoreSettingsFromDisk();
        _ = InitializeStartupAsync();
    }

    partial void OnConstructDataTabIndexChanged(int value)
    {
        RefreshGridEntryCountStatus();
    }

    partial void OnGridEntryCountStatusChanged(string value)
    {
        OnPropertyChanged(nameof(HasGridEntryCountStatus));
    }

    private void UpdateConstructBrowserEntryCounts(IReadOnlyList<PropertyTreeRow> roots)
    {
        _constructBrowserLevel1EntryCount = roots.Count;
        _constructBrowserLevel2EntryCount = roots.Sum(root => (long)root.Children.Count);
        RefreshGridEntryCountStatus();
    }

    private void RefreshBlueprintEntryCounts()
    {
        _blueprintsLevel1EntryCount = Blueprints.Count;
        RefreshGridEntryCountStatus();
    }

    private void RefreshGridEntryCountStatus()
    {
        GridEntryCountStatus = ConstructDataTabIndex switch
        {
            0 => FormatGridEntryCountStatus(
                "Construct Browser",
                _constructBrowserLevel1EntryCount,
                _constructBrowserLevel2EntryCount),
            1 => FormatGridEntryCountStatus(
                "Blueprints",
                _blueprintsLevel1EntryCount),
            _ => string.Empty
        };
    }

    private static string FormatGridEntryCountStatus(string scope, int level1Count, long level2Count)
    {
        return $"{scope} L1={level1Count.ToString("N0", CultureInfo.InvariantCulture)}, L2={level2Count.ToString("N0", CultureInfo.InvariantCulture)}";
    }

    private static string FormatGridEntryCountStatus(string scope, int level1Count)
    {
        return $"{scope} L1={level1Count.ToString("N0", CultureInfo.InvariantCulture)}";
    }

}
