// Helper Index:
// - WorkbenchSettings: Runtime settings model consumed directly by the view model.
// - PersistedWorkbenchSettings: Disk-serialization model including encrypted password field.
// - WindowPlacementSettings: Captures size/position/screen/maximized window state.
namespace myDUWorkbench.Models;

using System.Collections.Generic;

public sealed class WorkbenchSettings
{
    public string ConstructIdInput { get; set; } = string.Empty;
    public string PlayerIdInput { get; set; } = string.Empty;
    public bool HasPersistedPlayerFilterState { get; set; }
    public string ConstructNameSearchInput { get; set; } = string.Empty;
    public string PlayerNameSearchInput { get; set; } = string.Empty;
    public string EndpointTemplateInput { get; set; } = string.Empty;
    public string DbHostInput { get; set; } = string.Empty;
    public string ServerRootPathInput { get; set; } = string.Empty;
    public string NqUtilsDllPathInput { get; set; } = string.Empty;
    public string BlueprintImportEndpointInput { get; set; } = string.Empty;
    public string DbPortInput { get; set; } = string.Empty;
    public string DbNameInput { get; set; } = string.Empty;
    public string DbUserInput { get; set; } = string.Empty;
    public string DbPassword { get; set; } = string.Empty;
    public string PropertyLimitInput { get; set; } = string.Empty;
    public bool BlueprintImportAppendDateIfExists { get; set; }
    public string ElementTypeNameFilterInput { get; set; } = string.Empty;
    public List<string> ElementTypeFilterHistory { get; set; } = new();
    public bool AutoLoadOnStartup { get; set; } = true;
    public bool AutoLoadPlayerNames { get; set; } = true;
    public bool LimitToSelectedPlayerConstructs { get; set; } = true;
    public bool AutoConnectDatabase { get; set; }
    public int AutoConnectRetrySeconds { get; set; } = 30;
    public bool AutoWrapContent { get; set; }
    public bool AutoCollapseToFirstLevel { get; set; }
    public bool LuaVersioningEnabled { get; set; }
    public string LastSavedFolder { get; set; } = string.Empty;
    public ulong? SelectedConstructSuggestionId { get; set; }
    public string SelectedConstructSuggestionName { get; set; } = string.Empty;
    public ConstructSuggestionKind SelectedConstructSuggestionKind { get; set; } = ConstructSuggestionKind.Construct;
    public bool IncludeBlueprintOnlySuggestions { get; set; }
    public string SelectedElementNodeKey { get; set; } = string.Empty;
    public string SelectedDpuyamlNodeKey { get; set; } = string.Empty;
    public string SelectedContent2NodeKey { get; set; } = string.Empty;
    public string SelectedDatabankNodeKey { get; set; } = string.Empty;
    public Dictionary<string, string> GridColumnWidths { get; set; } = new();
    public Dictionary<string, bool> ElementPropertyActiveStates { get; set; } = new();
    public WindowPlacementSettings WindowPlacement { get; set; } = new();
}

public sealed class PersistedWorkbenchSettings
{
    public string ConstructIdInput { get; set; } = string.Empty;
    public string PlayerIdInput { get; set; } = string.Empty;
    public bool HasPersistedPlayerFilterState { get; set; }
    public string ConstructNameSearchInput { get; set; } = string.Empty;
    public string PlayerNameSearchInput { get; set; } = string.Empty;
    public string EndpointTemplateInput { get; set; } = string.Empty;
    public string DbHostInput { get; set; } = string.Empty;
    public string ServerRootPathInput { get; set; } = string.Empty;
    public string NqUtilsDllPathInput { get; set; } = string.Empty;
    public string BlueprintImportEndpointInput { get; set; } = string.Empty;
    public string DbPortInput { get; set; } = string.Empty;
    public string DbNameInput { get; set; } = string.Empty;
    public string DbUserInput { get; set; } = string.Empty;
    public string EncryptedDbPassword { get; set; } = string.Empty;
    public string PropertyLimitInput { get; set; } = string.Empty;
    public bool BlueprintImportAppendDateIfExists { get; set; }
    public string ElementTypeNameFilterInput { get; set; } = string.Empty;
    public List<string> ElementTypeFilterHistory { get; set; } = new();
    public bool AutoLoadOnStartup { get; set; } = true;
    public bool AutoLoadPlayerNames { get; set; } = true;
    public bool LimitToSelectedPlayerConstructs { get; set; } = true;
    public bool AutoConnectDatabase { get; set; }
    public int AutoConnectRetrySeconds { get; set; } = 30;
    public bool AutoWrapContent { get; set; }
    public bool AutoCollapseToFirstLevel { get; set; }
    public bool LuaVersioningEnabled { get; set; }
    public string LastSavedFolder { get; set; } = string.Empty;
    public ulong? SelectedConstructSuggestionId { get; set; }
    public string SelectedConstructSuggestionName { get; set; } = string.Empty;
    public ConstructSuggestionKind SelectedConstructSuggestionKind { get; set; } = ConstructSuggestionKind.Construct;
    public bool IncludeBlueprintOnlySuggestions { get; set; }
    public string SelectedElementNodeKey { get; set; } = string.Empty;
    public string SelectedDpuyamlNodeKey { get; set; } = string.Empty;
    public string SelectedContent2NodeKey { get; set; } = string.Empty;
    public string SelectedDatabankNodeKey { get; set; } = string.Empty;
    public Dictionary<string, string> GridColumnWidths { get; set; } = new();
    public Dictionary<string, bool> ElementPropertyActiveStates { get; set; } = new();
    public WindowPlacementSettings WindowPlacement { get; set; } = new();
}

public sealed class WindowPlacementSettings
{
    public double? Width { get; set; }
    public double? Height { get; set; }
    public int? PositionX { get; set; }
    public int? PositionY { get; set; }
    public string ScreenKey { get; set; } = string.Empty;
    public bool StartMaximized { get; set; }
}
