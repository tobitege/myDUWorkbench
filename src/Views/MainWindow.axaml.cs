// Helper Index:
// - SaveBlobAsync: Saves selected LUA/HTML/databank content through Avalonia storage APIs.
// - CaptureWindowPlacement: Captures current size, position, screen, and maximized state.
// - ApplyWindowPlacement: Restores saved window geometry while keeping it on a visible screen.
// - ApplyColumnWidths: Restores persisted DataGrid column widths for all tracked grids.
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
using myDUWorkbench.Controls;
using myDUWorkbench.Helpers;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using myDUWorkbench.ViewModels;
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

namespace myDUWorkbench.Views;

public partial class MainWindow : Window
{
    private const string LuaGrammarResourceSuffix = "Grammars.lua.tmLanguage.json";
    private const string LuaGrammarCacheFileName = "lua.tmLanguage.json";
    private const int LuaFoldRefreshDebounceMs = 250;
    private const int LuaEditorInfoStatusClearDelaySeconds = 10;
    private static readonly string[] LuaCoreComponentOrder = new[] {"library", "system", "player", "construct", "unit"};
    private static readonly bool LuaHoverTooltipsEnabled = false;
    private static readonly Regex LuaSectionHeaderRegex = new(
        "^\\s*-- ===== (?<index>\\d{3}) (?<title>.+?) =====\\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private BreakpointMargin? _luaBreakpointMargin;
    private ExecutionLineHighlighter? _luaExecutionHighlighter;
    private TextMate.Installation? _luaTextMateInstallation;
    private FoldingManager? _luaFoldingManager;
    private DispatcherTimer? _luaFoldRefreshTimer;
    private ToolTip? _luaHoverToolTip;
    private readonly HashSet<int> _luaBreakpoints = new();
    private readonly MyDuDataService _dataService = new();
    private readonly LuaBackupService _luaBackupService = new();
    private string _luaEditorCurrentFilePath = string.Empty;
    private string _luaEditorSuggestedFileName = "script.lua";
    private MainWindowViewModel.LuaEditorSourceContext? _luaEditorSourceContext;
    private LuaPropertyRawRecord? _luaEditorOriginalDbRecord;
    private int _luaExecutionLine = -1;
    private bool _luaExecutionLineIsCurrentFile = true;
    private bool _luaEditorInitialized;
    private bool _luaPendingDoubleClickSelection;
    private readonly ObservableCollection<LuaEditorSectionState> _luaEditorSections = new();
    private readonly HierarchicalModel<LuaEditorSectionTreeRow> _luaEditorSectionModel = CreateLuaEditorSectionModel();
    private readonly Dictionary<LuaEditorSectionState, LuaEditorSectionTreeRow> _luaEditorSectionNodeBySection = new();
    private IReadOnlyList<string> _luaEditorStructuredTitleBaseline = Array.Empty<string>();
    private bool _luaStructuredModeActive;
    private bool _luaSuppressSectionSelectionEvent;
    private bool _luaSuppressSectionTextSync;
    private int _luaSelectedStructuredSectionIndex = -1;
    private string _luaEditorLastPersistedContent = string.Empty;
    private bool _allowCloseAfterDiscardConfirmation;
    private string _luaEditorEncodingLabel = "UTF-8";
    private CancellationTokenSource? _luaInfoStatusClearCts;
    private string _luaInfoStatusMessage = string.Empty;
    private readonly DataGrid[] _hierarchicalTreeGrids;

    public MainWindow()
    {
        InitializeComponent();
        _hierarchicalTreeGrids = new[]
        {
            ElementPropertiesGrid,
            LuaBlocksGrid,
            HtmlRsGrid,
            DatabankGrid,
            LuaEditorSectionGrid
        };
        AttachHierarchicalGridLeftNavigation();
        ElementPropertiesGrid.AddHandler(
            InputElement.PointerPressedEvent,
            OnElementPropertiesGridPointerPressed,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        Loaded += OnLoaded;
        Opened += OnOpened;
        Closing += OnClosing;
    }
}
