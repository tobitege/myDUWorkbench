using Avalonia;
using Avalonia.Controls;
using Avalonia.Interactivity;
using System;

namespace myDUWorkbench.Controls;

public partial class DetailActionToolbar : UserControl
{
    public static readonly StyledProperty<bool> ShowSaveButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowSaveButton), true);

    public static readonly StyledProperty<bool> IsSaveEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsSaveEnabled), true);

    public static readonly StyledProperty<bool> ShowRefreshButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowRefreshButton), true);

    public static readonly StyledProperty<bool> IsRefreshEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsRefreshEnabled), true);

    public static readonly StyledProperty<bool> ShowBackupsButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowBackupsButton));

    public static readonly StyledProperty<bool> IsBackupsEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsBackupsEnabled), true);

    public static readonly StyledProperty<string> BackupsButtonTextProperty =
        AvaloniaProperty.Register<DetailActionToolbar, string>(nameof(BackupsButtonText), "Backups...");

    public static readonly StyledProperty<bool> ShowPrettyPrintButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowPrettyPrintButton), true);

    public static readonly StyledProperty<bool> IsPrettyPrintEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsPrettyPrintEnabled), true);

    public static readonly StyledProperty<bool> PrettyPrintCheckedProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(PrettyPrintChecked));

    public static readonly StyledProperty<bool> ShowClearButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowClearButton), true);

    public static readonly StyledProperty<bool> IsClearEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsClearEnabled), true);

    public static readonly StyledProperty<bool> ShowEditButtonProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowEditButton));

    public static readonly StyledProperty<bool> IsEditEnabledProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(IsEditEnabled), true);

    public static readonly StyledProperty<bool> ShowWrapToggleProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(ShowWrapToggle));

    public static readonly StyledProperty<bool> WrapCheckedProperty =
        AvaloniaProperty.Register<DetailActionToolbar, bool>(nameof(WrapChecked));

    public DetailActionToolbar()
    {
        InitializeComponent();
    }

    public bool ShowSaveButton
    {
        get => GetValue(ShowSaveButtonProperty);
        set => SetValue(ShowSaveButtonProperty, value);
    }

    public bool IsSaveEnabled
    {
        get => GetValue(IsSaveEnabledProperty);
        set => SetValue(IsSaveEnabledProperty, value);
    }

    public bool ShowRefreshButton
    {
        get => GetValue(ShowRefreshButtonProperty);
        set => SetValue(ShowRefreshButtonProperty, value);
    }

    public bool IsRefreshEnabled
    {
        get => GetValue(IsRefreshEnabledProperty);
        set => SetValue(IsRefreshEnabledProperty, value);
    }

    public bool ShowBackupsButton
    {
        get => GetValue(ShowBackupsButtonProperty);
        set => SetValue(ShowBackupsButtonProperty, value);
    }

    public bool IsBackupsEnabled
    {
        get => GetValue(IsBackupsEnabledProperty);
        set => SetValue(IsBackupsEnabledProperty, value);
    }

    public string BackupsButtonText
    {
        get => GetValue(BackupsButtonTextProperty);
        set => SetValue(BackupsButtonTextProperty, value);
    }

    public bool ShowPrettyPrintButton
    {
        get => GetValue(ShowPrettyPrintButtonProperty);
        set => SetValue(ShowPrettyPrintButtonProperty, value);
    }

    public bool IsPrettyPrintEnabled
    {
        get => GetValue(IsPrettyPrintEnabledProperty);
        set => SetValue(IsPrettyPrintEnabledProperty, value);
    }

    public bool PrettyPrintChecked
    {
        get => GetValue(PrettyPrintCheckedProperty);
        set => SetValue(PrettyPrintCheckedProperty, value);
    }

    public bool ShowClearButton
    {
        get => GetValue(ShowClearButtonProperty);
        set => SetValue(ShowClearButtonProperty, value);
    }

    public bool IsClearEnabled
    {
        get => GetValue(IsClearEnabledProperty);
        set => SetValue(IsClearEnabledProperty, value);
    }

    public bool ShowEditButton
    {
        get => GetValue(ShowEditButtonProperty);
        set => SetValue(ShowEditButtonProperty, value);
    }

    public bool IsEditEnabled
    {
        get => GetValue(IsEditEnabledProperty);
        set => SetValue(IsEditEnabledProperty, value);
    }

    public bool ShowWrapToggle
    {
        get => GetValue(ShowWrapToggleProperty);
        set => SetValue(ShowWrapToggleProperty, value);
    }

    public bool WrapChecked
    {
        get => GetValue(WrapCheckedProperty);
        set => SetValue(WrapCheckedProperty, value);
    }

    public event EventHandler<RoutedEventArgs>? SaveClick;
    public event EventHandler<RoutedEventArgs>? RefreshClick;
    public event EventHandler<RoutedEventArgs>? BackupsClick;
    public event EventHandler<RoutedEventArgs>? ClearClick;
    public event EventHandler<RoutedEventArgs>? EditClick;

    private void OnSaveClick(object? sender, RoutedEventArgs e)
    {
        SaveClick?.Invoke(sender ?? this, e);
    }

    private void OnRefreshClick(object? sender, RoutedEventArgs e)
    {
        RefreshClick?.Invoke(sender ?? this, e);
    }

    private void OnBackupsClick(object? sender, RoutedEventArgs e)
    {
        BackupsClick?.Invoke(sender ?? this, e);
    }

    private void OnClearClick(object? sender, RoutedEventArgs e)
    {
        ClearClick?.Invoke(sender ?? this, e);
    }

    private void OnEditClick(object? sender, RoutedEventArgs e)
    {
        EditClick?.Invoke(sender ?? this, e);
    }
}
