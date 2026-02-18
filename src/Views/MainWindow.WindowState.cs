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
using myDUWorker.Controls;
using myDUWorker.Helpers;
using myDUWorker.Models;
using myDUWorker.Services;
using myDUWorker.ViewModels;
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

namespace myDUWorker.Views;

public partial class MainWindow : Window
{
    private void OnOpened(object? sender, EventArgs e)
    {
        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        ApplyWindowPlacement(vm.GetSavedWindowPlacement());
        ApplyColumnWidths(vm.GetSavedGridColumnWidths());
    }

    private void OnLoaded(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        EnsureLuaEditorInitialized();
        _luaEditorEncodingLabel = "UTF-8";
        UpdateLuaEditorHeader();
        MarkLuaEditorCleanFromCurrentContent();
    }

    private static string FormatEncodingLabel(Encoding encoding)
    {
        if (encoding is null)
        {
            return "UTF-8";
        }

        if (string.Equals(encoding.WebName, "utf-8", StringComparison.OrdinalIgnoreCase))
        {
            return encoding.GetPreamble().Length > 0 ? "UTF-8 BOM" : "UTF-8";
        }

        string label = encoding.WebName.Replace("-", "_", StringComparison.Ordinal).ToUpperInvariant();
        return label.Length == 0 ? "UTF-8" : label;
    }

    private async void OnClosing(object? sender, WindowClosingEventArgs e)
    {
        if (!_allowCloseAfterDiscardConfirmation &&
            LuaEditorPageRoot.IsVisible &&
            HasLuaEditorUnsavedChanges())
        {
            e.Cancel = true;
            bool discard = await ConfirmDiscardLuaEditorChangesAsync("close the application");
            if (discard)
            {
                _allowCloseAfterDiscardConfirmation = true;
                Close();
            }

            return;
        }

        DetachHierarchicalGridLeftNavigation();
        CleanupLuaEditor();

        if (DataContext is not MainWindowViewModel vm)
        {
            return;
        }

        WindowPlacementSettings placement = CaptureWindowPlacement();
        vm.UpdateWindowPlacement(placement);
        Dictionary<string, string> widths = CaptureColumnWidths();
        vm.UpdateGridColumnWidths(widths);
        vm.PersistSettingsNow();
    }

    private WindowPlacementSettings CaptureWindowPlacement()
    {
        var placement = new WindowPlacementSettings
        {
            StartMaximized = WindowState == WindowState.Maximized
        };

        if (WindowState == WindowState.Normal)
        {
            if (Bounds.Width > 0)
            {
                placement.Width = Bounds.Width;
            }

            if (Bounds.Height > 0)
            {
                placement.Height = Bounds.Height;
            }

            placement.PositionX = Position.X;
            placement.PositionY = Position.Y;
        }

        Screen? currentScreen = Screens?.ScreenFromWindow(this);
        if (currentScreen is not null)
        {
            placement.ScreenKey = BuildScreenKey(currentScreen);
        }

        return placement;
    }

    private void ApplyWindowPlacement(WindowPlacementSettings placement)
    {
        if (placement.Width.HasValue && placement.Width.Value >= MinWidth)
        {
            Width = placement.Width.Value;
        }

        if (placement.Height.HasValue && placement.Height.Value >= MinHeight)
        {
            Height = placement.Height.Value;
        }

        Screen? targetScreen = ResolveTargetScreen(placement.ScreenKey, placement.PositionX, placement.PositionY);
        if (placement.PositionX.HasValue && placement.PositionY.HasValue)
        {
            var requested = new PixelPoint(placement.PositionX.Value, placement.PositionY.Value);
            Position = CoerceToVisiblePosition(requested, targetScreen);
        }
        else if (targetScreen is not null)
        {
            Position = CenterInWorkingArea(targetScreen);
        }

        if (placement.StartMaximized)
        {
            WindowState = WindowState.Maximized;
        }
    }

    private Screen? ResolveTargetScreen(string screenKey, int? x, int? y)
    {
        if (Screens is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(screenKey))
        {
            Screen? byKey = Screens.All.FirstOrDefault(s => string.Equals(BuildScreenKey(s), screenKey, StringComparison.Ordinal));
            if (byKey is not null)
            {
                return byKey;
            }
        }

        if (x.HasValue && y.HasValue)
        {
            Screen? byPoint = Screens.ScreenFromPoint(new PixelPoint(x.Value, y.Value));
            if (byPoint is not null)
            {
                return byPoint;
            }
        }

        return Screens.ScreenFromWindow(this) ?? Screens.Primary;
    }

    private PixelPoint CoerceToVisiblePosition(PixelPoint requested, Screen? screen)
    {
        if (screen is null)
        {
            return requested;
        }

        PixelRect wa = screen.WorkingArea;
        int safeWidth = (int)Math.Round(Math.Max(MinWidth, Width > 0 ? Width : MinWidth));
        int safeHeight = (int)Math.Round(Math.Max(MinHeight, Height > 0 ? Height : MinHeight));

        int maxX = Math.Max(wa.X, wa.X + wa.Width - Math.Min(safeWidth, wa.Width));
        int maxY = Math.Max(wa.Y, wa.Y + wa.Height - Math.Min(safeHeight, wa.Height));

        int x = Math.Clamp(requested.X, wa.X, maxX);
        int y = Math.Clamp(requested.Y, wa.Y, maxY);
        return new PixelPoint(x, y);
    }

    private PixelPoint CenterInWorkingArea(Screen screen)
    {
        PixelRect wa = screen.WorkingArea;
        int safeWidth = (int)Math.Round(Math.Max(MinWidth, Width > 0 ? Width : MinWidth));
        int safeHeight = (int)Math.Round(Math.Max(MinHeight, Height > 0 ? Height : MinHeight));
        int x = wa.X + Math.Max(0, (wa.Width - safeWidth) / 2);
        int y = wa.Y + Math.Max(0, (wa.Height - safeHeight) / 2);
        return new PixelPoint(x, y);
    }

    private static string BuildScreenKey(Screen screen)
    {
        PixelRect b = screen.Bounds;
        return $"{b.X},{b.Y},{b.Width},{b.Height}";
    }

    private Dictionary<string, string> CaptureColumnWidths()
    {
        return DataGridColumnWidthPersistence.Capture(
            (ConfigPropertiesGrid, "config-grid"),
            (ElementPropertiesGrid, "element-grid"),
            (LuaBlocksGrid, "lua-grid"),
            (HtmlRsGrid, "content2-grid"),
            (DatabankGrid, "databank-grid"));
    }

    private void ApplyColumnWidths(IReadOnlyDictionary<string, string> persisted)
    {
        DataGridColumnWidthPersistence.Apply(
            persisted,
            (ConfigPropertiesGrid, "config-grid"),
            (ElementPropertiesGrid, "element-grid"),
            (LuaBlocksGrid, "lua-grid"),
            (HtmlRsGrid, "content2-grid"),
            (DatabankGrid, "databank-grid"));
    }
}
