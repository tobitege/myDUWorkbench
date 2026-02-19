using Avalonia.Controls;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace myDUWorkbench.Helpers;

internal static class DataGridColumnWidthPersistence
{
    public static Dictionary<string, string> Capture(params (DataGrid Grid, string GridKey)[] grids)
    {
        var widths = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach ((DataGrid grid, string gridKey) in grids)
        {
            CaptureGridColumnWidths(grid, gridKey, widths);
        }

        return widths;
    }

    public static void Apply(
        IReadOnlyDictionary<string, string> persisted,
        params (DataGrid Grid, string GridKey)[] grids)
    {
        if (persisted.Count == 0)
        {
            return;
        }

        foreach ((DataGrid grid, string gridKey) in grids)
        {
            ApplyGridColumnWidths(grid, gridKey, persisted);
        }
    }

    private static void CaptureGridColumnWidths(DataGrid grid, string gridKey, Dictionary<string, string> widths)
    {
        for (int i = 0; i < grid.Columns.Count; i++)
        {
            DataGridColumn column = grid.Columns[i];
            string key = BuildColumnWidthKey(gridKey, i, column);
            widths[key] = SerializeColumnWidth(column.Width);
        }
    }

    private static void ApplyGridColumnWidths(DataGrid grid, string gridKey, IReadOnlyDictionary<string, string> persisted)
    {
        for (int i = 0; i < grid.Columns.Count; i++)
        {
            DataGridColumn column = grid.Columns[i];
            string key = BuildColumnWidthKey(gridKey, i, column);
            if (!persisted.TryGetValue(key, out string? serialized) || string.IsNullOrWhiteSpace(serialized))
            {
                continue;
            }

            if (TryParseColumnWidth(serialized, out DataGridLength width))
            {
                column.Width = width;
            }
        }
    }

    private static string BuildColumnWidthKey(string gridKey, int index, DataGridColumn column)
    {
        string header = column.Header?.ToString() ?? string.Empty;
        return $"{gridKey}|{index}|{header}";
    }

    private static string SerializeColumnWidth(DataGridLength width)
    {
        if (width.IsAuto)
        {
            return "Auto";
        }

        if (width.IsSizeToCells)
        {
            return "SizeToCells";
        }

        if (width.IsSizeToHeader)
        {
            return "SizeToHeader";
        }

        if (width.IsStar)
        {
            return $"Star:{width.Value.ToString("R", CultureInfo.InvariantCulture)}";
        }

        return $"Pixel:{width.Value.ToString("R", CultureInfo.InvariantCulture)}";
    }

    private static bool TryParseColumnWidth(string serialized, out DataGridLength width)
    {
        width = default;
        if (string.Equals(serialized, "Auto", StringComparison.Ordinal))
        {
            width = DataGridLength.Auto;
            return true;
        }

        if (string.Equals(serialized, "SizeToCells", StringComparison.Ordinal))
        {
            width = DataGridLength.SizeToCells;
            return true;
        }

        if (string.Equals(serialized, "SizeToHeader", StringComparison.Ordinal))
        {
            width = DataGridLength.SizeToHeader;
            return true;
        }

        if (serialized.StartsWith("Star:", StringComparison.Ordinal) &&
            double.TryParse(serialized[5..], NumberStyles.Float, CultureInfo.InvariantCulture, out double star) &&
            star > 0)
        {
            width = new DataGridLength(star, DataGridLengthUnitType.Star);
            return true;
        }

        if (serialized.StartsWith("Pixel:", StringComparison.Ordinal) &&
            double.TryParse(serialized[6..], NumberStyles.Float, CultureInfo.InvariantCulture, out double px) &&
            px > 0)
        {
            width = new DataGridLength(px, DataGridLengthUnitType.Pixel);
            return true;
        }

        return false;
    }
}
