using Avalonia.Controls;
using Avalonia.Interactivity;
using System;
using System.Globalization;
using myDUWorkbench.Models;

namespace myDUWorkbench.Views;

public partial class BlueprintDeleteProgressDialog : Window
{
    private bool _allowClose;
    private bool _cancelRequested;

    public BlueprintDeleteProgressDialog()
    {
        InitializeComponent();
        Closing += OnDialogClosing;
    }

    public BlueprintDeleteProgressDialog(int totalCount)
        : this()
    {
        Title = totalCount == 1 ? "Delete Blueprint" : "Delete Blueprints";
        CountText.Text = totalCount > 0
            ? $"0/{totalCount.ToString(CultureInfo.InvariantCulture)}"
            : "0/0";
    }

    public event EventHandler? CancelRequested;

    public void UpdateProgress(BlueprintDeleteProgress progress)
    {
        int safeTotal = progress.TotalCount <= 0 ? 1 : progress.TotalCount;
        int safeCurrent = Math.Clamp(progress.CurrentIndex, 0, safeTotal);
        string blueprintName = string.IsNullOrWhiteSpace(progress.BlueprintName)
            ? "(unnamed)"
            : progress.BlueprintName.Trim();

        CountText.Text =
            $"{safeCurrent.ToString(CultureInfo.InvariantCulture)}/{safeTotal.ToString(CultureInfo.InvariantCulture)}";
        CurrentBlueprintText.Text =
            $"ID {progress.BlueprintId.ToString(CultureInfo.InvariantCulture)} | {blueprintName}";
        DeleteProgressBar.Value = safeCurrent * 100d / safeTotal;
    }

    public void BeginCancelling()
    {
        if (_cancelRequested)
        {
            return;
        }

        _cancelRequested = true;
        StatusText.Text = "Cancelling...";
        CancelButton.IsEnabled = false;
    }

    public void CloseDialog()
    {
        _allowClose = true;
        Close();
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        RequestCancel();
    }

    private void OnDialogClosing(object? sender, WindowClosingEventArgs e)
    {
        _ = sender;
        if (_allowClose)
        {
            return;
        }

        e.Cancel = true;
        RequestCancel();
    }

    private void RequestCancel()
    {
        if (_cancelRequested)
        {
            return;
        }

        BeginCancelling();
        CancelRequested?.Invoke(this, EventArgs.Empty);
    }
}
