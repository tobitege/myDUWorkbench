using Avalonia;
using Avalonia.Controls;
using Avalonia.Input.Platform;
using Avalonia.Interactivity;
using System;

namespace myDUWorkbench.Views;

public partial class StatusDetailsDialog : Window
{
    public StatusDetailsDialog()
    {
        InitializeComponent();
    }

    public StatusDetailsDialog(string title, string details)
        : this()
    {
        string resolvedTitle = string.IsNullOrWhiteSpace(title) ? "Details" : title.Trim();
        Title = resolvedTitle;
        DetailsTextBox.Text = details ?? string.Empty;
    }

    private async void OnCopyClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        try
        {
            TopLevel? topLevel = TopLevel.GetTopLevel(this);
            IClipboard? clipboard = topLevel?.Clipboard;
            if (clipboard is null)
            {
                CopyResultText.Text = "Clipboard unavailable.";
                return;
            }

            await ClipboardExtensions.SetTextAsync(clipboard, DetailsTextBox.Text ?? string.Empty);
            CopyResultText.Text = "Copied.";
        }
        catch (Exception ex)
        {
            CopyResultText.Text = $"Copy failed: {ex.Message}";
        }
    }

    private void OnCloseClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close();
    }
}
