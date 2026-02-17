using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorker.Views;

public partial class UnsavedChangesDialog : Window
{
    public UnsavedChangesDialog()
    {
        InitializeComponent();
    }

    public UnsavedChangesDialog(string actionDescription)
        : this()
    {
        string action = string.IsNullOrWhiteSpace(actionDescription) ? "continue" : actionDescription.Trim();
        PromptText.Text = $"Discard unsaved changes and {action}?";
    }

    private void OnKeepEditingClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(false);
    }

    private void OnDiscardClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(true);
    }
}
