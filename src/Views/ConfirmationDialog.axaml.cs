using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorker.Views;

public partial class ConfirmationDialog : Window
{
    public ConfirmationDialog()
    {
        InitializeComponent();
    }

    public ConfirmationDialog(
        string title,
        string prompt,
        string confirmButtonText = "Confirm",
        string cancelButtonText = "Cancel")
        : this()
    {
        string resolvedTitle = string.IsNullOrWhiteSpace(title) ? "Confirm action" : title.Trim();
        string resolvedPrompt = string.IsNullOrWhiteSpace(prompt) ? "Are you sure?" : prompt.Trim();
        string resolvedConfirmText = string.IsNullOrWhiteSpace(confirmButtonText) ? "Confirm" : confirmButtonText.Trim();
        string resolvedCancelText = string.IsNullOrWhiteSpace(cancelButtonText) ? "Cancel" : cancelButtonText.Trim();

        Title = resolvedTitle;
        TitleText.Text = resolvedTitle;
        PromptText.Text = resolvedPrompt;
        ConfirmButton.Content = resolvedConfirmText;
        CancelButton.Content = resolvedCancelText;
    }

    private void OnConfirmClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(true);
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(false);
    }
}
